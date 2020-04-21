/*
 *  Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.cluster;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ControlResponsePoller;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.BackupResponseDecoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.status.CountersReader;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.archive.client.AeronArchive.*;
import static io.aeron.cluster.ClusterBackup.State.*;
import static io.aeron.cluster.MemberStatusAdapter.FRAGMENT_POLL_LIMIT;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;

/**
 * {@link Agent} which backs up a remote cluster by replicating the log and polling for snapshots.
 */
public class ClusterBackupAgent implements Agent, UnavailableCounterHandler
{
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final BackupResponseDecoder backupResponseDecoder = new BackupResponseDecoder();

    private final ClusterBackup.Context ctx;
    private final ClusterMarkFile markFile;
    private final AgentInvoker aeronClientInvoker;
    private final EpochClock epochClock;
    private final Aeron aeron;
    private final String[] clusterMemberStatusEndpoints;
    private final MemberStatusPublisher memberStatusPublisher = new MemberStatusPublisher();
    private final ArrayList<RecordingLog.Snapshot> snapshotsToRetrieve = new ArrayList<>(4);
    private final ArrayList<RecordingLog.Snapshot> snapshotsRetrieved = new ArrayList<>(4);
    private final Long2LongHashMap snapshotLengthMap = new Long2LongHashMap(NULL_LENGTH);
    private final Counter stateCounter;
    private final Counter liveLogPositionCounter;
    private final Counter nextQueryDeadlineMsCounter;
    private final ClusterBackupEventsListener eventsListener;
    private final long backupResponseTimeoutMs;
    private final long backupQueryIntervalMs;
    private final long backupProgressTimeoutMs;

    private ClusterBackup.State state = INIT;

    private RecordingLog recordingLog;

    private AeronArchive backupArchive;
    private AeronArchive.AsyncConnect clusterArchiveAsyncConnect;
    private AeronArchive clusterArchive;

    private SnapshotRetrieveMonitor snapshotRetrieveMonitor;

    private final FragmentAssembler memberStatusFragmentAssembler = new FragmentAssembler(this::onFragment);
    private final Subscription memberStatusSubscription;
    private ExclusivePublication memberStatusPublication;
    private ClusterMember[] clusterMembers;
    private ClusterMember leaderMember;
    private RecordingLog.Entry leaderLogEntry;
    private RecordingLog.Entry leaderLastTermEntry;

    private long timeOfLastTickMs = 0;
    private long timeOfLastBackupQueryMs = 0;
    private long timeOfLastProgressMs = 0;
    private long correlationId = NULL_VALUE;
    private long leaderLogRecordingId = NULL_VALUE;
    private long liveLogReplaySubscriptionId = NULL_VALUE;
    private long liveLogRecordingId = NULL_VALUE;
    private long liveLogReplayId = NULL_VALUE;
    private int leaderCommitPositionCounterId = NULL_VALUE;
    private int clusterMembersStatusEndpointsCursor = NULL_VALUE;
    private int snapshotCursor = 0;
    private int liveLogReplaySessionId = NULL_VALUE;
    private int liveLogRecCounterId = NULL_COUNTER_ID;

    ClusterBackupAgent(final ClusterBackup.Context ctx)
    {
        this.ctx = ctx;
        aeron = ctx.aeron();
        epochClock = ctx.epochClock();
        backupResponseTimeoutMs = TimeUnit.NANOSECONDS.toMillis(ctx.clusterBackupResponseTimeoutNs());
        backupQueryIntervalMs = TimeUnit.NANOSECONDS.toMillis(ctx.clusterBackupIntervalNs());
        backupProgressTimeoutMs = TimeUnit.NANOSECONDS.toMillis(ctx.clusterBackupProgressTimeoutNs());
        markFile = ctx.clusterMarkFile();
        eventsListener = ctx.eventsListener();

        clusterMemberStatusEndpoints = ctx.clusterMembersStatusEndpoints().split(",");

        aeronClientInvoker = aeron.conductorAgentInvoker();
        aeronClientInvoker.invoke();

        aeron.addUnavailableCounterHandler(this);

        memberStatusSubscription = aeron.addSubscription(ctx.memberStatusChannel(), ctx.memberStatusStreamId());

        stateCounter = ctx.stateCounter();
        liveLogPositionCounter = ctx.liveLogPositionCounter();
        nextQueryDeadlineMsCounter = ctx.nextQueryDeadlineMsCounter();
    }

    public void onStart()
    {
        backupArchive = AeronArchive.connect(ctx.archiveContext().clone());
        stateCounter.setOrdered(INIT.code());
        nextQueryDeadlineMsCounter.setOrdered(epochClock.time() - 1);
    }

    public void onClose()
    {
        if (!ctx.ownsAeronClient())
        {
            CloseHelper.close(memberStatusSubscription);
            CloseHelper.close(memberStatusPublication);
        }

        if (NULL_VALUE != liveLogReplaySubscriptionId)
        {
            backupArchive.stopRecording(liveLogReplaySubscriptionId);
        }

        CloseHelper.close(backupArchive);
        CloseHelper.close(clusterArchiveAsyncConnect);
        CloseHelper.close(clusterArchive);
        CloseHelper.close(recordingLog);
        ctx.close();
    }

    public int doWork()
    {
        final long nowMs = epochClock.time();
        int workCount = INIT == state ? init(nowMs) : 0;

        if (nowMs != timeOfLastTickMs)
        {
            timeOfLastTickMs = nowMs;
            workCount += aeronClientInvoker.invoke();
            markFile.updateActivityTimestamp(nowMs);
        }

        try
        {
            workCount += memberStatusSubscription.poll(memberStatusFragmentAssembler, FRAGMENT_POLL_LIMIT);

            switch (state)
            {
                case BACKUP_QUERY:
                    workCount += backupQuery(nowMs);
                    break;

                case SNAPSHOT_LENGTH_RETRIEVE:
                    workCount += snapshotLengthRetrieve(nowMs);
                    break;

                case SNAPSHOT_RETRIEVE:
                    workCount += snapshotRetrieve(nowMs);
                    break;

                case LIVE_LOG_REPLAY:
                    workCount += liveLogReplay(nowMs);
                    break;

                case UPDATE_RECORDING_LOG:
                    workCount += updateRecordingLog(nowMs);
                    break;

                case RESET_BACKUP:
                    workCount += resetBackup(nowMs);
                    break;

                case BACKING_UP:
                    workCount += backingUp(nowMs);
                    break;
            }

            if (hasProgressStalled(nowMs))
            {
                if (null != eventsListener)
                {
                    eventsListener.onPossibleClusterFailure();
                }

                state(RESET_BACKUP, nowMs);
            }
        }
        catch (final Exception ex)
        {
            if (null != eventsListener)
            {
                eventsListener.onPossibleClusterFailure();
            }

            state(RESET_BACKUP, nowMs);
            throw ex;
        }

        return workCount;
    }

    public String roleName()
    {
        return "cluster-backup";
    }

    public void reset()
    {
        clusterMembers = null;
        leaderMember = null;
        snapshotsToRetrieve.clear();
        snapshotsRetrieved.clear();
        snapshotLengthMap.clear();
        leaderLogEntry = null;
        leaderLastTermEntry = null;
        clusterMembersStatusEndpointsCursor = NULL_VALUE;

        if (null != recordingLog)
        {
            recordingLog.close();
            recordingLog = null;
        }

        memberStatusFragmentAssembler.clear();
        final ExclusivePublication memberStatusPublication = this.memberStatusPublication;
        final AeronArchive clusterArchive = this.clusterArchive;
        final AeronArchive.AsyncConnect clusterArchiveAsyncConnect = this.clusterArchiveAsyncConnect;

        this.memberStatusPublication = null;
        this.clusterArchive = null;
        this.clusterArchiveAsyncConnect = null;

        correlationId = NULL_VALUE;
        liveLogRecCounterId = NULL_COUNTER_ID;
        liveLogRecordingId = NULL_VALUE;
        liveLogReplayId = NULL_VALUE;
        liveLogReplaySubscriptionId = NULL_VALUE;

        CloseHelper.closeAll(
            memberStatusPublication, clusterArchive, clusterArchiveAsyncConnect);
    }

    private void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        if (messageHeaderDecoder.templateId() == BackupResponseDecoder.TEMPLATE_ID)
        {
            backupResponseDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            onBackupResponse(
                backupResponseDecoder.correlationId(),
                backupResponseDecoder.logRecordingId(),
                backupResponseDecoder.logLeadershipTermId(),
                backupResponseDecoder.logTermBaseLogPosition(),
                backupResponseDecoder.lastLeadershipTermId(),
                backupResponseDecoder.lastTermBaseLogPosition(),
                backupResponseDecoder.commitPositionCounterId(),
                backupResponseDecoder.leaderMemberId(),
                backupResponseDecoder);
        }
    }

    public void onUnavailableCounter(
        final CountersReader countersReader, final long registrationId, final int counterId)
    {
        if (counterId == liveLogRecCounterId ||
            (null != snapshotRetrieveMonitor && counterId == snapshotRetrieveMonitor.counterId))
        {
            if (null != eventsListener)
            {
                eventsListener.onPossibleClusterFailure();
            }

            state(RESET_BACKUP, epochClock.time());
        }
    }

    @SuppressWarnings("MethodLength")
    private void onBackupResponse(
        final long correlationId,
        final long logRecordingId,
        final long logLeadershipTermId,
        final long logTermBaseLogPosition,
        final long lastLeadershipTermId,
        final long lastTermBaseLogPosition,
        final int commitPositionCounterId,
        final int leaderMemberId,
        final BackupResponseDecoder backupResponseDecoder)
    {
        if (BACKUP_QUERY == state && correlationId == this.correlationId)
        {
            final BackupResponseDecoder.SnapshotsDecoder snapshotsDecoder = backupResponseDecoder.snapshots();

            if (snapshotsDecoder.count() > 0)
            {
                for (final BackupResponseDecoder.SnapshotsDecoder snapshot : snapshotsDecoder)
                {
                    final RecordingLog.Entry entry = recordingLog.getLatestSnapshot(snapshot.serviceId());

                    if (null != entry)
                    {
                        if (snapshot.logPosition() == entry.logPosition)
                        {
                            continue;
                        }
                    }

                    snapshotsToRetrieve.add(new RecordingLog.Snapshot(
                        snapshot.recordingId(),
                        snapshot.leadershipTermId(),
                        snapshot.termBaseLogPosition(),
                        snapshot.logPosition(),
                        snapshot.timestamp(),
                        snapshot.serviceId()));
                }
            }

            final RecordingLog.Entry lastTerm = recordingLog.findLastTerm();

            if (null == leaderMember || leaderMember.id() != leaderMemberId || logRecordingId != leaderLogRecordingId)
            {
                leaderLogRecordingId = logRecordingId;

                leaderLogEntry = new RecordingLog.Entry(
                    logRecordingId,
                    logLeadershipTermId,
                    logTermBaseLogPosition,
                    NULL_POSITION,
                    NULL_TIMESTAMP,
                    NULL_VALUE,
                    RecordingLog.ENTRY_TYPE_TERM,
                    true, -1
                );
            }

            if (null == lastTerm ||
                lastLeadershipTermId != lastTerm.leadershipTermId ||
                lastTermBaseLogPosition != lastTerm.termBaseLogPosition)
            {
                leaderLastTermEntry = new RecordingLog.Entry(
                    logRecordingId,
                    lastLeadershipTermId,
                    lastTermBaseLogPosition,
                    NULL_POSITION,
                    NULL_TIMESTAMP,
                    NULL_VALUE,
                    RecordingLog.ENTRY_TYPE_TERM,
                    true,
                    -1
                );
            }

            timeOfLastBackupQueryMs = 0;
            snapshotCursor = 0;
            this.correlationId = NULL_VALUE;
            leaderCommitPositionCounterId = commitPositionCounterId;

            clusterMembers = ClusterMember.parse(backupResponseDecoder.clusterMembers());
            leaderMember = ClusterMember.findMember(clusterMembers, leaderMemberId);

            if (null != eventsListener)
            {
                eventsListener.onBackupResponse(clusterMembers, leaderMember, snapshotsToRetrieve);
            }

            if (null == clusterArchive)
            {
                final ChannelUri leaderArchiveUri = ChannelUri.parse(ctx.archiveContext().controlRequestChannel());
                leaderArchiveUri.put(ENDPOINT_PARAM_NAME, leaderMember.archiveEndpoint());

                final AeronArchive.Context leaderArchiveCtx = new AeronArchive.Context()
                    .aeron(ctx.aeron())
                    .controlRequestChannel(leaderArchiveUri.toString())
                    .controlRequestStreamId(ctx.archiveContext().controlRequestStreamId())
                    .controlResponseChannel(ctx.archiveContext().controlResponseChannel())
                    .controlResponseStreamId(ctx.archiveContext().controlResponseStreamId());

                CloseHelper.close(clusterArchiveAsyncConnect);
                clusterArchiveAsyncConnect = AeronArchive.asyncConnect(leaderArchiveCtx);
            }

            final long nowMs = epochClock.time();

            timeOfLastProgressMs = nowMs;

            if (snapshotsToRetrieve.isEmpty())
            {
                state(LIVE_LOG_REPLAY, nowMs);
            }
            else
            {
                state(SNAPSHOT_LENGTH_RETRIEVE, nowMs);
            }
        }
    }

    private int init(final long nowMs)
    {
        CloseHelper.close(recordingLog);
        recordingLog = new RecordingLog(ctx.clusterDir());
        timeOfLastProgressMs = nowMs;
        state(BACKUP_QUERY, nowMs);
        return 1;
    }

    private int resetBackup(final long nowMs)
    {
        reset();
        timeOfLastProgressMs = nowMs;
        state(INIT, nowMs);
        return 1;
    }

    private int backupQuery(final long nowMs)
    {
        if (null == memberStatusPublication || nowMs > (timeOfLastBackupQueryMs + backupResponseTimeoutMs))
        {
            int cursor = ++clusterMembersStatusEndpointsCursor;
            if (cursor >= clusterMemberStatusEndpoints.length)
            {
                clusterMembersStatusEndpointsCursor = 0;
                cursor = 0;
            }

            CloseHelper.close(clusterArchiveAsyncConnect);
            clusterArchiveAsyncConnect = null;
            CloseHelper.close(clusterArchive);
            clusterArchive = null;

            CloseHelper.close(memberStatusPublication);
            final ChannelUri uri = ChannelUri.parse(ctx.memberStatusChannel());
            uri.put(ENDPOINT_PARAM_NAME, clusterMemberStatusEndpoints[cursor]);
            memberStatusPublication = aeron.addExclusivePublication(uri.toString(), ctx.memberStatusStreamId());
            correlationId = NULL_VALUE;
            timeOfLastBackupQueryMs = nowMs;

            return 1;
        }
        else if (NULL_VALUE == correlationId && memberStatusPublication.isConnected())
        {
            final long correlationId = aeron.nextCorrelationId();

            if (memberStatusPublisher.backupQuery(
                memberStatusPublication,
                correlationId,
                ctx.memberStatusStreamId(),
                AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION,
                ctx.memberStatusChannel(),
                ArrayUtil.EMPTY_BYTE_ARRAY))
            {
                timeOfLastBackupQueryMs = nowMs;
                this.correlationId = correlationId;

                return 1;
            }
        }

        return 0;
    }

    private int snapshotLengthRetrieve(final long nowMs)
    {
        int workCount = 0;

        if (null == clusterArchive)
        {
            clusterArchive = clusterArchiveAsyncConnect.poll();
            return null == clusterArchive ? 0 : 1;
        }

        if (NULL_VALUE == correlationId)
        {
            final long stopPositionCorrelationId = ctx.aeron().nextCorrelationId();
            final RecordingLog.Snapshot snapshot = snapshotsToRetrieve.get(snapshotCursor);

            if (clusterArchive.archiveProxy().getStopPosition(
                snapshot.recordingId,
                stopPositionCorrelationId,
                clusterArchive.controlSessionId()))
            {
                correlationId = stopPositionCorrelationId;
                timeOfLastProgressMs = nowMs;
                workCount++;
            }
        }
        else if (pollForResponse(clusterArchive, correlationId))
        {
            final long snapshotStopPosition = (int)clusterArchive.controlResponsePoller().relevantId();
            correlationId = NULL_VALUE;

            if (NULL_POSITION == snapshotStopPosition)
            {
                state(RESET_BACKUP, nowMs);
            }

            snapshotLengthMap.put(snapshotCursor, snapshotStopPosition);
            if (++snapshotCursor >= snapshotsToRetrieve.size())
            {
                snapshotCursor = 0;
                state(SNAPSHOT_RETRIEVE, nowMs);
            }

            timeOfLastProgressMs = nowMs;
            workCount++;
        }

        return workCount;
    }

    private int snapshotRetrieve(final long nowMs)
    {
        int workCount = 0;

        if (null == clusterArchive)
        {
            clusterArchive = clusterArchiveAsyncConnect.poll();
            return null == clusterArchive ? 0 : 1;
        }

        if (null != snapshotRetrieveMonitor)
        {
            if (snapshotRetrieveMonitor.hasRecordingProgressed())
            {
                timeOfLastProgressMs = nowMs;
                workCount++;
            }
            else if (snapshotRetrieveMonitor.isDone())
            {
                final RecordingLog.Snapshot snapshot = snapshotsToRetrieve.get(snapshotCursor);

                snapshotsRetrieved.add(new RecordingLog.Snapshot(
                    snapshotRetrieveMonitor.recordingId,
                    snapshot.leadershipTermId,
                    snapshot.termBaseLogPosition,
                    snapshot.logPosition,
                    snapshot.timestamp,
                    snapshot.serviceId));

                snapshotRetrieveMonitor = null;
                correlationId = NULL_VALUE;
                timeOfLastProgressMs = nowMs;

                if (++snapshotCursor >= snapshotsToRetrieve.size())
                {
                    state(LIVE_LOG_REPLAY, nowMs);
                    workCount++;
                }
            }
        }
        else if (NULL_VALUE == correlationId)
        {
            final long replayId = ctx.aeron().nextCorrelationId();
            final RecordingLog.Snapshot snapshot = snapshotsToRetrieve.get(snapshotCursor);
            final String transferChannel = "aeron:udp?endpoint=" + ctx.transferEndpoint();

            if (clusterArchive.archiveProxy().replay(
                snapshot.recordingId,
                0,
                NULL_LENGTH,
                transferChannel,
                ctx.replayStreamId(),
                replayId,
                clusterArchive.controlSessionId()))
            {
                correlationId = replayId;
                timeOfLastProgressMs = nowMs;
                workCount++;
            }
        }
        else if (pollForResponse(clusterArchive, correlationId))
        {
            final int replaySessionId = (int)clusterArchive.controlResponsePoller().relevantId();
            final String replayChannel =
                "aeron:udp?endpoint=" + ctx.transferEndpoint() + "|session-id=" + replaySessionId;

            backupArchive.startRecording(replayChannel, ctx.replayStreamId(), SourceLocation.REMOTE, true);

            snapshotRetrieveMonitor = new SnapshotRetrieveMonitor(
                replaySessionId, ctx.aeron().countersReader(), snapshotLengthMap.get(snapshotCursor));

            timeOfLastProgressMs = nowMs;
            workCount++;
        }

        return workCount;
    }

    private int liveLogReplay(final long nowMs)
    {
        int workCount = 0;

        if (NULL_VALUE == liveLogRecordingId)
        {
            if (null == clusterArchive)
            {
                clusterArchive = clusterArchiveAsyncConnect.poll();
                return null == clusterArchive ? 0 : 1;
            }

            if (NULL_VALUE == correlationId)
            {
                final long replayId = ctx.aeron().nextCorrelationId();
                final RecordingLog.Entry logEntry = recordingLog.findLastTerm();
                final long startPosition = null == logEntry ?
                    NULL_POSITION : backupArchive.getStopPosition(logEntry.recordingId);

                final String transferChannel = "aeron:udp?endpoint=" + ctx.transferEndpoint();

                if (clusterArchive.archiveProxy().boundedReplay(
                    leaderLogRecordingId,
                    startPosition,
                    NULL_LENGTH,
                    leaderCommitPositionCounterId,
                    transferChannel,
                    ctx.logStreamId(),
                    replayId,
                    clusterArchive.controlSessionId()))
                {
                    correlationId = replayId;
                    timeOfLastProgressMs = nowMs;
                    workCount++;
                }
            }
            else if (NULL_VALUE != liveLogReplaySubscriptionId && NULL_COUNTER_ID == liveLogRecCounterId)
            {
                final CountersReader countersReader = aeron.countersReader();

                if ((liveLogRecCounterId = RecordingPos.findCounterIdBySession(
                    countersReader, liveLogReplaySessionId)) != NULL_COUNTER_ID)
                {
                    liveLogPositionCounter.setOrdered(countersReader.getCounterValue(liveLogRecCounterId));

                    liveLogRecordingId = RecordingPos.getRecordingId(countersReader, liveLogRecCounterId);
                    timeOfLastBackupQueryMs = nowMs;
                    timeOfLastProgressMs = nowMs;

                    state(UPDATE_RECORDING_LOG, nowMs);
                }
            }
            else if (pollForResponse(clusterArchive, correlationId))
            {
                final RecordingLog.Entry logEntry = recordingLog.findLastTerm();
                liveLogReplayId = clusterArchive.controlResponsePoller().relevantId();
                liveLogReplaySessionId = (int)liveLogReplayId;
                final String replayChannel =
                    "aeron:udp?endpoint=" + ctx.transferEndpoint() + "|session-id=" + liveLogReplaySessionId;

                timeOfLastProgressMs = nowMs;

                if (null == logEntry)
                {
                    liveLogReplaySubscriptionId = backupArchive.startRecording(
                        replayChannel, ctx.logStreamId(), SourceLocation.REMOTE);
                }
                else
                {
                    liveLogReplaySubscriptionId = backupArchive.extendRecording(
                        logEntry.recordingId, replayChannel, ctx.logStreamId(), SourceLocation.REMOTE);
                }
            }
        }
        else
        {
            timeOfLastProgressMs = nowMs;
            state(UPDATE_RECORDING_LOG, nowMs);
        }

        return workCount;
    }

    private int updateRecordingLog(final long nowMs)
    {
        boolean wasRecordingLogUpdated = false;
        final long snapshotLeadershipTermId = snapshotsRetrieved.isEmpty() ?
            NULL_VALUE : snapshotsRetrieved.get(0).leadershipTermId;

        if (null != leaderLogEntry &&
            recordingLog.isUnknown(leaderLogEntry.leadershipTermId) &&
            leaderLogEntry.leadershipTermId <= snapshotLeadershipTermId)
        {
            recordingLog.appendTerm(
                liveLogRecordingId,
                leaderLogEntry.leadershipTermId,
                leaderLogEntry.termBaseLogPosition,
                leaderLogEntry.timestamp);

            wasRecordingLogUpdated = true;
            leaderLogEntry = null;
        }

        if (!snapshotsRetrieved.isEmpty())
        {
            for (int i = snapshotsRetrieved.size() - 1; i >= 0; i--)
            {
                final RecordingLog.Snapshot snapshot = snapshotsRetrieved.get(i);

                recordingLog.appendSnapshot(
                    snapshot.recordingId,
                    snapshot.leadershipTermId,
                    snapshot.termBaseLogPosition,
                    snapshot.logPosition,
                    snapshot.timestamp,
                    snapshot.serviceId);
            }

            wasRecordingLogUpdated = true;
        }

        if (null != leaderLastTermEntry && recordingLog.isUnknown(leaderLastTermEntry.leadershipTermId))
        {
            recordingLog.appendTerm(
                liveLogRecordingId,
                leaderLastTermEntry.leadershipTermId,
                leaderLastTermEntry.termBaseLogPosition,
                leaderLastTermEntry.timestamp);

            wasRecordingLogUpdated = true;
            leaderLastTermEntry = null;
        }

        if (wasRecordingLogUpdated && null != eventsListener)
        {
            eventsListener.onUpdatedRecordingLog(recordingLog, snapshotsRetrieved);
        }

        snapshotsRetrieved.clear();
        snapshotsToRetrieve.clear();
        snapshotLengthMap.clear();

        timeOfLastProgressMs = nowMs;

        nextQueryDeadlineMsCounter.setOrdered(nowMs + backupQueryIntervalMs);
        state(BACKING_UP, nowMs);

        return 1;
    }

    private int backingUp(final long nowMs)
    {
        int workCount = 0;

        if (nowMs > nextQueryDeadlineMsCounter.get())
        {
            timeOfLastBackupQueryMs = nowMs;
            timeOfLastProgressMs = nowMs;
            state(BACKUP_QUERY, nowMs);
            workCount += 1;
        }

        if (NULL_COUNTER_ID != liveLogRecCounterId)
        {
            final long liveLogPosition = aeron.countersReader().getCounterValue(liveLogRecCounterId);

            if (liveLogPositionCounter.proposeMaxOrdered(liveLogPosition))
            {
                if (null != eventsListener)
                {
                    eventsListener.onLiveLogProgress(liveLogRecordingId, liveLogRecCounterId, liveLogPosition);
                }

                workCount += 1;
            }
        }

        return workCount;
    }

    private void state(final ClusterBackup.State newState, final long nowMs)
    {
        stateChange(state, newState, nowMs);

        if (BACKUP_QUERY == newState && null != eventsListener)
        {
            eventsListener.onBackupQuery();
        }

        stateCounter.setOrdered(newState.code());
        state = newState;
    }

    @SuppressWarnings("unused")
    private void stateChange(final ClusterBackup.State oldState, final ClusterBackup.State newState, final long nowMs)
    {
        //System.out.println(nowMs + ": " + oldState + " -> " + newState);
    }

    private static boolean pollForResponse(final AeronArchive archive, final long correlationId)
    {
        final ControlResponsePoller poller = archive.controlResponsePoller();

        if (poller.poll() > 0 && poller.isPollComplete())
        {
            if (poller.controlSessionId() == archive.controlSessionId() && poller.correlationId() == correlationId)
            {
                if (poller.code() == ControlResponseCode.ERROR)
                {
                    throw new ClusterException(
                        "archive response for correlationId=" + correlationId + ", error: " + poller.errorMessage());
                }

                return true;
            }
        }

        return false;
    }

    private boolean hasProgressStalled(final long nowMs)
    {
        return (NULL_COUNTER_ID == liveLogRecCounterId) && (nowMs > (timeOfLastProgressMs + backupProgressTimeoutMs));
    }

    static class SnapshotRetrieveMonitor
    {
        private final long endPosition;
        private final int sessionId;
        private final CountersReader countersReader;

        private long lastRecordingPosition = 0;
        private long recordingId = RecordingPos.NULL_RECORDING_ID;
        private long recordingPosition = NULL_POSITION;
        private int counterId;

        SnapshotRetrieveMonitor(final int sessionId, final CountersReader countersReader, final long endPosition)
        {
            this.endPosition = endPosition;
            this.countersReader = countersReader;
            this.counterId = RecordingPos.findCounterIdBySession(countersReader, sessionId);
            this.sessionId = sessionId;
        }

        boolean isDone()
        {
            return endPosition <= recordingPosition;
        }

        boolean hasRecordingProgressed()
        {
            final boolean result;

            if (NULL_COUNTER_ID == counterId)
            {
                counterId = RecordingPos.findCounterIdBySession(countersReader, sessionId);
                result = NULL_COUNTER_ID != counterId;
            }
            else if (RecordingPos.NULL_RECORDING_ID == recordingId)
            {
                recordingId = RecordingPos.getRecordingId(countersReader, counterId);
                result = RecordingPos.NULL_RECORDING_ID != recordingId;
            }
            else
            {
                recordingPosition = countersReader.getCounterValue(counterId);
                result = recordingPosition > lastRecordingPosition;
                lastRecordingPosition = recordingPosition;
            }

            return result;
        }
    }
}
