/*
 *  Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import io.aeron.cluster.codecs.SnapshotMarkerDecoder;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.CountersReader;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.archive.client.AeronArchive.NULL_LENGTH;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.MemberStatusAdapter.FRAGMENT_POLL_LIMIT;

public class ClusterBackupAgent implements Agent, FragmentHandler, UnavailableImageHandler
{
    enum State
    {
        CHECK_BACKUP,
        BACKUP_QUERY,
        SNAPSHOT_RETRIEVE,
        LIVE_LOG_REPLAY,
        BACKING_UP
    }

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final BackupResponseDecoder backupResponseDecoder = new BackupResponseDecoder();

    private final ClusterBackup.Context ctx;
    private final ClusterMarkFile markFile;
    private final AgentInvoker aeronClientInvoker;
    private final EpochClock epochClock;
    private final Aeron aeron;
    private final IdleStrategy idleStrategy;
    private final String[] clusterMemberStatusEndpoints;
    private final MemberStatusPublisher memberStatusPublisher = new MemberStatusPublisher();
    private final ArrayList<RecordingLog.Snapshot> leaderSnapshots = new ArrayList<>(4);
    private final long intervalMs;

    private ClusterBackupAgent.State state = State.CHECK_BACKUP;
    private AeronArchive backupArchive;
    private AeronArchive.AsyncConnect clusterArchiveAsyncConnect;
    private AeronArchive clusterArchive;
    private Subscription snapshotRetrieveSubscription;
    private Image snapshotRetrieveImage;
    private SnapshotReader snapshotReader;
    private ClusterMember[] clusterMembers;
    private ClusterMember leaderMember;

    private Subscription memberStatusSubscription;
    private Subscription liveLogReplaySubscription;
    private ExclusivePublication memberStatusPublication;

    private long timeOfLastActivityMs = 0;
    private long correlationId = NULL_VALUE;
    private long logRecordingId = NULL_VALUE;
    private int clusterMembersStatusEndpointsCursor = -1;
    private int snapshotCursor = 0;
    private int snapshotReplaySessionId = NULL_VALUE;
    private int liveLogReplaySessionId = NULL_VALUE;
    private long snapshotRetrieveSubscriptionId = NULL_VALUE;
    private long liveLogReplaySubscriptionId = NULL_VALUE;

    ClusterBackupAgent(final ClusterBackup.Context ctx)
    {
        this.ctx = ctx;
        this.aeron = ctx.aeron();
        this.epochClock = ctx.epochClock();
        this.intervalMs = TimeUnit.NANOSECONDS.toMillis(ctx.clusterBackupIntervalNs());
        this.idleStrategy = ctx.idleStrategy();
        this.markFile = ctx.clusterMarkFile();

        this.clusterMemberStatusEndpoints = ctx.clusterMembersStatusEndpoints().split(",");

        aeronClientInvoker = aeron.conductorAgentInvoker();
        aeronClientInvoker.invoke();

        memberStatusSubscription = aeron.addSubscription(
            ctx.memberStatusChannel(), ctx.memberStatusStreamId());
    }

    public void onStart()
    {
        backupArchive = AeronArchive.connect(ctx.archiveContext().clone());

    }

    public void onClose()
    {
        if (!ctx.ownsAeronClient())
        {
            CloseHelper.close(memberStatusSubscription);
            CloseHelper.close(memberStatusPublication);
            CloseHelper.close(liveLogReplaySubscription);
        }

        CloseHelper.close(backupArchive);
        CloseHelper.close(clusterArchive);
        ctx.close();
    }

    public int doWork()
    {
        int workCount = 0;
        final long nowMs = epochClock.time();
        workCount += memberStatusSubscription.poll(this, FRAGMENT_POLL_LIMIT);

        switch (state)
        {
            case CHECK_BACKUP:
                workCount += checkLocalArchive(nowMs);
                break;

            case BACKUP_QUERY:
                workCount += backupQuery(nowMs);
                break;

            case SNAPSHOT_RETRIEVE:
                workCount += snapshotRetrieve();
                break;

            case LIVE_LOG_REPLAY:
                workCount += liveLogReplay(nowMs);
                break;

            case BACKING_UP:
                workCount += backingUp(nowMs);
                break;
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
        leaderSnapshots.clear();

        CloseHelper.close(memberStatusSubscription);
        memberStatusSubscription = null;

        CloseHelper.close(memberStatusPublication);
        memberStatusPublication = null;

        CloseHelper.close(liveLogReplaySubscription);
        liveLogReplaySubscription = null;
    }

    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
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
                backupResponseDecoder.commitPositionCounterId(),
                backupResponseDecoder.leaderMemberId(),
                backupResponseDecoder.snapshots(),
                backupResponseDecoder.memberEndpoints());
        }
    }

    public void onUnavailableImage(final Image image)
    {
        // TODO: clean up and restart after a delay
        reset();
        state(State.CHECK_BACKUP);
    }

    private void onBackupResponse(
        final long correlationId,
        final long logRecordingId,
        final int commitPositionCounterId,
        final int leaderMemberId,
        final BackupResponseDecoder.SnapshotsDecoder snapshotsDecoder,
        final String memberEndpoints)
    {
        if (State.BACKUP_QUERY == state && correlationId == this.correlationId)
        {
            if (snapshotsDecoder.count() > 0)
            {
                for (final BackupResponseDecoder.SnapshotsDecoder snapshot : snapshotsDecoder)
                {
                    leaderSnapshots.add(new RecordingLog.Snapshot(
                        snapshot.recordingId(),
                        snapshot.leadershipTermId(),
                        snapshot.termBaseLogPosition(),
                        snapshot.logPosition(),
                        snapshot.timestamp(),
                        snapshot.serviceId()));
                }
            }

            timeOfLastActivityMs = 0;
            snapshotCursor = 0;
            this.correlationId = NULL_VALUE;

            clusterMembers = ClusterMember.parse(memberEndpoints);
            leaderMember = ClusterMember.findMember(clusterMembers, leaderMemberId);

            if (leaderSnapshots.isEmpty())
            {
                state(State.LIVE_LOG_REPLAY);
            }
            else
            {
                final ChannelUri leaderArchiveUri = ChannelUri.parse(ctx.archiveContext().controlRequestChannel());
                leaderArchiveUri.put(ENDPOINT_PARAM_NAME, leaderMember.archiveEndpoint());

                final AeronArchive.Context leaderArchiveCtx = new AeronArchive.Context()
                    .aeron(ctx.aeron())
                    .controlRequestChannel(leaderArchiveUri.toString())
                    .controlRequestStreamId(ctx.archiveContext().controlRequestStreamId())
                    .controlResponseChannel(ctx.archiveContext().controlResponseChannel())
                    .controlResponseStreamId(ctx.archiveContext().controlResponseStreamId());

                clusterArchiveAsyncConnect = AeronArchive.asyncConnect(leaderArchiveCtx);
                state(State.SNAPSHOT_RETRIEVE);
            }
        }
    }

    private int checkLocalArchive(final long nowMs)
    {
        // TODO: grab current recording log and

        state(State.BACKUP_QUERY);
        return 0;
    }

    private int backupQuery(final long nowMs)
    {
        if (nowMs > (timeOfLastActivityMs + intervalMs))
        {
            clusterMembersStatusEndpointsCursor = Math.min(
                clusterMembersStatusEndpointsCursor + 1, clusterMemberStatusEndpoints.length - 1);

            CloseHelper.close(memberStatusPublication);
            final ChannelUri memberStatusUri = ChannelUri.parse(ctx.memberStatusChannel());
            memberStatusUri.put(ENDPOINT_PARAM_NAME, clusterMemberStatusEndpoints[clusterMembersStatusEndpointsCursor]);
            memberStatusPublication = ctx.aeron().addExclusivePublication(
                memberStatusUri.toString(), ctx.memberStatusStreamId());

            correlationId = ctx.aeron().nextCorrelationId();

            if (memberStatusPublisher.backupQuery(
                memberStatusPublication,
                correlationId,
                ctx.memberStatusStreamId(),
                AeronCluster.Configuration.SEMANTIC_VERSION,
                ctx.memberStatusChannel(),
                ArrayUtil.EMPTY_BYTE_ARRAY))
            {
                timeOfLastActivityMs = nowMs;
                return 1;
            }
        }

        return 0;
    }

    private int snapshotRetrieve()
    {
        int workCount = 0;

        if (null == clusterArchive)
        {
            clusterArchive = clusterArchiveAsyncConnect.poll();
            return null == clusterArchive ? 0 : 1;
        }

        if (null != snapshotReader)
        {
            if (snapshotReader.poll() == 0)
            {
                if (snapshotReader.isDone())
                {
                    // TODO: save snapshot info
//                    consensusModuleAgent.retrievedSnapshot(
//                        snapshotReader.recordingId(), leaderSnapshots.get(snapshotCursor));

                    CloseHelper.close(snapshotRetrieveSubscription);
                    backupArchive.stopRecording(snapshotRetrieveSubscriptionId);
                    snapshotRetrieveSubscription = null;
                    snapshotRetrieveImage = null;
                    snapshotReader = null;
                    correlationId = NULL_VALUE;
                    snapshotReplaySessionId = NULL_VALUE;

                    if (++snapshotCursor >= leaderSnapshots.size())
                    {
                        state(State.LIVE_LOG_REPLAY);
                        workCount++;
                    }
                }
                else if (null != snapshotRetrieveImage && snapshotRetrieveImage.isClosed())
                {
                    throw new ClusterException("retrieval of snapshot image ended unexpectedly");
                }
            }
            else
            {
                workCount++;
            }
        }
        else if (null == snapshotRetrieveImage && null != snapshotRetrieveSubscription)
        {
            snapshotRetrieveImage = snapshotRetrieveSubscription.imageBySessionId(snapshotReplaySessionId);
            if (null != snapshotRetrieveImage)
            {
                snapshotReader = new SnapshotReader(snapshotRetrieveImage, ctx.aeron().countersReader());
                workCount++;
            }
        }
        else if (NULL_VALUE == correlationId)
        {
            final long replayId = ctx.aeron().nextCorrelationId();
            final RecordingLog.Snapshot snapshot = leaderSnapshots.get(snapshotCursor);
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
                this.correlationId = replayId;
                workCount++;
            }
        }
        else if (pollForResponse(clusterArchive, correlationId))
        {
            snapshotReplaySessionId = (int)clusterArchive.controlResponsePoller().relevantId();
            final String replaySubscriptionChannel =
                "aeron:udp?endpoint=" + ctx.transferEndpoint() + "|session-id=" + snapshotReplaySessionId;

            snapshotRetrieveSubscription = ctx.aeron().addSubscription(replaySubscriptionChannel, ctx.replayStreamId());
            snapshotRetrieveSubscriptionId = backupArchive.startRecording(
                replaySubscriptionChannel, ctx.replayStreamId(), SourceLocation.REMOTE);
            workCount++;
        }

        return workCount;
    }

    private int liveLogReplay(final long nowMs)
    {
        int workCount = 0;

        if (null == clusterArchive)
        {
            clusterArchive = clusterArchiveAsyncConnect.poll();
            return null == clusterArchive ? 0 : 1;
        }

        if (NULL_VALUE == correlationId)
        {
            final long replayId = ctx.aeron().nextCorrelationId();
            final long startPosition =
                leaderSnapshots.isEmpty() ? 0 : leaderSnapshots.get(leaderSnapshots.size() - 1).logPosition;
            final String transferChannel = "aeron:udp?endpoint=" + ctx.transferEndpoint();

            if (clusterArchive.archiveProxy().replay(
                logRecordingId,
                startPosition,
                NULL_LENGTH,
                transferChannel,
                ctx.replayStreamId(),
                replayId,
                clusterArchive.controlSessionId()))
            {
                this.correlationId = replayId;
                workCount++;
            }
        }
        else if (pollForResponse(clusterArchive, correlationId))
        {
            liveLogReplaySessionId = (int)clusterArchive.controlResponsePoller().relevantId();
            final String replaySubscriptionChannel =
                "aeron:udp?endpoint=" + ctx.transferEndpoint() + "|session-id=" + liveLogReplaySessionId;

            liveLogReplaySubscription = ctx.aeron().addSubscription(replaySubscriptionChannel, ctx.replayStreamId());
            liveLogReplaySubscriptionId = backupArchive.startRecording(
                replaySubscriptionChannel, ctx.replayStreamId(), SourceLocation.REMOTE);
            state(State.BACKING_UP);
        }

        return workCount;
    }

    private int backingUp(final long nowMs)
    {
        final int workCount = 0;

        // TODO: poll liveLogReplaySubscription until EOS or Snapshot taken.

        return workCount;
    }

    private void state(final State newState)
    {
        this.state = newState;
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

    static class SnapshotReader implements ControlledFragmentHandler
    {
        private static final int FRAGMENT_LIMIT = 10;

        private boolean inSnapshot = false;
        private boolean isDone = false;
        private long endPosition = 0;
        private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private final SnapshotMarkerDecoder snapshotMarkerDecoder = new SnapshotMarkerDecoder();
        private final CountersReader countersReader;
        private final Image image;
        private long recordingId = RecordingPos.NULL_RECORDING_ID;
        private long recordingPosition = NULL_POSITION;
        private int counterId;

        SnapshotReader(final Image image, final CountersReader countersReader)
        {
            this.countersReader = countersReader;
            this.image = image;
            counterId = RecordingPos.findCounterIdBySession(countersReader, image.sessionId());
        }

        boolean isDone()
        {
            return isDone && (endPosition <= recordingPosition);
        }

        long recordingId()
        {
            return recordingId;
        }

        void pollRecordingPosition()
        {
            if (CountersReader.NULL_COUNTER_ID == counterId)
            {
                counterId = RecordingPos.findCounterIdBySession(countersReader, image.sessionId());
            }
            else if (RecordingPos.NULL_RECORDING_ID == recordingId)
            {
                recordingId = RecordingPos.getRecordingId(countersReader, counterId);
            }
            else
            {
                recordingPosition = countersReader.getCounterValue(counterId);
            }
        }

        int poll()
        {
            pollRecordingPosition();

            return image.controlledPoll(this, FRAGMENT_LIMIT);
        }

        public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            messageHeaderDecoder.wrap(buffer, offset);

            if (messageHeaderDecoder.templateId() == SnapshotMarkerDecoder.TEMPLATE_ID)
            {
                snapshotMarkerDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long typeId = snapshotMarkerDecoder.typeId();
                if (typeId != ConsensusModule.Configuration.SNAPSHOT_TYPE_ID &&
                    typeId != ClusteredServiceContainer.SNAPSHOT_TYPE_ID)
                {
                    throw new ClusterException("unexpected snapshot type: " + typeId);
                }

                switch (snapshotMarkerDecoder.mark())
                {
                    case BEGIN:
                        if (inSnapshot)
                        {
                            throw new ClusterException("already in snapshot");
                        }
                        inSnapshot = true;
                        return Action.CONTINUE;

                    case END:
                        if (!inSnapshot)
                        {
                            throw new ClusterException("missing begin snapshot");
                        }
                        isDone = true;
                        endPosition = header.position();
                        return Action.BREAK;
                }
            }

            return ControlledFragmentHandler.Action.CONTINUE;
        }
    }
}
