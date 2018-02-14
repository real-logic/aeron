/*
 * Copyright 2017 Real Logic Ltd.
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
package io.aeron.cluster.service;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.ClusterCncFile;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.Header;
import io.aeron.status.ReadableCounter;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.CountersReader;

import java.util.Collection;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static java.util.Collections.unmodifiableCollection;

final class ClusteredServiceAgent implements Agent, Cluster, ServiceControlListener
{
    private final int serviceId;
    private boolean isRecovering;
    private final boolean shouldCloseResources;
    private final AeronArchive.Context archiveCtx;
    private final ClusteredServiceContainer.Context ctx;
    private final Aeron aeron;
    private final Long2ObjectHashMap<ClientSession> sessionByIdMap = new Long2ObjectHashMap<>();
    private final Collection<ClientSession> readOnlyClientSessions = unmodifiableCollection(sessionByIdMap.values());
    private final ClusteredService service;
    private final ServiceControlPublisher serviceControlPublisher;
    private final ServiceControlAdapter serviceControlAdapter;
    private final IdleStrategy idleStrategy;
    private final RecordingLog recordingLog;
    private final EpochClock epochClock;
    private final CachedEpochClock cachedEpochClock = new CachedEpochClock();
    private final ClusterCncFile cncFile;

    private long baseLogPosition;
    private long leadershipTermId;
    private long timestampMs;
    private BoundedLogAdapter logAdapter;
    private ActiveLog activeLog;
    private ReadableCounter roleCounter;
    private Role role = Role.FOLLOWER;

    ClusteredServiceAgent(final ClusteredServiceContainer.Context ctx)
    {
        this.ctx = ctx;

        archiveCtx = ctx.archiveContext();
        aeron = ctx.aeron();
        shouldCloseResources = ctx.ownsAeronClient();
        service = ctx.clusteredService();
        recordingLog = ctx.recordingLog();
        idleStrategy = ctx.idleStrategy();
        serviceId = ctx.serviceId();
        epochClock = ctx.epochClock();
        cncFile = ctx.clusterCncFile();

        serviceControlPublisher = new ServiceControlPublisher(
            aeron.addPublication(ctx.serviceControlChannel(), ctx.serviceControlStreamId()));

        serviceControlAdapter = new ServiceControlAdapter(
            aeron.addSubscription(ctx.serviceControlChannel(), ctx.serviceControlStreamId()), this);
    }

    public void onStart()
    {
        service.onStart(this);

        final CountersReader counters = aeron.countersReader();
        final int recoveryCounterId = findRecoveryCounterId(counters);

        isRecovering = true;
        checkForSnapshot(counters, recoveryCounterId);
        checkForReplay(counters, recoveryCounterId);
        isRecovering = false;

        joinActiveLog(counters);

        findClusterRoleCounter(counters);
        role = Role.get((int)roleCounter.get());

        if (Role.LEADER == role)
        {
            for (final ClientSession session : sessionByIdMap.values())
            {
                session.connect(aeron);
            }
        }
    }

    public void onClose()
    {
        if (shouldCloseResources)
        {
            CloseHelper.close(logAdapter);
            CloseHelper.close(serviceControlPublisher);
            CloseHelper.close(serviceControlAdapter);

            for (final ClientSession session : sessionByIdMap.values())
            {
                session.disconnect();
            }
        }
    }

    public int doWork()
    {
        final long nowMs = epochClock.time();
        if (cachedEpochClock.time() != nowMs)
        {
            cncFile.updateActivityTimestamp(nowMs);
            cachedEpochClock.update(nowMs);
        }

        int workCount = logAdapter.poll();
        if (0 == workCount)
        {
            if (logAdapter.image().isClosed())
            {
                throw new AgentTerminationException("Image closed unexpectedly");
            }

            if (!CommitPos.isActive(aeron.countersReader(), logAdapter.upperBoundCounterId()))
            {
                throw new AgentTerminationException("Commit position is not active");
            }
        }

        workCount += serviceControlAdapter.poll();
        if (activeLog != null)
        {
            // TODO: handle new log case
            activeLog = null;
        }

        return workCount;
    }

    public String roleName()
    {
        return ctx.serviceName();
    }

    public Cluster.Role role()
    {
        return role;
    }

    public Aeron aeron()
    {
        return aeron;
    }

    public ClientSession getClientSession(final long clusterSessionId)
    {
        return sessionByIdMap.get(clusterSessionId);
    }

    public Collection<ClientSession> clientSessions()
    {
        return readOnlyClientSessions;
    }

    public long timeMs()
    {
        return timestampMs;
    }

    public void scheduleTimer(final long correlationId, final long deadlineMs)
    {
        serviceControlPublisher.scheduleTimer(correlationId, deadlineMs);
    }

    public void cancelTimer(final long correlationId)
    {
        serviceControlPublisher.cancelTimer(correlationId);
    }

    public void onJoinLog(
        final long leadershipTermId,
        final int commitPositionId,
        final int logSessionId,
        final int logStreamId,
        final String logChannel)
    {
        activeLog = new ActiveLog(leadershipTermId, commitPositionId, logSessionId, logStreamId, logChannel);
    }

    void onSessionMessage(
        final long clusterSessionId,
        final long correlationId,
        final long timestampMs,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        this.timestampMs = timestampMs;

        service.onSessionMessage(
            clusterSessionId,
            correlationId,
            timestampMs,
            buffer,
            offset,
            length,
            header);
    }

    void onTimerEvent(final long correlationId, final long timestampMs)
    {
        this.timestampMs = timestampMs;

        service.onTimerEvent(correlationId, timestampMs);
    }

    void onSessionOpen(
        final long clusterSessionId,
        final long timestampMs,
        final int responseStreamId,
        final String responseChannel,
        final byte[] principalData)
    {
        this.timestampMs = timestampMs;

        final ClientSession session = new ClientSession(
            clusterSessionId,
            responseStreamId,
            responseChannel,
            principalData,
            this);

        if (Role.LEADER == role)
        {
            session.connect(aeron);
        }

        sessionByIdMap.put(clusterSessionId, session);
        service.onSessionOpen(session, timestampMs);
    }

    void onSessionClose(final long clusterSessionId, final long timestampMs, final CloseReason closeReason)
    {
        this.timestampMs = timestampMs;

        final ClientSession session = sessionByIdMap.remove(clusterSessionId);
        session.disconnect();
        service.onSessionClose(session, timestampMs, closeReason);
    }

    void onServiceAction(final long termPosition, final long timestampMs, final ClusterAction action)
    {
        this.timestampMs = timestampMs;

        executeAction(action, termPosition);
    }

    void addSession(
        final long clusterSessionId,
        final int responseStreamId,
        final String responseChannel,
        final byte[] principalData)
    {
        final ClientSession session = new ClientSession(
            clusterSessionId,
            responseStreamId,
            responseChannel,
            principalData,
            ClusteredServiceAgent.this);

        sessionByIdMap.put(clusterSessionId, session);
    }

    private void checkForSnapshot(final CountersReader counters, final int recoveryCounterId)
    {
        final long termPosition = RecoveryState.getTermPosition(counters, recoveryCounterId);
        leadershipTermId = RecoveryState.getLeadershipTermId(counters, recoveryCounterId);
        timestampMs = RecoveryState.getTimestamp(counters, recoveryCounterId);

        if (NULL_POSITION != termPosition)
        {
            final RecordingLog.Entry snapshotEntry = recordingLog.getSnapshot(leadershipTermId, termPosition);
            if (null == snapshotEntry)
            {
                throw new IllegalStateException("No snapshot available for term position: " + termPosition);
            }

            baseLogPosition = snapshotEntry.logPosition;
            loadSnapshot(snapshotEntry.recordingId);
        }

        serviceControlPublisher.ackAction(baseLogPosition, leadershipTermId, serviceId, ClusterAction.INIT);
    }

    private void checkForReplay(final CountersReader counters, final int recoveryCounterId)
    {
        final long replayTermCount = RecoveryState.getReplayTermCount(counters, recoveryCounterId);
        if (0 == replayTermCount)
        {
            return;
        }

        service.onReplayBegin();

        for (int i = 0; i < replayTermCount; i++)
        {
            awaitActiveLog();

            final int counterId = activeLog.commitPositionId;
            baseLogPosition = CommitPos.getBaseLogPosition(counters, counterId);
            leadershipTermId = CommitPos.getLeadershipTermId(counters, counterId);

            try (Subscription subscription = aeron.addSubscription(activeLog.channel, activeLog.streamId))
            {
                serviceControlPublisher.ackAction(baseLogPosition, leadershipTermId, serviceId, ClusterAction.READY);

                final Image image = awaitImage(activeLog.sessionId, subscription);
                final ReadableCounter limit = new ReadableCounter(counters, counterId);
                final BoundedLogAdapter adapter = new BoundedLogAdapter(image, limit, this);

                consumeImage(image, adapter);

                serviceControlPublisher.ackAction(baseLogPosition, leadershipTermId, serviceId, ClusterAction.REPLAY);
            }
        }

        service.onReplayEnd();
    }

    private void awaitActiveLog()
    {
        activeLog = null;
        while (true)
        {
            final int fragments = serviceControlAdapter.poll();
            if (activeLog != null)
            {
                break;
            }

            checkInterruptedStatus();
            idleStrategy.idle(fragments);
        }
    }

    private void consumeImage(final Image image, final BoundedLogAdapter adapter)
    {
        while (true)
        {
            final int workCount = adapter.poll();
            if (workCount == 0)
            {
                if (image.isClosed())
                {
                    if (!image.isEndOfStream())
                    {
                        throw new IllegalStateException("Unexpected close of replay");
                    }

                    break;
                }

                checkInterruptedStatus();
            }

            idleStrategy.idle(workCount);
        }
    }

    private int findRecoveryCounterId(final CountersReader counters)
    {
        int counterId = RecoveryState.findCounterId(counters);

        while (CountersReader.NULL_COUNTER_ID == counterId)
        {
            checkInterruptedStatus();
            idleStrategy.idle();

            counterId = RecoveryState.findCounterId(counters);
        }

        return counterId;
    }

    private void joinActiveLog(final CountersReader counters)
    {
        awaitActiveLog();

        final int commitPositionId = activeLog.commitPositionId;
        if (!CommitPos.isActive(counters, commitPositionId))
        {
            throw new IllegalStateException("CommitPos counter not active: " + commitPositionId);
        }

        final int logSessionId = activeLog.sessionId;
        leadershipTermId = activeLog.leadershipTermId;
        baseLogPosition = CommitPos.getBaseLogPosition(counters, commitPositionId);

        final Subscription logSubscription = aeron.addSubscription(activeLog.channel, activeLog.streamId);
        final Image image = awaitImage(logSessionId, logSubscription);

        serviceControlPublisher.ackAction(baseLogPosition, leadershipTermId, serviceId, ClusterAction.READY);

        logAdapter = new BoundedLogAdapter(image, new ReadableCounter(counters, commitPositionId), this);
        activeLog = null;
    }

    private Image awaitImage(final int sessionId, final Subscription subscription)
    {
        idleStrategy.reset();
        Image image;
        while ((image = subscription.imageBySessionId(sessionId)) == null)
        {
            checkInterruptedStatus();
            idleStrategy.idle();
        }

        return image;
    }

    private void findClusterRoleCounter(final CountersReader counters)
    {
        idleStrategy.reset();

        int counterId = ClusterNodeRole.findCounterId(counters);
        while (CountersReader.NULL_COUNTER_ID == counterId)
        {
            checkInterruptedStatus();
            idleStrategy.idle();
            counterId = ClusterNodeRole.findCounterId(counters);
        }

        roleCounter = new ReadableCounter(counters, counterId);
    }

    private void loadSnapshot(final long recordingId)
    {
        try (AeronArchive archive = AeronArchive.connect(archiveCtx))
        {
            final RecordingExtent recordingExtent = new RecordingExtent();
            if (0 == archive.listRecording(recordingId, recordingExtent))
            {
                throw new IllegalStateException("Could not find recordingId: " + recordingId);
            }

            final String channel = ctx.replayChannel();
            final int streamId = ctx.replayStreamId();

            final long length = recordingExtent.stopPosition - recordingExtent.startPosition;
            final int sessionId = (int)archive.startReplay(recordingId, 0, length, channel, streamId);

            final String replaySessionChannel = ChannelUri.addSessionId(channel, sessionId);
            try (Subscription subscription = aeron.addSubscription(replaySessionChannel, streamId))
            {
                final Image image = awaitImage(sessionId, subscription);
                loadState(image);
                service.onLoadSnapshot(image);
            }
        }
    }

    private void loadState(final Image image)
    {
        final ServiceSnapshotLoader snapshotLoader = new ServiceSnapshotLoader(image, this);
        while (true)
        {
            final int fragments = snapshotLoader.poll();
            if (snapshotLoader.isDone())
            {
                break;
            }

            if (fragments == 0)
            {
                checkInterruptedStatus();

                if (image.isClosed())
                {
                    throw new IllegalStateException("Snapshot ended unexpectedly");
                }

                idleStrategy.idle(fragments);
            }
        }
    }

    private void onTakeSnapshot(final long termPosition)
    {
        final long recordingId;
        final String channel = ctx.snapshotChannel();
        final int streamId = ctx.snapshotStreamId();

        try (AeronArchive archive = AeronArchive.connect(archiveCtx);
            Publication publication = aeron.addExclusivePublication(channel, streamId))
        {
            final String recordingChannel = ChannelUri.addSessionId(channel, publication.sessionId());
            archive.startRecording(recordingChannel, streamId, SourceLocation.LOCAL);
            idleStrategy.reset();

            try
            {
                final CountersReader counters = aeron.countersReader();
                final int counterId = awaitRecordingCounter(publication, counters);

                recordingId = RecordingPos.getRecordingId(counters, counterId);
                snapshotState(publication, baseLogPosition + termPosition);
                service.onTakeSnapshot(publication);

                do
                {
                    idleStrategy.idle();
                    checkInterruptedStatus();

                    if (!RecordingPos.isActive(counters, counterId, recordingId))
                    {
                        throw new IllegalStateException("Recording has stopped unexpectedly: " + recordingId);
                    }
                }
                while (counters.getCounterValue(counterId) < publication.position());
            }
            finally
            {
                archive.stopRecording(recordingChannel, streamId);
            }
        }

        recordingLog.appendSnapshot(recordingId, leadershipTermId, baseLogPosition, termPosition, timestampMs);
    }

    private void snapshotState(final Publication publication, final long logPosition)
    {
        final ServiceSnapshotTaker snapshotTaker = new ServiceSnapshotTaker(publication, idleStrategy, null);

        snapshotTaker.markBegin(ClusteredServiceContainer.SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0);

        for (final ClientSession clientSession : sessionByIdMap.values())
        {
            snapshotTaker.snapshotSession(clientSession);
        }

        snapshotTaker.markEnd(ClusteredServiceContainer.SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0);
    }

    private void executeAction(final ClusterAction action, final long termPosition)
    {
        if (isRecovering)
        {
            return;
        }

        final long logPosition = baseLogPosition + termPosition;

        switch (action)
        {
            case SNAPSHOT:
                onTakeSnapshot(termPosition);
                serviceControlPublisher.ackAction(logPosition, leadershipTermId, serviceId, action);
                break;

            case SHUTDOWN:
                onTakeSnapshot(termPosition);
                ctx.recordingLog().commitLeadershipTermPosition(leadershipTermId, termPosition);
                serviceControlPublisher.ackAction(logPosition, leadershipTermId, serviceId, action);
                ctx.terminationHook().run();
                break;

            case ABORT:
                ctx.recordingLog().commitLeadershipTermPosition(leadershipTermId, termPosition);
                serviceControlPublisher.ackAction(logPosition, leadershipTermId, serviceId, action);
                ctx.terminationHook().run();
                break;
        }
    }

    private int awaitRecordingCounter(final Publication publication, final CountersReader counters)
    {
        int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
        while (CountersReader.NULL_COUNTER_ID == counterId)
        {
            checkInterruptedStatus();
            idleStrategy.idle();
            counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
        }

        return counterId;
    }

    private static void checkInterruptedStatus()
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new AgentTerminationException("Unexpected interrupt during operation");
        }
    }

    static class ActiveLog
    {
        final long leadershipTermId;
        final int commitPositionId;
        final int sessionId;
        final int streamId;
        final String channel;

        ActiveLog(
            final long leadershipTermId,
            final int commitPositionId,
            final int sessionId,
            final int streamId,
            final String channel)
        {
            this.leadershipTermId = leadershipTermId;
            this.commitPositionId = commitPositionId;
            this.sessionId = sessionId;
            this.streamId = streamId;
            this.channel = channel;
        }
    }
}
