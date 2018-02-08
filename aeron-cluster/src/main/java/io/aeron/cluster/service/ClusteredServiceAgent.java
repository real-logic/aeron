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
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.Header;
import io.aeron.status.ReadableCounter;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.CountersReader;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.CommonContext.SPY_PREFIX;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;

final class ClusteredServiceAgent implements Agent, Cluster
{
    private boolean isRecovering = true;
    private final boolean shouldCloseResources;
    private final AeronArchive.Context archiveCtx;
    private final ClusteredServiceContainer.Context ctx;
    private final Aeron aeron;
    private final Subscription logSubscription;
    private final Long2ObjectHashMap<ClientSession> sessionByIdMap = new Long2ObjectHashMap<>();
    private final ClusteredService service;
    private final ConsensusModuleProxy consensusModule;
    private final IdleStrategy idleStrategy;
    private final RecordingLog recordingLog;

    private long baseLogPosition;
    private long leadershipTermId;
    private long currentRecordingId;
    private long timestampMs;
    private BoundedLogAdapter logAdapter;
    private ReadableCounter commitPosition;
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

        String logChannel = ctx.logChannel();
        logChannel = logChannel.contains(IPC_CHANNEL) ? logChannel : SPY_PREFIX + logChannel;

        logSubscription = aeron.addSubscription(logChannel, ctx.logStreamId());

        consensusModule = new ConsensusModuleProxy(
            ctx.serviceId(),
            aeron.addExclusivePublication(ctx.consensusModuleChannel(), ctx.consensusModuleStreamId()),
            idleStrategy);
    }

    public void onStart()
    {
        service.onStart(this);

        final CountersReader counters = aeron.countersReader();
        final int recoveryCounterId = findRecoveryCounterId(counters);

        checkForSnapshot(counters, recoveryCounterId);
        checkForReplay(counters, recoveryCounterId);

        isRecovering = false;
        findCommitPositionCounter(counters, logSubscription);

        leadershipTermId = CommitPos.getLeadershipTermId(counters, commitPosition.counterId());
        final int sessionId = CommitPos.getLogSessionId(counters, commitPosition.counterId());
        final Image image = logSubscription.imageBySessionId(sessionId);
        logAdapter = new BoundedLogAdapter(image, commitPosition, this);

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
            CloseHelper.close(logSubscription);
            CloseHelper.close(consensusModule);

            for (final ClientSession session : sessionByIdMap.values())
            {
                session.disconnect();
            }
        }
    }

    public int doWork()
    {
        final int workCount = logAdapter.poll();
        if (0 == workCount)
        {
            if (logAdapter.image().isClosed())
            {
                throw new AgentTerminationException("Image closed unexpectedly");
            }

            if (!CommitPos.isActive(aeron.countersReader(), commitPosition.counterId(), currentRecordingId))
            {
                throw new AgentTerminationException("Commit position is not active");
            }
        }

        return workCount;
    }

    public String roleName()
    {
        return "clustered-service";
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

    public long timeMs()
    {
        return timestampMs;
    }

    public void scheduleTimer(final long correlationId, final long deadlineMs)
    {
        consensusModule.scheduleTimer(correlationId, deadlineMs);
    }

    public void cancelTimer(final long correlationId)
    {
        consensusModule.cancelTimer(correlationId);
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
                throw new IllegalStateException("No snapshot available for term: " + termPosition);
            }

            baseLogPosition = snapshotEntry.logPosition;
            loadSnapshot(snapshotEntry.recordingId);
        }

        consensusModule.sendAcknowledgment(ClusterAction.INIT, baseLogPosition, leadershipTermId, timestampMs);
    }

    private void checkForReplay(final CountersReader counters, final int recoveryCounterId)
    {
        final long replayTermCount = RecoveryState.getReplayTermCount(counters, recoveryCounterId);
        if (0 == replayTermCount)
        {
            return;
        }

        service.onReplayBegin();

        try (Subscription subscription = aeron.addSubscription(ctx.replayChannel(), ctx.replayStreamId()))
        {
            for (int i = 0; i < replayTermCount; i++)
            {
                final int counterId = findReplayCommitPositionCounterId(counters, i);
                final int sessionId = CommitPos.getLogSessionId(counters, counterId);
                baseLogPosition = CommitPos.getBaseLogPosition(counters, counterId);
                leadershipTermId = CommitPos.getLeadershipTermId(counters, counterId);

                idleStrategy.reset();
                Image image;
                while ((image = subscription.imageBySessionId(sessionId)) == null)
                {
                    checkInterruptedStatus();
                    idleStrategy.idle();
                }

                final ReadableCounter limit = new ReadableCounter(counters, counterId);
                final BoundedLogAdapter adapter = new BoundedLogAdapter(image, limit, this);

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

                consensusModule.sendAcknowledgment(ClusterAction.INIT, baseLogPosition, leadershipTermId, timestampMs);
            }
        }

        service.onReplayEnd();
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

    private int findReplayCommitPositionCounterId(final CountersReader counters, final int replayStep)
    {
        int counterId = CommitPos.findCounterIdByReplayStep(counters, replayStep);

        while (CountersReader.NULL_COUNTER_ID == counterId)
        {
            checkInterruptedStatus();
            idleStrategy.idle();

            counterId = CommitPos.findCounterIdByReplayStep(counters, replayStep);
        }

        return counterId;
    }

    private void findCommitPositionCounter(final CountersReader counters, final Subscription logSubscription)
    {
        idleStrategy.reset();
        while (!logSubscription.isConnected())
        {
            checkInterruptedStatus();
            idleStrategy.idle();
        }

        final int sessionId = logSubscription.imageAtIndex(0).sessionId();

        int counterId = CommitPos.findCounterIdBySession(counters, sessionId);
        while (CountersReader.NULL_COUNTER_ID == counterId)
        {
            checkInterruptedStatus();
            idleStrategy.idle();

            counterId = CommitPos.findCounterIdBySession(counters, sessionId);
        }

        currentRecordingId = CommitPos.getRecordingId(counters, counterId);
        commitPosition = new ReadableCounter(counters, counterId);
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

            final String replaySubscriptionChannel = ChannelUri.addSessionId(channel, sessionId);
            try (Subscription subscription = aeron.addSubscription(replaySubscriptionChannel, streamId))
            {
                Image image;
                while ((image = subscription.imageBySessionId(sessionId)) == null)
                {
                    checkInterruptedStatus();
                    idleStrategy.idle();
                }

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
                int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
                while (CountersReader.NULL_COUNTER_ID == counterId)
                {
                    checkInterruptedStatus();
                    idleStrategy.idle();
                    counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
                }

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
                consensusModule.sendAcknowledgment(action, logPosition, leadershipTermId, timestampMs);
                break;

            case SHUTDOWN:
                onTakeSnapshot(termPosition);
                ctx.recordingLog().commitLeadershipTermPosition(leadershipTermId, termPosition);
                consensusModule.sendAcknowledgment(action, logPosition, leadershipTermId, timestampMs);
                ctx.terminationHook().run();
                break;

            case ABORT:
                ctx.recordingLog().commitLeadershipTermPosition(leadershipTermId, termPosition);
                consensusModule.sendAcknowledgment(action, logPosition, leadershipTermId, timestampMs);
                ctx.terminationHook().run();
                break;
        }
    }

    private static void checkInterruptedStatus()
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new AgentTerminationException("Unexpected interrupt during operation");
        }
    }
}
