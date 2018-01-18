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

final class ClusteredServiceAgent implements Agent, Cluster
{
    enum State
    {
        INIT, REPLAY, ACTIVE, SNAPSHOT, CLOSED
    }

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

    private long leadershipTermBeginPosition = 0;
    private long leadershipTermId;
    private long currentRecordingId;
    private long timestampMs;
    private BoundedLogAdapter logAdapter;
    private ReadableCounter consensusPosition;
    private State state = State.INIT;
    private Role role = Role.CANDIDATE;

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
        final int counterId = findRecoveryCounterId(counters);

        checkForSnapshot(counters, counterId);
        checkForReplay(counters, counterId);

        findConsensusPosition(counters, logSubscription);

        final int sessionId = ConsensusPos.getSessionId(counters, consensusPosition.counterId());
        final Image image = logSubscription.imageBySessionId(sessionId);
        logAdapter = new BoundedLogAdapter(image, consensusPosition, this);

        state = State.ACTIVE;
        role = Role.LEADER;

        for (final ClientSession session : sessionByIdMap.values())
        {
            session.connect(aeron);
        }
    }

    public void onClose()
    {
        state = State.CLOSED;

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
                throw new IllegalStateException("Image closed unexpectedly");
            }

            if (!ConsensusPos.isActive(aeron.countersReader(), consensusPosition.counterId(), currentRecordingId))
            {
                throw new IllegalStateException("Consensus position is not active");
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

    public boolean isReplay()
    {
        return State.REPLAY == state;
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

    void onServiceAction(final long resultingPosition, final long timestampMs, final ServiceAction action)
    {
        this.timestampMs = timestampMs;

        executeAction(action, leadershipTermBeginPosition + resultingPosition);
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
        final long logPosition = RecoveryState.getLogPosition(counters, recoveryCounterId);

        if (logPosition > 0)
        {
            final RecordingLog.Entry snapshotEntry = recordingLog.getSnapshotByPosition(logPosition);
            if (null == snapshotEntry)
            {
                throw new IllegalStateException("No snapshot available for position: " + logPosition);
            }

            leadershipTermBeginPosition = logPosition;
            leadershipTermId = RecoveryState.getLeadershipTermId(counters, recoveryCounterId);
            timestampMs = RecoveryState.getTimestamp(counters, recoveryCounterId);

            snapshotEntry.confirmMatch(logPosition, leadershipTermId, timestampMs);
            loadSnapshot(snapshotEntry.recordingId);
        }

        consensusModule.sendAcknowledgment(
            ServiceAction.INIT, leadershipTermBeginPosition, leadershipTermId, timestampMs);
    }

    private void checkForReplay(final CountersReader counters, final int recoveryCounterId)
    {
        final long replayTermCount = RecoveryState.getReplayTermCount(counters, recoveryCounterId);
        if (0 == replayTermCount)
        {
            return;
        }

        state = State.REPLAY;

        try (Subscription subscription = aeron.addSubscription(ctx.replayChannel(), ctx.replayStreamId()))
        {
            for (int i = 0; i < replayTermCount; i++)
            {
                final int counterId = findReplayConsensusCounterId(counters, i);
                final int sessionId = ConsensusPos.getSessionId(counters, counterId);
                leadershipTermBeginPosition = ConsensusPos.getBeginningLogPosition(counters, counterId);

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

                consensusModule.sendAcknowledgment(
                    ServiceAction.REPLAY, leadershipTermBeginPosition, leadershipTermId, timestampMs);
            }
        }
    }

    private int findRecoveryCounterId(final CountersReader counters)
    {
        int counterId = RecoveryState.findCounterId(counters);

        while (RecoveryState.NULL_COUNTER_ID == counterId)
        {
            checkInterruptedStatus();
            idleStrategy.idle();

            counterId = RecoveryState.findCounterId(counters);
        }

        return counterId;
    }

    private int findReplayConsensusCounterId(final CountersReader counters, final int replayStep)
    {
        int counterId = ConsensusPos.findCounterIdByReplayStep(counters, replayStep);

        while (RecoveryState.NULL_COUNTER_ID == counterId)
        {
            checkInterruptedStatus();
            idleStrategy.idle();

            counterId = ConsensusPos.findCounterIdByReplayStep(counters, replayStep);
        }

        return counterId;
    }

    private void findConsensusPosition(final CountersReader counters, final Subscription logSubscription)
    {
        idleStrategy.reset();
        while (!logSubscription.isConnected())
        {
            checkInterruptedStatus();
            idleStrategy.idle();
        }

        final int sessionId = logSubscription.imageAtIndex(0).sessionId();

        int counterId = ConsensusPos.findCounterIdBySession(counters, sessionId);
        while (ConsensusPos.NULL_COUNTER_ID == counterId)
        {
            checkInterruptedStatus();
            idleStrategy.idle();

            counterId = ConsensusPos.findCounterIdBySession(counters, sessionId);
        }

        currentRecordingId = ConsensusPos.getRecordingId(counters, counterId);
        consensusPosition = new ReadableCounter(counters, counterId);
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

            try (Subscription subscription = aeron.addSubscription(channel, streamId))
            {
                final long length = recordingExtent.stopPosition - recordingExtent.startPosition;
                final int sessionId = (int)archive.startReplay(recordingId, 0, length, channel, streamId);

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
        final SnapshotLoader snapshotLoader = new SnapshotLoader(image, this);
        while (snapshotLoader.inProgress())
        {
            final int fragments = snapshotLoader.poll();
            if (fragments == 0)
            {
                checkInterruptedStatus();

                if (image.isClosed() && snapshotLoader.inProgress())
                {
                    throw new IllegalStateException("Snapshot ended unexpectedly");
                }

                idleStrategy.idle(fragments);
            }
        }
    }

    private void onTakeSnapshot(final long logPosition)
    {
        state = State.SNAPSHOT;
        final long recordingId;

        try (AeronArchive archive = AeronArchive.connect(archiveCtx))
        {
            final String channel = ctx.snapshotChannel();
            final int streamId = ctx.snapshotStreamId();

            archive.startRecording(channel, streamId, SourceLocation.LOCAL);

            try (Publication publication = aeron.addExclusivePublication(channel, streamId))
            {
                idleStrategy.reset();

                final CountersReader counters = aeron.countersReader();
                int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
                while (RecordingPos.NULL_COUNTER_ID == counterId)
                {
                    checkInterruptedStatus();
                    idleStrategy.idle();
                    counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
                }

                recordingId = RecordingPos.getRecordingId(counters, counterId);
                snapshotState(publication, logPosition);
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
                archive.stopRecording(channel, streamId);
            }
        }

        recordingLog.appendSnapshot(recordingId, logPosition, leadershipTermId, timestampMs);
        state = State.ACTIVE;
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

    private void executeAction(final ServiceAction action, final long position)
    {
        if (State.ACTIVE != state)
        {
            return;
        }

        switch (action)
        {
            case SNAPSHOT:
                onTakeSnapshot(position);
                consensusModule.sendAcknowledgment(ServiceAction.SNAPSHOT, position, leadershipTermId, timestampMs);
                break;

            case SHUTDOWN:
                onTakeSnapshot(position);
                consensusModule.sendAcknowledgment(ServiceAction.SHUTDOWN, position, leadershipTermId, timestampMs);
                state = State.CLOSED;
                ctx.terminationHook().run();
                break;

            case ABORT:
                consensusModule.sendAcknowledgment(ServiceAction.ABORT, position, leadershipTermId, timestampMs);
                state = State.CLOSED;
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
