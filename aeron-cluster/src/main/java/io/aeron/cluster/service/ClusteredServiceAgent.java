/*
 * Copyright 2014-2018 Real Logic Ltd.
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
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.Header;
import io.aeron.status.ReadableCounter;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

import java.util.Collection;

import static io.aeron.archive.client.AeronArchive.NULL_LENGTH;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.codecs.ClusterAction.READY;
import static io.aeron.cluster.codecs.ClusterAction.REPLAY;
import static java.util.Collections.unmodifiableCollection;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;

class ClusteredServiceAgent implements Agent, Cluster, ServiceControlListener
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
    private final ClusterMarkFile markFile;

    private long termBaseLogPosition;
    private long leadershipTermId;
    private long timestampMs;
    private BoundedLogAdapter logAdapter;
    private NewActiveLogEvent newActiveLogEvent;
    private ReadableCounter roleCounter;
    private AtomicCounter heartbeatCounter;
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
        markFile = ctx.clusterMarkFile();

        final String channel = ctx.serviceControlChannel();
        final int streamId = ctx.serviceControlStreamId();
        serviceControlPublisher = new ServiceControlPublisher(aeron.addPublication(channel, streamId));
        serviceControlAdapter = new ServiceControlAdapter(aeron.addSubscription(channel, streamId), this);
    }

    public void onStart()
    {
        final CountersReader counters = aeron.countersReader();
        roleCounter = awaitClusterRoleCounter(counters);
        findHeartbeatCounter(counters);

        service.onStart(this);
        isRecovering = true;
        final int recoveryCounterId = awaitRecoveryCounter(counters);
        checkForSnapshot(counters, recoveryCounterId);
        checkForReplay(counters, recoveryCounterId);
        isRecovering = false;
        service.onReady();
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
        int workCount = 0;

        final long nowMs = epochClock.time();
        if (cachedEpochClock.time() != nowMs)
        {
            cachedEpochClock.update(nowMs);
            markFile.updateActivityTimestamp(nowMs);
            checkHealthAndUpdateHeartbeat(nowMs);
            workCount += serviceControlAdapter.poll();

            if (newActiveLogEvent != null)
            {
                joinActiveLog();
            }
        }

        workCount += null != logAdapter ? logAdapter.poll() : 0;

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

    public boolean closeSession(final long clusterSessionId)
    {
        final ClientSession clientSession = sessionByIdMap.get(clusterSessionId);
        if (clientSession == null)
        {
            throw new IllegalArgumentException("unknown clusterSessionId: " + clusterSessionId);
        }

        if (clientSession.isClosing())
        {
            return true;
        }

        if (serviceControlPublisher.closeSession(clusterSessionId))
        {
            clientSession.markClosing();
            return true;
        }

        return false;
    }

    public long timeMs()
    {
        return timestampMs;
    }

    public boolean scheduleTimer(final long correlationId, final long deadlineMs)
    {
        return serviceControlPublisher.scheduleTimer(correlationId, deadlineMs);
    }

    public boolean cancelTimer(final long correlationId)
    {
        return serviceControlPublisher.cancelTimer(correlationId);
    }

    public void onJoinLog(
        final long leadershipTermId,
        final int commitPositionId,
        final int logSessionId,
        final int logStreamId,
        final boolean ackBeforeImage,
        final String logChannel)
    {
        newActiveLogEvent = new NewActiveLogEvent(
            leadershipTermId, commitPositionId, logSessionId, logStreamId, ackBeforeImage, logChannel);
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
        final byte[] encodedPrincipal)
    {
        this.timestampMs = timestampMs;

        final ClientSession session = new ClientSession(
            clusterSessionId,
            responseStreamId,
            responseChannel,
            encodedPrincipal,
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
        final byte[] encodedPrincipal)
    {
        final ClientSession session = new ClientSession(
            clusterSessionId,
            responseStreamId,
            responseChannel,
            encodedPrincipal,
            ClusteredServiceAgent.this);

        sessionByIdMap.put(clusterSessionId, session);
    }

    private void checkHealthAndUpdateHeartbeat(final long nowMs)
    {
        if (null == logAdapter || !logAdapter.image().isClosed())
        {
            heartbeatCounter.setOrdered(nowMs);
        }
    }

    private void role(final Role newRole)
    {
        if (newRole != role)
        {
            role = newRole;
            service.onRoleChange(newRole);
        }
    }

    private void checkForSnapshot(final CountersReader counters, final int recoveryCounterId)
    {
        final long termPosition = RecoveryState.getTermPosition(counters, recoveryCounterId);
        leadershipTermId = RecoveryState.getLeadershipTermId(counters, recoveryCounterId);
        timestampMs = RecoveryState.getTimestamp(counters, recoveryCounterId);
        long recordingId = ServiceControlListener.NULL_VALUE;

        if (NULL_POSITION != termPosition)
        {
            final RecordingLog.Entry snapshot = recordingLog.getSnapshot(leadershipTermId, termPosition, serviceId);
            if (null == snapshot)
            {
                throw new IllegalStateException("no snapshot available for term position: " + termPosition);
            }

            recordingId = snapshot.recordingId;
            termBaseLogPosition = snapshot.termBaseLogPosition + snapshot.termPosition;
            loadSnapshot(recordingId);
        }

        serviceControlPublisher.ackAction(
            termBaseLogPosition, leadershipTermId, recordingId, serviceId, ClusterAction.INIT);
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
            final int counterId = newActiveLogEvent.commitPositionId;
            leadershipTermId = CommitPos.getLeadershipTermId(counters, counterId);
            termBaseLogPosition = CommitPos.getTermBaseLogPosition(counters, counterId);

            if (CommitPos.getLeadershipTermLength(counters, counterId) > 0)
            {
                try (Subscription subscription = aeron.addSubscription(
                    newActiveLogEvent.channel, newActiveLogEvent.streamId))
                {
                    serviceControlPublisher.ackAction(termBaseLogPosition, leadershipTermId, serviceId, READY);

                    final Image image = awaitImage(newActiveLogEvent.sessionId, subscription);
                    final ReadableCounter limit = new ReadableCounter(counters, counterId);
                    final BoundedLogAdapter adapter = new BoundedLogAdapter(image, limit, this);

                    consumeImage(image, adapter);

                    termBaseLogPosition += image.position();
                }
            }

            newActiveLogEvent = null;
            serviceControlPublisher.ackAction(termBaseLogPosition, leadershipTermId, serviceId, REPLAY);
        }

        service.onReplayEnd();
    }

    private void awaitActiveLog()
    {
        while (null == newActiveLogEvent)
        {
            checkInterruptedStatus();
            idleStrategy.idle(serviceControlAdapter.poll());
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
                        throw new IllegalStateException("unexpected close of replay");
                    }

                    break;
                }

                checkInterruptedStatus();
            }

            idleStrategy.idle(workCount);
        }
    }

    private int awaitRecoveryCounter(final CountersReader counters)
    {
        idleStrategy.reset();
        int counterId = RecoveryState.findCounterId(counters);
        while (NULL_COUNTER_ID == counterId)
        {
            checkInterruptedStatus();
            idleStrategy.idle();

            counterId = RecoveryState.findCounterId(counters);
        }

        return counterId;
    }

    private void joinActiveLog()
    {
        if (null != logAdapter)
        {
            if (!logAdapter.isCaughtUp())
            {
                return;
            }

            logAdapter.close();
            logAdapter = null;
        }

        final CountersReader counters = aeron.countersReader();

        final int commitPositionId = newActiveLogEvent.commitPositionId;
        if (!CommitPos.isActive(counters, commitPositionId))
        {
            throw new IllegalStateException("CommitPos counter not active: " + commitPositionId);
        }

        final int logSessionId = newActiveLogEvent.sessionId;
        leadershipTermId = newActiveLogEvent.leadershipTermId;
        termBaseLogPosition = CommitPos.getTermBaseLogPosition(counters, commitPositionId);

        final Subscription logSubscription = aeron.addSubscription(
            newActiveLogEvent.channel, newActiveLogEvent.streamId);

        if (newActiveLogEvent.ackBeforeImage)
        {
            serviceControlPublisher.ackAction(termBaseLogPosition, leadershipTermId, serviceId, READY);
        }

        final Image image = awaitImage(logSessionId, logSubscription);
        heartbeatCounter.setOrdered(epochClock.time());

        if (!newActiveLogEvent.ackBeforeImage)
        {
            serviceControlPublisher.ackAction(termBaseLogPosition, leadershipTermId, serviceId, READY);
        }

        newActiveLogEvent = null;
        logAdapter = new BoundedLogAdapter(image, new ReadableCounter(counters, commitPositionId), this);

        role(Role.get((int)roleCounter.get()));

        for (final ClientSession session : sessionByIdMap.values())
        {
            if (Role.LEADER == role)
            {
                session.connect(aeron);
            }
            else
            {
                session.disconnect();
            }
        }
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

    private ReadableCounter awaitClusterRoleCounter(final CountersReader counters)
    {
        idleStrategy.reset();
        int counterId = ClusterNodeRole.findCounterId(counters);
        while (NULL_COUNTER_ID == counterId)
        {
            checkInterruptedStatus();
            idleStrategy.idle();
            counterId = ClusterNodeRole.findCounterId(counters);
        }

        return new ReadableCounter(counters, counterId);
    }

    private void loadSnapshot(final long recordingId)
    {
        try (AeronArchive archive = AeronArchive.connect(archiveCtx))
        {
            final String channel = ctx.replayChannel();
            final int streamId = ctx.replayStreamId();
            final int sessionId = (int)archive.startReplay(recordingId, 0, NULL_LENGTH, channel, streamId);

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
                    throw new IllegalStateException("snapshot ended unexpectedly");
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
            Publication publication = archive.addRecordedExclusivePublication(channel, streamId))
        {
            try
            {
                final CountersReader counters = aeron.countersReader();
                final int counterId = awaitRecordingCounter(publication, counters);

                recordingId = RecordingPos.getRecordingId(counters, counterId);
                snapshotState(publication, termBaseLogPosition + termPosition);
                service.onTakeSnapshot(publication);

                awaitRecordingComplete(recordingId, publication.position(), counters, counterId, archive);
            }
            finally
            {
                archive.stopRecording(publication);
            }
        }

        recordingLog.appendSnapshot(
            recordingId, leadershipTermId, termBaseLogPosition, termPosition, timestampMs, serviceId);
    }

    private void awaitRecordingComplete(
        final long recordingId,
        final long completePosition,
        final CountersReader counters,
        final int counterId,
        final AeronArchive archive)
    {
        idleStrategy.reset();
        do
        {
            idleStrategy.idle();
            checkInterruptedStatus();

            if (!RecordingPos.isActive(counters, counterId, recordingId))
            {
                throw new IllegalStateException("recording has stopped unexpectedly: " + recordingId);
            }

            archive.checkForErrorResponse();
        }
        while (counters.getCounterValue(counterId) < completePosition);
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

        final long logPosition = termBaseLogPosition + termPosition;

        switch (action)
        {
            case SNAPSHOT:
                onTakeSnapshot(termPosition);
                serviceControlPublisher.ackAction(logPosition, leadershipTermId, serviceId, action);
                break;

            case SHUTDOWN:
                onTakeSnapshot(termPosition);
                serviceControlPublisher.ackAction(logPosition, leadershipTermId, serviceId, action);
                ctx.terminationHook().run();
                break;

            case ABORT:
                serviceControlPublisher.ackAction(logPosition, leadershipTermId, serviceId, action);
                ctx.terminationHook().run();
                break;
        }
    }

    private int awaitRecordingCounter(final Publication publication, final CountersReader counters)
    {
        idleStrategy.reset();
        int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
        while (NULL_COUNTER_ID == counterId)
        {
            checkInterruptedStatus();
            idleStrategy.idle();
            counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
        }

        return counterId;
    }

    private void findHeartbeatCounter(final CountersReader counters)
    {
        final int heartbeatCounterId = ServiceHeartbeat.findCounterId(counters, ctx.serviceId());
        if (NULL_COUNTER_ID == heartbeatCounterId)
        {
            throw new IllegalStateException("failed to find heartbeat counter");
        }

        heartbeatCounter = new AtomicCounter(counters.valuesBuffer(), heartbeatCounterId);
    }

    private static void checkInterruptedStatus()
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new AgentTerminationException("unexpected interrupt during operation");
        }
    }
}
