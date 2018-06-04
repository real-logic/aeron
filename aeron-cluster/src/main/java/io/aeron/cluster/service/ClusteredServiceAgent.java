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

import static io.aeron.Aeron.NULL_VALUE;
import static java.util.Collections.unmodifiableCollection;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;

class ClusteredServiceAgent implements Agent, Cluster
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
    private final ConsensusModuleProxy consensusModuleProxy;
    private final ServiceAdapter serviceAdapter;
    private final IdleStrategy idleStrategy;
    private final EpochClock epochClock;
    private final CachedEpochClock cachedEpochClock = new CachedEpochClock();
    private final ClusterMarkFile markFile;

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
        idleStrategy = ctx.idleStrategy();
        serviceId = ctx.serviceId();
        epochClock = ctx.epochClock();
        markFile = ctx.clusterMarkFile();

        final String channel = ctx.serviceControlChannel();
        consensusModuleProxy = new ConsensusModuleProxy(aeron.addPublication(channel, ctx.consensusModuleStreamId()));
        serviceAdapter = new ServiceAdapter(aeron.addSubscription(channel, ctx.serviceStreamId()), this);
    }

    public void onStart()
    {
        final CountersReader counters = aeron.countersReader();
        roleCounter = awaitClusterRoleCounter(counters);
        heartbeatCounter = awaitHeartbeatCounter(counters);

        service.onStart(this);
        isRecovering = true;
        final int recoveryCounterId = awaitRecoveryCounter(counters);
        heartbeatCounter.setOrdered(epochClock.time());
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
            CloseHelper.close(consensusModuleProxy);
            CloseHelper.close(serviceAdapter);

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
            workCount += serviceAdapter.poll();

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

        if (consensusModuleProxy.closeSession(clusterSessionId))
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
        return consensusModuleProxy.scheduleTimer(correlationId, deadlineMs);
    }

    public boolean cancelTimer(final long correlationId)
    {
        return consensusModuleProxy.cancelTimer(correlationId);
    }

    public void idle()
    {
        checkInterruptedStatus();
        idleStrategy.idle();
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
            clusterSessionId, responseStreamId, responseChannel, encodedPrincipal, this);

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

    void onServiceAction(final long logPosition, final long timestampMs, final ClusterAction action)
    {
        this.timestampMs = timestampMs;

        executeAction(action, logPosition);
    }

    void addSession(
        final long clusterSessionId,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedPrincipal)
    {
        sessionByIdMap.put(clusterSessionId, new ClientSession(
            clusterSessionId, responseStreamId, responseChannel, encodedPrincipal, this));
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
        leadershipTermId = RecoveryState.getLeadershipTermId(counters, recoveryCounterId);
        timestampMs = RecoveryState.getTimestamp(counters, recoveryCounterId);

        if (NULL_VALUE != leadershipTermId)
        {
            loadSnapshot(RecoveryState.getSnapshotRecordingId(counters, recoveryCounterId, serviceId));
        }

        final long logPosition = RecoveryState.getLogPosition(counters, recoveryCounterId);
        heartbeatCounter.setOrdered(epochClock.time());
        consensusModuleProxy.ack(logPosition, leadershipTermId, serviceId);
    }

    private void checkForReplay(final CountersReader counters, final int recoveryCounterId)
    {
        if (RecoveryState.hasReplay(counters, recoveryCounterId))
        {
            service.onReplayBegin();
            awaitActiveLog();

            final int counterId = newActiveLogEvent.commitPositionId;
            leadershipTermId = CommitPos.getLeadershipTermId(counters, counterId);
            long logPosition = CommitPos.getLogPosition(counters, counterId);

            try (Subscription subscription = aeron.addSubscription(
                newActiveLogEvent.channel, newActiveLogEvent.streamId))
            {
                consensusModuleProxy.ack(logPosition, leadershipTermId, serviceId);

                final Image image = awaitImage(newActiveLogEvent.sessionId, subscription);
                final ReadableCounter limit = new ReadableCounter(counters, counterId);
                final BoundedLogAdapter adapter = new BoundedLogAdapter(image, limit, this);

                logPosition = CommitPos.getMaxLogPosition(counters, counterId);
                consumeImage(image, adapter, logPosition);
            }

            newActiveLogEvent = null;
            heartbeatCounter.setOrdered(epochClock.time());
            consensusModuleProxy.ack(logPosition, leadershipTermId, serviceId);
            service.onReplayEnd();
        }
    }

    private void awaitActiveLog()
    {
        idleStrategy.reset();
        while (null == newActiveLogEvent)
        {
            serviceAdapter.poll();
            checkInterruptedStatus();
            idleStrategy.idle();
        }
    }

    private void consumeImage(final Image image, final BoundedLogAdapter adapter, final long maxLogPosition)
    {
        while (true)
        {
            final int workCount = adapter.poll();
            if (workCount == 0)
            {
                if (image.position() == maxLogPosition)
                {
                    break;
                }

                if (image.isClosed())
                {
                    throw new IllegalStateException("unexpected close of replay");
                }
            }

            checkInterruptedStatus();
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
        final long logPosition = CommitPos.getLogPosition(counters, commitPositionId);

        final Subscription logSubscription = aeron.addSubscription(
            newActiveLogEvent.channel, newActiveLogEvent.streamId);

        if (newActiveLogEvent.ackBeforeImage)
        {
            consensusModuleProxy.ack(logPosition, leadershipTermId, serviceId);
        }

        final Image image = awaitImage(logSessionId, logSubscription);
        heartbeatCounter.setOrdered(epochClock.time());

        if (!newActiveLogEvent.ackBeforeImage)
        {
            consensusModuleProxy.ack(logPosition, leadershipTermId, serviceId);
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

    private AtomicCounter awaitHeartbeatCounter(final CountersReader counters)
    {
        idleStrategy.reset();
        int counterId = ServiceHeartbeat.findCounterId(counters, ctx.serviceId());
        while (NULL_COUNTER_ID == counterId)
        {
            checkInterruptedStatus();
            idleStrategy.idle();
            counterId = ServiceHeartbeat.findCounterId(counters, ctx.serviceId());
        }

        return new AtomicCounter(counters.valuesBuffer(), counterId);
    }

    private void loadSnapshot(final long recordingId)
    {
        try (AeronArchive archive = AeronArchive.connect(archiveCtx))
        {
            final String channel = ctx.replayChannel();
            final int streamId = ctx.replayStreamId();
            final int sessionId = (int)archive.startReplay(recordingId, 0, NULL_VALUE, channel, streamId);

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

    private long onTakeSnapshot(final long logPosition)
    {
        final long recordingId;

        try (AeronArchive archive = AeronArchive.connect(archiveCtx);
            Publication publication = archive.addRecordedExclusivePublication(
                ctx.snapshotChannel(), ctx.snapshotStreamId()))
        {
            try
            {
                final CountersReader counters = aeron.countersReader();
                final int counterId = awaitRecordingCounter(publication, counters);

                recordingId = RecordingPos.getRecordingId(counters, counterId);
                snapshotState(publication, logPosition);
                service.onTakeSnapshot(publication);

                awaitRecordingComplete(recordingId, publication.position(), counters, counterId, archive);
            }
            finally
            {
                archive.stopRecording(publication);
            }
        }

        return recordingId;
    }

    private void awaitRecordingComplete(
        final long recordingId,
        final long position,
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
        while (counters.getCounterValue(counterId) < position);
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

    private void executeAction(final ClusterAction action, final long logPosition)
    {
        if (isRecovering)
        {
            return;
        }

        switch (action)
        {
            case SNAPSHOT:
                consensusModuleProxy.ack(logPosition, leadershipTermId, onTakeSnapshot(logPosition), serviceId);
                break;

            case SHUTDOWN:
                consensusModuleProxy.ack(logPosition, leadershipTermId, onTakeSnapshot(logPosition), serviceId);
                ctx.terminationHook().run();
                break;

            case ABORT:
                consensusModuleProxy.ack(logPosition, leadershipTermId, serviceId);
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

    private static void checkInterruptedStatus()
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new AgentTerminationException("unexpected interrupt during operation");
        }
    }
}
