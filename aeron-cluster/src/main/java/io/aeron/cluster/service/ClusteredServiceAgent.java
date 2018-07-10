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
import io.aeron.cluster.client.ClusterException;
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
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static java.util.Collections.unmodifiableCollection;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;

class ClusteredServiceAgent implements Agent, Cluster
{
    private final int serviceId;
    private boolean isRecovering;
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
    private final ClusterMarkFile markFile;

    private long ackId = 0;
    private long clusterTimeMs;
    private long cachedTimeMs;
    private BoundedLogAdapter logAdapter;
    private ActiveLogEvent activeLogEvent;
    private AtomicCounter heartbeatCounter;
    private ReadableCounter roleCounter;
    private Role role = Role.FOLLOWER;

    ClusteredServiceAgent(final ClusteredServiceContainer.Context ctx)
    {
        this.ctx = ctx;

        archiveCtx = ctx.archiveContext();
        aeron = ctx.aeron();
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
    }

    public void onClose()
    {
        if (!ctx.ownsAeronClient())
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

        if (checkForClockTick())
        {
            pollServiceAdapter();
            workCount += 1;
        }

        if (null != logAdapter)
        {
            final int polled = logAdapter.poll();
            if (0 == polled)
            {
                if (logAdapter.isConsumed(aeron.countersReader()))
                {
                    consensusModuleProxy.ack(logAdapter.position(), ackId++, serviceId);
                    logAdapter.close();
                    logAdapter = null;
                }
                else if (logAdapter.isImageClosed())
                {
                    logAdapter.close();
                    logAdapter = null;
                }
            }

            workCount += polled;
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

    public boolean closeSession(final long clusterSessionId)
    {
        final ClientSession clientSession = sessionByIdMap.get(clusterSessionId);
        if (clientSession == null)
        {
            throw new ClusterException("unknown clusterSessionId: " + clusterSessionId);
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
        return clusterTimeMs;
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
        if (!checkForClockTick())
        {
            checkInterruptedStatus();
            idleStrategy.idle();
        }
    }

    public void onJoinLog(
        final long leadershipTermId,
        final int commitPositionId,
        final int logSessionId,
        final int logStreamId,
        final String logChannel)
    {
        activeLogEvent = new ActiveLogEvent(leadershipTermId, commitPositionId, logSessionId, logStreamId, logChannel);
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
        this.clusterTimeMs = timestampMs;
        final ClientSession clientSession = sessionByIdMap.get(clusterSessionId);

        try
        {
            service.onSessionMessage(
                clientSession,
                correlationId,
                timestampMs,
                buffer,
                offset,
                length,
                header);
        }
        finally
        {
            clientSession.lastCorrelationId(correlationId);
        }
    }

    void onTimerEvent(final long correlationId, final long timestampMs)
    {
        this.clusterTimeMs = timestampMs;

        service.onTimerEvent(correlationId, timestampMs);
    }

    void onSessionOpen(
        final long clusterSessionId,
        final long correlationId,
        final long timestampMs,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedPrincipal)
    {
        this.clusterTimeMs = timestampMs;

        final ClientSession session = new ClientSession(
            clusterSessionId, correlationId, responseStreamId, responseChannel, encodedPrincipal, this);

        if (Role.LEADER == role)
        {
            session.connect(aeron);
        }

        sessionByIdMap.put(clusterSessionId, session);
        service.onSessionOpen(session, timestampMs);
    }

    void onSessionClose(final long clusterSessionId, final long timestampMs, final CloseReason closeReason)
    {
        this.clusterTimeMs = timestampMs;

        final ClientSession session = sessionByIdMap.remove(clusterSessionId);
        session.disconnect();
        service.onSessionClose(session, timestampMs, closeReason);
    }

    void onServiceAction(
        final long logPosition, final long leadershipTermId, final long timestampMs, final ClusterAction action)
    {
        this.clusterTimeMs = timestampMs;

        executeAction(action, logPosition, leadershipTermId);
    }

    @SuppressWarnings("unused")
    void onNewLeadershipTermEvent(
        final long leadershipTermId,
        final long logPosition,
        final long timestampMs,
        final int leaderMemberId,
        final int logSessionId)
    {
        this.clusterTimeMs = timestampMs;
    }

    void addSession(
        final long clusterSessionId,
        final long lastCorrelationId,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedPrincipal)
    {
        sessionByIdMap.put(clusterSessionId, new ClientSession(
            clusterSessionId, lastCorrelationId, responseStreamId, responseChannel, encodedPrincipal, this));
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
        final long leadershipTermId = RecoveryState.getLeadershipTermId(counters, recoveryCounterId);
        clusterTimeMs = RecoveryState.getTimestamp(counters, recoveryCounterId);

        if (NULL_VALUE != leadershipTermId)
        {
            loadSnapshot(RecoveryState.getSnapshotRecordingId(counters, recoveryCounterId, serviceId));
        }

        heartbeatCounter.setOrdered(epochClock.time());
        consensusModuleProxy.ack(RecoveryState.getLogPosition(counters, recoveryCounterId), ackId++, serviceId);
    }

    private void checkForReplay(final CountersReader counters, final int recoveryCounterId)
    {
        if (RecoveryState.hasReplay(counters, recoveryCounterId))
        {
            awaitActiveLog();

            final int counterId = activeLogEvent.commitPositionId;

            try (Subscription subscription = aeron.addSubscription(activeLogEvent.channel, activeLogEvent.streamId))
            {
                consensusModuleProxy.ack(CommitPos.getLogPosition(counters, counterId), ackId++, serviceId);

                final Image image = awaitImage(activeLogEvent.sessionId, subscription);
                final ReadableCounter limit = new ReadableCounter(counters, counterId);
                final BoundedLogAdapter adapter = new BoundedLogAdapter(image, limit, this);

                consumeImage(image, adapter);
            }

            activeLogEvent = null;
            heartbeatCounter.setOrdered(epochClock.time());
        }
    }

    private void awaitActiveLog()
    {
        idleStrategy.reset();
        while (null == activeLogEvent)
        {
            serviceAdapter.poll();
            checkInterruptedStatus();
            idleStrategy.idle();
        }
    }

    private void consumeImage(final Image image, final BoundedLogAdapter adapter)
    {
        while (true)
        {
            final int workCount = adapter.poll();
            if (workCount == 0)
            {
                if (adapter.isConsumed(aeron.countersReader()))
                {
                    consensusModuleProxy.ack(image.position(), ackId++, serviceId);
                    break;
                }

                if (image.isClosed())
                {
                    throw new ClusterException("unexpected close of replay");
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
        final CountersReader counters = aeron.countersReader();
        final int commitPositionId = activeLogEvent.commitPositionId;
        if (!CommitPos.isActive(counters, commitPositionId))
        {
            throw new ClusterException("CommitPos counter not active: " + commitPositionId);
        }

        final Subscription logSubscription = aeron.addSubscription(activeLogEvent.channel, activeLogEvent.streamId);
        consensusModuleProxy.ack(CommitPos.getLogPosition(counters, commitPositionId), ackId++, serviceId);

        final Image image = awaitImage(activeLogEvent.sessionId, logSubscription);
        heartbeatCounter.setOrdered(epochClock.time());

        activeLogEvent = null;
        logAdapter = new BoundedLogAdapter(image, new ReadableCounter(counters, commitPositionId), this);

        role(Role.get((int)roleCounter.get()));

        for (final ClientSession session : sessionByIdMap.values())
        {
            if (Role.LEADER == role)
            {
                session.connect(aeron);
                session.resetClosing();
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
                    throw new ClusterException("snapshot ended unexpectedly");
                }

                idleStrategy.idle(fragments);
            }
        }
    }

    private long onTakeSnapshot(final long logPosition, final long leadershipTermId)
    {
        final long recordingId;

        try (AeronArchive archive = AeronArchive.connect(archiveCtx);
            Publication publication = aeron.addExclusivePublication(ctx.snapshotChannel(), ctx.snapshotStreamId()))
        {
            final String channel = ChannelUri.addSessionId(ctx.snapshotChannel(), publication.sessionId());
            final long subscriptionId = archive.startRecording(channel, ctx.snapshotStreamId(), LOCAL);
            try
            {
                final CountersReader counters = aeron.countersReader();
                final int counterId = awaitRecordingCounter(publication.sessionId(), counters);

                recordingId = RecordingPos.getRecordingId(counters, counterId);
                snapshotState(publication, logPosition, leadershipTermId);
                service.onTakeSnapshot(publication);

                awaitRecordingComplete(recordingId, publication.position(), counters, counterId, archive);
            }
            finally
            {
                archive.stopRecording(subscriptionId);
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
                throw new ClusterException("recording has stopped unexpectedly: " + recordingId);
            }

            archive.checkForErrorResponse();
        }
        while (counters.getCounterValue(counterId) < position);
    }

    private void snapshotState(final Publication publication, final long logPosition, final long leadershipTermId)
    {
        final ServiceSnapshotTaker snapshotTaker = new ServiceSnapshotTaker(publication, idleStrategy, null);

        snapshotTaker.markBegin(ClusteredServiceContainer.SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0);

        for (final ClientSession clientSession : sessionByIdMap.values())
        {
            snapshotTaker.snapshotSession(clientSession);
        }

        snapshotTaker.markEnd(ClusteredServiceContainer.SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0);
    }

    private void executeAction(final ClusterAction action, final long position, final long leadershipTermId)
    {
        if (isRecovering)
        {
            return;
        }

        switch (action)
        {
            case SNAPSHOT:
                consensusModuleProxy.ack(position, ackId++, onTakeSnapshot(position, leadershipTermId), serviceId);
                break;

            case SHUTDOWN:
                consensusModuleProxy.ack(position, ackId++, onTakeSnapshot(position, leadershipTermId), serviceId);
                ctx.terminationHook().run();
                break;

            case ABORT:
                consensusModuleProxy.ack(position, ackId++, serviceId);
                ctx.terminationHook().run();
                break;
        }
    }

    private int awaitRecordingCounter(final int sessionId, final CountersReader counters)
    {
        idleStrategy.reset();
        int counterId = RecordingPos.findCounterIdBySession(counters, sessionId);
        while (NULL_COUNTER_ID == counterId)
        {
            checkInterruptedStatus();
            idleStrategy.idle();
            counterId = RecordingPos.findCounterIdBySession(counters, sessionId);
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

    private boolean checkForClockTick()
    {
        final long nowMs = epochClock.time();

        if (cachedTimeMs != nowMs)
        {
            cachedTimeMs = nowMs;

            if (consensusModuleProxy.isConnected())
            {
                markFile.updateActivityTimestamp(nowMs);
                heartbeatCounter.setOrdered(nowMs);
            }
            else
            {
                ctx.errorHandler().onError(new ClusterException("Consensus Module not connected"));
                ctx.terminationHook().run();
            }

            return true;
        }

        return false;
    }

    private void pollServiceAdapter()
    {
        serviceAdapter.poll();

        if (null != activeLogEvent && null == logAdapter)
        {
            joinActiveLog();
        }
    }
}
