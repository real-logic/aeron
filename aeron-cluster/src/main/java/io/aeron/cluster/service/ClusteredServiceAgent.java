/*
 * Copyright 2014-2019 Real Logic Ltd.
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
import io.aeron.driver.Configuration;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.status.ReadableCounter;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

import java.util.Collection;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;
import static java.util.Collections.unmodifiableCollection;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;

class ClusteredServiceAgent implements Agent, Cluster
{
    private final int serviceId;
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
    private final UnsafeBuffer headerBuffer = new UnsafeBuffer(
        new byte[Configuration.MAX_UDP_PAYLOAD_LENGTH - DataHeaderFlyweight.HEADER_LENGTH]);
    private final DirectBufferVector headerVector = new DirectBufferVector(headerBuffer, 0, SESSION_HEADER_LENGTH);
    private final SessionMessageHeaderEncoder sessionMessageHeaderEncoder = new SessionMessageHeaderEncoder();

    private long ackId = 0;
    private long clusterTimeMs;
    private long cachedTimeMs;
    private long clusterLogPosition = NULL_POSITION;
    private long terminationPosition = NULL_POSITION;
    private long roleChangePosition = NULL_POSITION;
    private int memberId = NULL_VALUE;
    private boolean isServiceActive;
    private BoundedLogAdapter logAdapter;
    private AtomicCounter heartbeatCounter;
    private ReadableCounter roleCounter;
    private ReadableCounter commitPosition;
    private ActiveLogEvent activeLogEvent;
    private Role role = Role.FOLLOWER;
    private String logChannel = null;

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

        sessionMessageHeaderEncoder.wrapAndApplyHeader(headerBuffer, 0, new MessageHeaderEncoder());
    }

    public void onStart()
    {
        final CountersReader counters = aeron.countersReader();
        roleCounter = awaitClusterRoleCounter(counters);
        heartbeatCounter = awaitHeartbeatCounter(counters);
        commitPosition = awaitCommitPositionCounter(counters);

        final int recoveryCounterId = awaitRecoveryCounter(counters);
        heartbeatCounter.setOrdered(epochClock.time());

        isServiceActive = true;
        checkForSnapshot(counters, recoveryCounterId);
        checkForReplay(counters, recoveryCounterId);
    }

    public void onClose()
    {
        if (isServiceActive)
        {
            isServiceActive = false;
            try
            {
                service.onTerminate(this);
            }
            catch (final Exception ex)
            {
                ctx.countedErrorHandler().onError(ex);
            }
        }

        if (!ctx.ownsAeronClient())
        {
            for (final ClientSession session : sessionByIdMap.values())
            {
                session.disconnect();
            }

            CloseHelper.close(logAdapter);
            CloseHelper.close(serviceAdapter);
            CloseHelper.close(consensusModuleProxy);
        }

        ctx.close();
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
                if (logAdapter.isDone())
                {
                    checkPosition(logAdapter.position(), activeLogEvent);
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

    public int memberId()
    {
        return memberId;
    }

    public Aeron aeron()
    {
        return aeron;
    }

    public ClusteredServiceContainer.Context context()
    {
        return ctx;
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

    public long logPosition()
    {
        return clusterLogPosition;
    }

    public boolean scheduleTimer(final long correlationId, final long deadlineMs)
    {
        return consensusModuleProxy.scheduleTimer(correlationId, deadlineMs);
    }

    public boolean cancelTimer(final long correlationId)
    {
        return consensusModuleProxy.cancelTimer(correlationId);
    }

    public long offer(final DirectBuffer buffer, final int offset, final int length)
    {
        sessionMessageHeaderEncoder.clusterSessionId(0);

        return consensusModuleProxy.offer(headerBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length);
    }

    public long offer(final DirectBufferVector[] vectors)
    {
        sessionMessageHeaderEncoder.clusterSessionId(0);
        vectors[0] = headerVector;

        return consensusModuleProxy.offer(vectors);
    }

    public long tryClaim(final int length, final BufferClaim bufferClaim)
    {
        sessionMessageHeaderEncoder.clusterSessionId(0);

        return consensusModuleProxy.tryClaim(length + SESSION_HEADER_LENGTH, bufferClaim, headerBuffer);
    }

    public void idle()
    {
        checkInterruptedStatus();
        checkForClockTick();
        idleStrategy.idle();
    }

    public void idle(final int workCount)
    {
        checkInterruptedStatus();
        checkForClockTick();
        idleStrategy.idle(workCount);
    }

    public void onJoinLog(
        final long leadershipTermId,
        final long logPosition,
        final long maxLogPosition,
        final int memberId,
        final int logSessionId,
        final int logStreamId,
        final String logChannel)
    {
        if (null != logAdapter && !logChannel.equals(this.logChannel))
        {
            final long existingPosition = logAdapter.position();
            if (existingPosition != logPosition)
            {
                throw new ClusterException("existing position " + existingPosition + " new position " + logPosition);
            }

            logAdapter.close();
            logAdapter = null;
        }

        roleChangePosition = NULL_POSITION;
        activeLogEvent = new ActiveLogEvent(
            leadershipTermId, logPosition, maxLogPosition, memberId, logSessionId, logStreamId, logChannel);
    }

    public void onServiceTerminationPosition(final long logPosition)
    {
        terminationPosition = logPosition;
    }

    public void onElectionStartEvent(final long logPosition)
    {
        roleChangePosition = logPosition;
    }

    void onSessionMessage(
        final long logPosition,
        final long clusterSessionId,
        final long timestampMs,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        clusterLogPosition = logPosition;
        clusterTimeMs = timestampMs;
        final ClientSession clientSession = sessionByIdMap.get(clusterSessionId);

        service.onSessionMessage(clientSession, timestampMs, buffer, offset, length, header);
    }

    void onTimerEvent(final long logPosition, final long correlationId, final long timestampMs)
    {
        clusterLogPosition = logPosition;
        clusterTimeMs = timestampMs;
        service.onTimerEvent(correlationId, timestampMs);
    }

    void onSessionOpen(
        final long leadershipTermId,
        final long logPosition,
        final long clusterSessionId,
        final long timestampMs,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedPrincipal)
    {
        clusterLogPosition = logPosition;
        clusterTimeMs = timestampMs;

        if (sessionByIdMap.containsKey(clusterSessionId))
        {
            throw new ClusterException("clashing open clusterSessionId=" + clusterSessionId +
                " leadershipTermId=" + leadershipTermId + " logPosition=" + logPosition);
        }

        final ClientSession session = new ClientSession(
            clusterSessionId, responseStreamId, responseChannel, encodedPrincipal, this);

        if (Role.LEADER == role && ctx.isRespondingService())
        {
            session.connect(aeron);
        }

        sessionByIdMap.put(clusterSessionId, session);
        service.onSessionOpen(session, timestampMs);
    }

    void onSessionClose(
        final long leadershipTermId,
        final long logPosition,
        final long clusterSessionId,
        final long timestampMs,
        final CloseReason closeReason)
    {
        clusterLogPosition = logPosition;
        clusterTimeMs = timestampMs;
        final ClientSession session = sessionByIdMap.remove(clusterSessionId);

        if (null == session)
        {
            throw new ClusterException(
                "unknown clusterSessionId=" + clusterSessionId + " for close reason=" + closeReason +
                " leadershipTermId=" + leadershipTermId + " logPosition=" + logPosition);
        }

        session.disconnect();
        service.onSessionClose(session, timestampMs, closeReason);
    }

    void onServiceAction(
        final long leadershipTermId, final long logPosition, final long timestampMs, final ClusterAction action)
    {
        clusterLogPosition = logPosition;
        clusterTimeMs = timestampMs;
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
        sessionMessageHeaderEncoder.leadershipTermId(leadershipTermId);
        clusterLogPosition = logPosition;
        clusterTimeMs = timestampMs;
    }

    @SuppressWarnings("unused")
    void onMembershipChange(
        final long leadershipTermId,
        final long logPosition,
        final long timestampMs,
        final int leaderMemberId,
        final int clusterSize,
        final ChangeType changeType,
        final int memberId,
        final String clusterMembers)
    {
        clusterLogPosition = logPosition;
        clusterTimeMs = timestampMs;

        if (memberId == this.memberId && changeType == ChangeType.QUIT)
        {
            terminate(logPosition);
        }
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

    void handleError(final Throwable ex)
    {
        ctx.countedErrorHandler().onError(ex);
    }

    long offer(
        final long clusterSessionId,
        final Publication publication,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        if (role != Cluster.Role.LEADER)
        {
            return ClientSession.MOCKED_OFFER;
        }

        if (null == publication)
        {
            return Publication.NOT_CONNECTED;
        }

        sessionMessageHeaderEncoder
            .clusterSessionId(clusterSessionId)
            .timestamp(clusterTimeMs);

        return publication.offer(headerBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length, null);
    }

    long offer(final long clusterSessionId, final Publication publication, final DirectBufferVector[] vectors)
    {
        if (role != Cluster.Role.LEADER)
        {
            return ClientSession.MOCKED_OFFER;
        }

        if (null == publication)
        {
            return Publication.NOT_CONNECTED;
        }

        sessionMessageHeaderEncoder
            .clusterSessionId(clusterSessionId)
            .timestamp(clusterTimeMs);

        vectors[0] = headerVector;

        return publication.offer(vectors, null);
    }

    long tryClaim(
        final long clusterSessionId,
        final Publication publication,
        final int length,
        final BufferClaim bufferClaim)
    {
        if (role != Cluster.Role.LEADER)
        {
            bufferClaim.wrap(headerBuffer, 0, length);
            return ClientSession.MOCKED_OFFER;
        }

        if (null == publication)
        {
            return Publication.NOT_CONNECTED;
        }

        final long offset = publication.tryClaim(length + SESSION_HEADER_LENGTH, bufferClaim);
        if (offset > 0)
        {
            sessionMessageHeaderEncoder
                .clusterSessionId(clusterSessionId)
                .timestamp(clusterTimeMs);

            bufferClaim.putBytes(headerBuffer, 0, SESSION_HEADER_LENGTH);
        }

        return offset;
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
        clusterLogPosition = RecoveryState.getLogPosition(counters, recoveryCounterId);
        clusterTimeMs = RecoveryState.getTimestamp(counters, recoveryCounterId);
        final long leadershipTermId = RecoveryState.getLeadershipTermId(counters, recoveryCounterId);

        if (NULL_VALUE != leadershipTermId)
        {
            loadSnapshot(RecoveryState.getSnapshotRecordingId(counters, recoveryCounterId, serviceId));
        }
        else
        {
            service.onStart(this, null);
        }

        heartbeatCounter.setOrdered(epochClock.time());
        while (!consensusModuleProxy.ack(clusterLogPosition, ackId++, serviceId))
        {
            idle();
        }
    }

    private void checkForReplay(final CountersReader counters, final int recoveryCounterId)
    {
        if (RecoveryState.hasReplay(counters, recoveryCounterId))
        {
            awaitActiveLog();

            try (Subscription subscription = aeron.addSubscription(activeLogEvent.channel, activeLogEvent.streamId))
            {
                while (!consensusModuleProxy.ack(activeLogEvent.logPosition, ackId++, serviceId))
                {
                    idle();
                }

                final Image image = awaitImage(activeLogEvent.sessionId, subscription);
                final BoundedLogAdapter adapter = new BoundedLogAdapter(image, commitPosition, this);

                consumeImage(image, adapter, activeLogEvent.maxLogPosition);
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
            idle();
            serviceAdapter.poll();
        }
    }

    private void consumeImage(final Image image, final BoundedLogAdapter adapter, final long maxLogPosition)
    {
        while (true)
        {
            final int workCount = adapter.poll();
            if (workCount == 0)
            {
                if (adapter.position() >= maxLogPosition)
                {
                    while (!consensusModuleProxy.ack(image.position(), ackId++, serviceId))
                    {
                        idle();
                    }
                    break;
                }

                if (image.isClosed())
                {
                    throw new ClusterException("unexpected close of replay");
                }
            }

            idle(workCount);
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

            heartbeatCounter.setOrdered(epochClock.time());
            counterId = RecoveryState.findCounterId(counters);
        }

        return counterId;
    }

    private void joinActiveLog()
    {
        final Subscription logSubscription = aeron.addSubscription(activeLogEvent.channel, activeLogEvent.streamId);

        while (!consensusModuleProxy.ack(activeLogEvent.logPosition, ackId++, serviceId))
        {
            idle();
        }

        final Image image = awaitImage(activeLogEvent.sessionId, logSubscription);
        heartbeatCounter.setOrdered(epochClock.time());

        sessionMessageHeaderEncoder.leadershipTermId(activeLogEvent.leadershipTermId);
        memberId = activeLogEvent.memberId;
        ctx.clusterMarkFile().memberId(memberId);
        logChannel = activeLogEvent.channel;
        activeLogEvent = null;
        logAdapter = new BoundedLogAdapter(image, commitPosition, this);

        role(Role.get((int)roleCounter.get()));

        for (final ClientSession session : sessionByIdMap.values())
        {
            if (Role.LEADER == role)
            {
                if (ctx.isRespondingService())
                {
                    session.connect(aeron);
                }

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
            idle();
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

    private ReadableCounter awaitCommitPositionCounter(final CountersReader counters)
    {
        idleStrategy.reset();
        int counterId = CommitPos.findCounterId(counters);
        while (NULL_COUNTER_ID == counterId)
        {
            checkInterruptedStatus();
            idleStrategy.idle();
            heartbeatCounter.setOrdered(epochClock.time());
            counterId = CommitPos.findCounterId(counters);
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
        try (AeronArchive archive = AeronArchive.connect(archiveCtx.clone()))
        {
            final String channel = ctx.replayChannel();
            final int streamId = ctx.replayStreamId();
            final int sessionId = (int)archive.startReplay(recordingId, 0, NULL_VALUE, channel, streamId);

            final String replaySessionChannel = ChannelUri.addSessionId(channel, sessionId);
            try (Subscription subscription = aeron.addSubscription(replaySessionChannel, streamId))
            {
                final Image image = awaitImage(sessionId, subscription);
                loadState(image);
                service.onStart(this, image);
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

        try (AeronArchive archive = AeronArchive.connect(archiveCtx.clone());
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
            idle();

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
        if (ClusterAction.SNAPSHOT == action)
        {
            while (!consensusModuleProxy.ack(position, ackId++, onTakeSnapshot(position, leadershipTermId), serviceId))
            {
                idle();
            }
        }
    }

    private int awaitRecordingCounter(final int sessionId, final CountersReader counters)
    {
        idleStrategy.reset();
        int counterId = RecordingPos.findCounterIdBySession(counters, sessionId);
        while (NULL_COUNTER_ID == counterId)
        {
            idle();
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
                ctx.countedErrorHandler().onError(new ClusterException("Consensus Module not connected"));
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

        if (NULL_POSITION != terminationPosition)
        {
            checkForTermination();
        }

        if (NULL_POSITION != roleChangePosition)
        {
            checkForRoleChange();
        }
    }

    private void checkForTermination()
    {
        if (null != logAdapter && logAdapter.position() >= terminationPosition)
        {
            final long logPosition = terminationPosition;
            terminationPosition = NULL_VALUE;
            terminate(logPosition);
        }
    }

    private void checkForRoleChange()
    {
        if (null != logAdapter && logAdapter.position() >= roleChangePosition)
        {
            roleChangePosition = NULL_VALUE;
            role(Role.get((int)roleCounter.get()));
        }
    }

    private void terminate(final long logPosition)
    {
        isServiceActive = false;
        try
        {
            service.onTerminate(this);
        }
        catch (final Exception ex)
        {
            ctx.countedErrorHandler().onError(ex);
        }

        while (!consensusModuleProxy.ack(logPosition, ackId++, serviceId))
        {
            idle();
        }

        ctx.terminationHook().run();
    }

    private static void checkPosition(final long existingPosition, final ActiveLogEvent activeLogEvent)
    {
        if (null != activeLogEvent && existingPosition != activeLogEvent.logPosition)
        {
            throw new ClusterException(
                "existing position " + existingPosition + " new position " + activeLogEvent.logPosition);
        }
    }
}
