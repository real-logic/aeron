/*
 * Copyright 2014-2020 Real Logic Limited.
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
import org.agrona.SemanticVersion;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.CountersReader;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.MARK_FILE_UPDATE_INTERVAL_NS;
import static io.aeron.cluster.service.ClusteredServiceContainer.SNAPSHOT_TYPE_ID;
import static java.util.Collections.unmodifiableCollection;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;

class ClusteredServiceAgent implements Agent, Cluster, IdleStrategy
{
    static final long MARK_FILE_UPDATE_INTERVAL_MS = TimeUnit.NANOSECONDS.toMillis(MARK_FILE_UPDATE_INTERVAL_NS);

    private boolean isServiceActive;
    private volatile boolean isAbort;
    private final int serviceId;
    private int memberId = NULL_VALUE;
    private long ackId = 0;
    private long timeOfLastMarkFileUpdateMs;
    private long cachedTimeMs;
    private long clusterTime;
    private long clusterLogPosition = NULL_POSITION;
    private long terminationPosition = NULL_POSITION;
    private long roleChangePosition = NULL_POSITION;

    private final AeronArchive.Context archiveCtx;
    private final ClusteredServiceContainer.Context ctx;
    private final Aeron aeron;
    private final AgentInvoker aeronAgentInvoker;
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
    private final Runnable abortHandler = this::abort;

    private BoundedLogAdapter logAdapter;
    private ReadableCounter roleCounter;
    private ReadableCounter commitPosition;
    private ActiveLogEvent activeLogEvent;
    private Role role = Role.FOLLOWER;
    private String logChannel = null;
    private TimeUnit timeUnit = null;

    ClusteredServiceAgent(final ClusteredServiceContainer.Context ctx)
    {
        this.ctx = ctx;

        archiveCtx = ctx.archiveContext();
        aeron = ctx.aeron();
        aeronAgentInvoker = ctx.aeron().conductorAgentInvoker();
        service = ctx.clusteredService();
        idleStrategy = ctx.idleStrategy();
        serviceId = ctx.serviceId();
        epochClock = ctx.epochClock();
        markFile = ctx.clusterMarkFile();

        final String channel = ctx.serviceControlChannel();
        consensusModuleProxy = new ConsensusModuleProxy(aeron.addPublication(channel, ctx.consensusModuleStreamId()));
        serviceAdapter = new ServiceAdapter(aeron.addSubscription(channel, ctx.serviceStreamId()), this);
        sessionMessageHeaderEncoder.wrapAndApplyHeader(headerBuffer, 0, new MessageHeaderEncoder());
        aeron.addCloseHandler(abortHandler);
    }

    public void onStart()
    {
        final CountersReader counters = aeron.countersReader();
        roleCounter = awaitClusterRoleCounter(counters);
        commitPosition = awaitCommitPositionCounter(counters);

        final int recoveryCounterId = awaitRecoveryCounter(counters);

        isServiceActive = true;
        checkForSnapshot(counters, recoveryCounterId);
        checkForReplay(counters, recoveryCounterId);
    }

    public void onClose()
    {
        if (isAbort)
        {
            ctx.abortLatch().countDown();
        }
        else
        {
            aeron.removeCloseHandler(abortHandler);

            final CountedErrorHandler errorHandler = ctx.countedErrorHandler();
            if (isServiceActive)
            {
                isServiceActive = false;
                try
                {
                    service.onTerminate(this);
                }
                catch (final Throwable ex)
                {
                    errorHandler.onError(ex);
                }
            }

            if (!ctx.ownsAeronClient() && !aeron.isClosed())
            {
                for (final ClientSession session : sessionByIdMap.values())
                {
                    session.disconnect(errorHandler);
                }

                CloseHelper.close(errorHandler, logAdapter);
                CloseHelper.close(errorHandler, serviceAdapter);
                CloseHelper.close(errorHandler, consensusModuleProxy);
            }
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
                    CloseHelper.close(ctx.countedErrorHandler(), logAdapter);
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

    public TimeUnit timeUnit()
    {
        return timeUnit;
    }

    public long time()
    {
        return clusterTime;
    }

    public long logPosition()
    {
        return clusterLogPosition;
    }

    public boolean scheduleTimer(final long correlationId, final long deadline)
    {
        return consensusModuleProxy.scheduleTimer(correlationId, deadline);
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

    public IdleStrategy idleStrategy()
    {
        return this;
    }

    public void reset()
    {
        idleStrategy.reset();
    }

    public void idle()
    {
        idleStrategy.idle();
        checkForClockTick();
    }

    public void idle(final int workCount)
    {
        idleStrategy.idle(workCount);
        if (workCount <= 0)
        {
            checkForClockTick();
        }
    }

    void onJoinLog(
        final long leadershipTermId,
        final long logPosition,
        final long maxLogPosition,
        final int memberId,
        final int logSessionId,
        final int logStreamId,
        final boolean isStartup,
        final String logChannel)
    {
        if (null != logAdapter && !logChannel.equals(this.logChannel))
        {
            final long existingPosition = logAdapter.position();
            if (existingPosition != logPosition)
            {
                throw new ClusterException("existing position " + existingPosition + " new position " + logPosition);
            }

            CloseHelper.close(ctx.countedErrorHandler(), logAdapter);
            logAdapter = null;
        }

        roleChangePosition = NULL_POSITION;
        activeLogEvent = new ActiveLogEvent(
            leadershipTermId, logPosition, maxLogPosition, memberId, logSessionId, logStreamId, isStartup, logChannel);
    }

    void onServiceTerminationPosition(final long logPosition)
    {
        terminationPosition = logPosition;
    }

    void onElectionStartEvent(final long logPosition)
    {
        roleChangePosition = logPosition;
    }

    void onSessionMessage(
        final long logPosition,
        final long clusterSessionId,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        clusterLogPosition = logPosition;
        clusterTime = timestamp;
        final ClientSession clientSession = sessionByIdMap.get(clusterSessionId);

        service.onSessionMessage(clientSession, timestamp, buffer, offset, length, header);
    }

    void onTimerEvent(final long logPosition, final long correlationId, final long timestamp)
    {
        clusterLogPosition = logPosition;
        clusterTime = timestamp;
        service.onTimerEvent(correlationId, timestamp);
    }

    void onSessionOpen(
        final long leadershipTermId,
        final long logPosition,
        final long clusterSessionId,
        final long timestamp,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedPrincipal)
    {
        clusterLogPosition = logPosition;
        clusterTime = timestamp;

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
        service.onSessionOpen(session, timestamp);
    }

    void onSessionClose(
        final long leadershipTermId,
        final long logPosition,
        final long clusterSessionId,
        final long timestamp,
        final CloseReason closeReason)
    {
        clusterLogPosition = logPosition;
        clusterTime = timestamp;
        final ClientSession session = sessionByIdMap.remove(clusterSessionId);

        if (null == session)
        {
            throw new ClusterException(
                "unknown clusterSessionId=" + clusterSessionId + " for close reason=" + closeReason +
                " leadershipTermId=" + leadershipTermId + " logPosition=" + logPosition);
        }

        session.disconnect(ctx.countedErrorHandler());
        service.onSessionClose(session, timestamp, closeReason);
    }

    void onServiceAction(
        final long leadershipTermId, final long logPosition, final long timestamp, final ClusterAction action)
    {
        clusterLogPosition = logPosition;
        clusterTime = timestamp;
        executeAction(action, logPosition, leadershipTermId);
    }

    void onNewLeadershipTermEvent(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final int leaderMemberId,
        final int logSessionId,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        if (SemanticVersion.major(ctx.appVersion()) != SemanticVersion.major(appVersion))
        {
            ctx.errorHandler().onError(new ClusterException(
                "incompatible version: " + SemanticVersion.toString(ctx.appVersion()) +
                " log=" + SemanticVersion.toString(appVersion)));
            ctx.terminationHook().run();
        }
        else
        {
            sessionMessageHeaderEncoder.leadershipTermId(leadershipTermId);
            clusterLogPosition = logPosition;
            clusterTime = timestamp;
            this.timeUnit = timeUnit;

            service.onNewLeadershipTermEvent(
                leadershipTermId,
                logPosition,
                timestamp,
                termBaseLogPosition,
                leaderMemberId,
                logSessionId,
                timeUnit,
                appVersion);
        }
    }

    @SuppressWarnings("unused")
    void onMembershipChange(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final int leaderMemberId,
        final int clusterSize,
        final ChangeType changeType,
        final int memberId,
        final String clusterMembers)
    {
        clusterLogPosition = logPosition;
        clusterTime = timestamp;

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
            .timestamp(clusterTime);

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
            .timestamp(clusterTime);

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
            final int maxPayloadLength = headerBuffer.capacity() - SESSION_HEADER_LENGTH;
            if (length > maxPayloadLength)
            {
                throw new IllegalArgumentException(
                    "claim exceeds maxPayloadLength of " + maxPayloadLength + ", length=" + length);
            }

            bufferClaim.wrap(headerBuffer, 0, length + SESSION_HEADER_LENGTH);
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
                .timestamp(clusterTime);

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
        clusterTime = RecoveryState.getTimestamp(counters, recoveryCounterId);
        final long leadershipTermId = RecoveryState.getLeadershipTermId(counters, recoveryCounterId);

        if (NULL_VALUE != leadershipTermId)
        {
            loadSnapshot(RecoveryState.getSnapshotRecordingId(counters, recoveryCounterId, serviceId));
        }
        else
        {
            service.onStart(this, null);
        }

        final long id = ackId++;
        idleStrategy.reset();
        while (!consensusModuleProxy.ack(clusterLogPosition, clusterTime, id, NULL_VALUE, serviceId))
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
                final long id = ackId++;
                idleStrategy.reset();
                while (!consensusModuleProxy.ack(activeLogEvent.logPosition, clusterTime, id, NULL_VALUE, serviceId))
                {
                    idle();
                }

                final Image image = awaitImage(activeLogEvent.sessionId, subscription);
                final BoundedLogAdapter adapter = new BoundedLogAdapter(image, commitPosition, this);
                consumeImage(image, adapter, activeLogEvent.maxLogPosition);
            }

            activeLogEvent = null;
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
                    final long id = ackId++;
                    while (!consensusModuleProxy.ack(image.position(), clusterTime, id, NULL_VALUE, serviceId))
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
            idle();
            counterId = RecoveryState.findCounterId(counters);
        }

        return counterId;
    }

    private void joinActiveLog()
    {
        final Subscription logSubscription = aeron.addSubscription(activeLogEvent.channel, activeLogEvent.streamId);

        final long id = ackId++;
        idleStrategy.reset();
        while (!consensusModuleProxy.ack(activeLogEvent.logPosition, clusterTime, id, NULL_VALUE, serviceId))
        {
            idle();
        }

        final boolean isStartup = activeLogEvent.isStartup;
        final Image image = awaitImage(activeLogEvent.sessionId, logSubscription);

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
                if (ctx.isRespondingService() && !isStartup)
                {
                    session.connect(aeron);
                }

                session.resetClosing();
            }
            else
            {
                session.disconnect(ctx.countedErrorHandler());
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
            idle();
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
            idle();
            counterId = CommitPos.findCounterId(counters);
        }

        return new ReadableCounter(counters, counterId);
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
                if (image.isClosed())
                {
                    throw new ClusterException("snapshot ended unexpectedly");
                }
            }

            idle(fragments);
        }

        final int appVersion = snapshotLoader.appVersion();
        if (SemanticVersion.major(ctx.appVersion()) != SemanticVersion.major(appVersion))
        {
            throw new ClusterException(
                "incompatible version: " + SemanticVersion.toString(ctx.appVersion()) +
                " snapshot=" + SemanticVersion.toString(appVersion));
        }

        timeUnit = snapshotLoader.timeUnit();
    }

    private long onTakeSnapshot(final long logPosition, final long leadershipTermId)
    {
        final long recordingId;

        try (AeronArchive archive = AeronArchive.connect(archiveCtx.clone());
            ExclusivePublication publication = aeron.addExclusivePublication(
                ctx.snapshotChannel(), ctx.snapshotStreamId()))
        {
            final String channel = ChannelUri.addSessionId(ctx.snapshotChannel(), publication.sessionId());
            final long subscriptionId = archive.startRecording(channel, ctx.snapshotStreamId(), LOCAL);
            try
            {
                final CountersReader counters = aeron.countersReader();
                final int counterId = awaitRecordingCounter(publication.sessionId(), counters);

                recordingId = RecordingPos.getRecordingId(counters, counterId);
                snapshotState(publication, logPosition, leadershipTermId);

                checkForClockTick();
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

    private void snapshotState(
        final ExclusivePublication publication, final long logPosition, final long leadershipTermId)
    {
        final ServiceSnapshotTaker snapshotTaker = new ServiceSnapshotTaker(
            publication, idleStrategy, aeronAgentInvoker);

        snapshotTaker.markBegin(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0, timeUnit, ctx.appVersion());

        for (final ClientSession clientSession : sessionByIdMap.values())
        {
            snapshotTaker.snapshotSession(clientSession);
        }

        snapshotTaker.markEnd(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0, timeUnit, ctx.appVersion());
    }

    private void executeAction(final ClusterAction action, final long position, final long leadershipTermId)
    {
        if (ClusterAction.SNAPSHOT == action)
        {
            final long recordingId = onTakeSnapshot(position, leadershipTermId);
            final long id = ackId++;
            idleStrategy.reset();
            while (!consensusModuleProxy.ack(position, clusterTime, id, recordingId, serviceId))
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

    private boolean checkForClockTick()
    {
        if (isAbort || aeron.isClosed())
        {
            isAbort = true;
            throw new AgentTerminationException("unexpected Aeron close");
        }

        final long nowMs = epochClock.time();
        if (cachedTimeMs != nowMs)
        {
            cachedTimeMs = nowMs;

            if (Thread.interrupted())
            {
                isAbort = true;
                throw new AgentTerminationException("unexpected interrupt - " + context().clusterDir());
            }

            if (null != aeronAgentInvoker)
            {
                aeronAgentInvoker.invoke();
                if (isAbort || aeron.isClosed())
                {
                    isAbort = true;
                    throw new AgentTerminationException("unexpected Aeron close");
                }
            }

            if (nowMs >= (timeOfLastMarkFileUpdateMs + MARK_FILE_UPDATE_INTERVAL_MS))
            {
                markFile.updateActivityTimestamp(nowMs);
                timeOfLastMarkFileUpdateMs = nowMs;
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

        final long id = ackId++;
        while (!consensusModuleProxy.ack(logPosition, clusterTime, id, NULL_VALUE, serviceId))
        {
            idle();
        }

        ctx.terminationHook().run();
    }

    private void abort()
    {
        isAbort = true;

        try
        {
            ctx.abortLatch().await(AgentRunner.RETRY_CLOSE_TIMEOUT_MS * 3L, TimeUnit.MILLISECONDS);
        }
        catch (final InterruptedException ignore)
        {
            Thread.currentThread().interrupt();
        }
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
