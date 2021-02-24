/*
 * Copyright 2014-2021 Real Logic Limited.
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
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.driver.Configuration;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.status.ReadableCounter;
import org.agrona.*;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.CountersReader;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.*;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;

final class ClusteredServiceAgent implements Agent, Cluster, IdleStrategy
{
    static final long MARK_FILE_UPDATE_INTERVAL_MS = TimeUnit.NANOSECONDS.toMillis(MARK_FILE_UPDATE_INTERVAL_NS);

    private volatile boolean isAbort;
    private boolean isServiceActive;
    private final int serviceId;
    private int memberId = NULL_VALUE;
    private long closeHandlerRegistrationId;
    private long ackId = 0;
    private long terminationPosition = NULL_POSITION;
    private long markFileUpdateDeadlineMs;
    private long cachedTimeMs;
    private long clusterTime;
    private long logPosition = NULL_POSITION;

    private final IdleStrategy idleStrategy;
    private final ClusteredServiceContainer.Context ctx;
    private final Aeron aeron;
    private final AgentInvoker aeronAgentInvoker;
    private final ClusteredService service;
    private final ConsensusModuleProxy consensusModuleProxy;
    private final ServiceAdapter serviceAdapter;
    private final EpochClock epochClock;
    private final UnsafeBuffer headerBuffer = new UnsafeBuffer(
        new byte[Configuration.MAX_UDP_PAYLOAD_LENGTH - DataHeaderFlyweight.HEADER_LENGTH]);
    private final DirectBufferVector headerVector = new DirectBufferVector(headerBuffer, 0, SESSION_HEADER_LENGTH);
    private final SessionMessageHeaderEncoder sessionMessageHeaderEncoder = new SessionMessageHeaderEncoder();
    private final Long2ObjectHashMap<ClientSession> sessionByIdMap = new Long2ObjectHashMap<>();
    private final Collection<ClientSession> unmodifiableClientSessions =
        new UnmodifiableClientSessionCollection(sessionByIdMap.values());
    private final BoundedLogAdapter logAdapter;
    private String activeLifecycleCallbackName;
    private ReadableCounter commitPosition;
    private ActiveLogEvent activeLogEvent;
    private Role role = Role.FOLLOWER;
    private TimeUnit timeUnit = null;

    ClusteredServiceAgent(final ClusteredServiceContainer.Context ctx)
    {
        logAdapter = new BoundedLogAdapter(this, ctx.logFragmentLimit());
        this.ctx = ctx;

        aeron = ctx.aeron();
        aeronAgentInvoker = ctx.aeron().conductorAgentInvoker();
        service = ctx.clusteredService();
        idleStrategy = ctx.idleStrategy();
        serviceId = ctx.serviceId();
        epochClock = ctx.epochClock();

        final String channel = ctx.controlChannel();
        consensusModuleProxy = new ConsensusModuleProxy(aeron.addPublication(channel, ctx.consensusModuleStreamId()));
        serviceAdapter = new ServiceAdapter(aeron.addSubscription(channel, ctx.serviceStreamId()), this);
        sessionMessageHeaderEncoder.wrapAndApplyHeader(headerBuffer, 0, new MessageHeaderEncoder());
    }

    public void onStart()
    {
        closeHandlerRegistrationId = aeron.addCloseHandler(this::abort);
        final CountersReader counters = aeron.countersReader();
        commitPosition = awaitCommitPositionCounter(counters, ctx.clusterId());

        recoverState(counters);
    }

    public void onClose()
    {
        aeron.removeCloseHandler(closeHandlerRegistrationId);

        if (isAbort)
        {
            ctx.abortLatch().countDown();
        }
        else
        {
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

        if (null != logAdapter.image())
        {
            final int polled = logAdapter.poll(commitPosition.get());
            workCount += polled;

            if (0 == polled && logAdapter.isDone())
            {
                closeLog();
            }
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
        return unmodifiableClientSessions;
    }

    public void forEachClientSession(final Consumer<? super ClientSession> action)
    {
        sessionByIdMap.values().forEach(action);
    }

    public boolean closeClientSession(final long clusterSessionId)
    {
        checkForLifecycleCallback();

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
        return logPosition;
    }

    public boolean scheduleTimer(final long correlationId, final long deadline)
    {
        checkForLifecycleCallback();

        return consensusModuleProxy.scheduleTimer(correlationId, deadline);
    }

    public boolean cancelTimer(final long correlationId)
    {
        checkForLifecycleCallback();

        return consensusModuleProxy.cancelTimer(correlationId);
    }

    public long offer(final DirectBuffer buffer, final int offset, final int length)
    {
        checkForLifecycleCallback();
        sessionMessageHeaderEncoder.clusterSessionId(0);

        return consensusModuleProxy.offer(headerBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length);
    }

    public long offer(final DirectBufferVector[] vectors)
    {
        checkForLifecycleCallback();
        sessionMessageHeaderEncoder.clusterSessionId(0);
        vectors[0] = headerVector;

        return consensusModuleProxy.offer(vectors);
    }

    public long tryClaim(final int length, final BufferClaim bufferClaim)
    {
        checkForLifecycleCallback();
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
        if (Thread.currentThread().isInterrupted())
        {
            throw new AgentTerminationException("interrupted");
        }
        checkForClockTick();
    }

    public void idle(final int workCount)
    {
        idleStrategy.idle(workCount);
        if (workCount <= 0)
        {
            if (Thread.currentThread().isInterrupted())
            {
                throw new AgentTerminationException("interrupted");
            }
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
        final Cluster.Role role,
        final String logChannel)
    {
        logAdapter.maxLogPosition(logPosition);
        activeLogEvent = new ActiveLogEvent(
            leadershipTermId,
            logPosition,
            maxLogPosition,
            memberId,
            logSessionId,
            logStreamId,
            isStartup,
            role,
            logChannel);
    }

    void onServiceTerminationPosition(final long logPosition)
    {
        terminationPosition = logPosition;
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
        this.logPosition = logPosition;
        clusterTime = timestamp;
        final ClientSession clientSession = sessionByIdMap.get(clusterSessionId);

        service.onSessionMessage(clientSession, timestamp, buffer, offset, length, header);
    }

    void onTimerEvent(final long logPosition, final long correlationId, final long timestamp)
    {
        this.logPosition = logPosition;
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
        this.logPosition = logPosition;
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
        this.logPosition = logPosition;
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
        this.logPosition = logPosition;
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
            terminateAgent();
        }
        else
        {
            sessionMessageHeaderEncoder.leadershipTermId(leadershipTermId);
            this.logPosition = logPosition;
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

    void onMembershipChange(
        final long logPosition, final long timestamp, final ChangeType changeType, final int memberId)
    {
        this.logPosition = logPosition;
        clusterTime = timestamp;

        if (memberId == this.memberId && changeType == ChangeType.QUIT)
        {
            terminate();
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
        checkForLifecycleCallback();

        if (Cluster.Role.LEADER != role)
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
        checkForLifecycleCallback();

        if (Cluster.Role.LEADER != role)
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
        checkForLifecycleCallback();

        if (Cluster.Role.LEADER != role)
        {
            final int maxPayloadLength = headerBuffer.capacity() - SESSION_HEADER_LENGTH;
            if (length > maxPayloadLength)
            {
                throw new IllegalArgumentException(
                    "claim exceeds maxPayloadLength=" + maxPayloadLength + ", length=" + length);
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
            activeLifecycleCallbackName = "onRoleChange";
            try
            {
                service.onRoleChange(newRole);
            }
            finally
            {
                activeLifecycleCallbackName = null;
            }
        }
    }

    private void recoverState(final CountersReader counters)
    {
        final int recoveryCounterId = awaitRecoveryCounter(counters);
        logPosition = RecoveryState.getLogPosition(counters, recoveryCounterId);
        clusterTime = RecoveryState.getTimestamp(counters, recoveryCounterId);
        final long leadershipTermId = RecoveryState.getLeadershipTermId(counters, recoveryCounterId);
        sessionMessageHeaderEncoder.leadershipTermId(leadershipTermId);
        isServiceActive = true;

        activeLifecycleCallbackName = "onStart";
        try
        {
            if (NULL_VALUE != leadershipTermId)
            {
                loadSnapshot(RecoveryState.getSnapshotRecordingId(counters, recoveryCounterId, serviceId));
            }
            else
            {
                service.onStart(this, null);
            }
        }
        finally
        {
            activeLifecycleCallbackName = null;
        }

        final long id = ackId++;
        idleStrategy.reset();
        while (!consensusModuleProxy.ack(logPosition, clusterTime, id, aeron.clientId(), serviceId))
        {
            idle();
        }
    }

    private int awaitRecoveryCounter(final CountersReader counters)
    {
        idleStrategy.reset();
        int counterId = RecoveryState.findCounterId(counters, ctx.clusterId());
        while (NULL_COUNTER_ID == counterId)
        {
            idle();
            counterId = RecoveryState.findCounterId(counters, ctx.clusterId());
        }

        return counterId;
    }

    private void closeLog()
    {
        logPosition = Math.max(logAdapter.image().position(), logPosition);
        CloseHelper.close(ctx.countedErrorHandler(), logAdapter);
        role(Role.FOLLOWER);
    }

    private void joinActiveLog(final ActiveLogEvent activeLog)
    {
        if (Role.LEADER != activeLog.role)
        {
            for (final ClientSession session : sessionByIdMap.values())
            {
                session.disconnect(ctx.countedErrorHandler());
            }
        }

        Subscription logSubscription = aeron.addSubscription(activeLog.channel, activeLog.streamId);
        try
        {
            logAdapter.image(awaitImage(activeLog.sessionId, logSubscription));
            logAdapter.maxLogPosition(activeLog.maxLogPosition);
            logSubscription = null;

            final long id = ackId++;
            idleStrategy.reset();
            while (!consensusModuleProxy.ack(activeLog.logPosition, clusterTime, id, NULL_VALUE, serviceId))
            {
                idle();
            }
        }
        finally
        {
            CloseHelper.quietClose(logSubscription);
        }

        memberId = activeLog.memberId;
        ctx.clusterMarkFile().memberId(memberId);

        if (Role.LEADER == activeLog.role)
        {
            for (final ClientSession session : sessionByIdMap.values())
            {
                if (ctx.isRespondingService() && !activeLog.isStartup)
                {
                    session.connect(aeron);
                }

                session.resetClosing();
            }
        }

        role(activeLog.role);
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

    private ReadableCounter awaitCommitPositionCounter(final CountersReader counters, final int clusterId)
    {
        idleStrategy.reset();
        int counterId = ClusterCounters.find(counters, COMMIT_POSITION_TYPE_ID, clusterId);
        while (NULL_COUNTER_ID == counterId)
        {
            idle();
            counterId = ClusterCounters.find(counters, COMMIT_POSITION_TYPE_ID, clusterId);
        }

        return new ReadableCounter(counters, counterId);
    }

    private void loadSnapshot(final long recordingId)
    {
        try (AeronArchive archive = AeronArchive.connect(ctx.archiveContext().clone()))
        {
            final String channel = ctx.replayChannel();
            final int streamId = ctx.replayStreamId();
            final int sessionId = (int)archive.startReplay(recordingId, 0, NULL_VALUE, channel, streamId);

            final String replaySessionChannel = ChannelUri.addSessionId(channel, sessionId);
            try (Subscription subscription = aeron.addSubscription(replaySessionChannel, streamId))
            {
                final Image image = awaitImage(sessionId, subscription);
                loadState(image, archive);
                service.onStart(this, image);
            }
        }
    }

    private void loadState(final Image image, final AeronArchive archive)
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

                archive.checkForErrorResponse();
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
        try (AeronArchive archive = AeronArchive.connect(ctx.archiveContext().clone());
            ExclusivePublication publication = aeron.addExclusivePublication(
                ctx.snapshotChannel(), ctx.snapshotStreamId()))
        {
            final String channel = ChannelUri.addSessionId(ctx.snapshotChannel(), publication.sessionId());
            archive.startRecording(channel, ctx.snapshotStreamId(), LOCAL, true);
            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitRecordingCounter(publication.sessionId(), counters, archive);
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);

            snapshotState(publication, logPosition, leadershipTermId);
            checkForClockTick();
            archive.checkForErrorResponse();
            service.onTakeSnapshot(publication);
            awaitRecordingComplete(recordingId, publication.position(), counters, counterId, archive);

            return recordingId;
        }
        catch (final ArchiveException ex)
        {
            if (ex.errorCode() == ArchiveException.STORAGE_SPACE)
            {
                throw new AgentTerminationException(ex);
            }

            throw ex;
        }
    }

    private void awaitRecordingComplete(
        final long recordingId,
        final long position,
        final CountersReader counters,
        final int counterId,
        final AeronArchive archive)
    {
        idleStrategy.reset();
        while (counters.getCounterValue(counterId) < position)
        {
            idle();
            archive.checkForErrorResponse();

            if (!RecordingPos.isActive(counters, counterId, recordingId))
            {
                throw new ClusterException("recording stopped unexpectedly: " + recordingId);
            }
        }
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

    private void executeAction(final ClusterAction action, final long logPosition, final long leadershipTermId)
    {
        if (ClusterAction.SNAPSHOT == action)
        {
            final long recordingId = onTakeSnapshot(logPosition, leadershipTermId);
            final long id = ackId++;
            idleStrategy.reset();
            while (!consensusModuleProxy.ack(logPosition, clusterTime, id, recordingId, serviceId))
            {
                idle();
            }
        }
    }

    private int awaitRecordingCounter(final int sessionId, final CountersReader counters, final AeronArchive archive)
    {
        idleStrategy.reset();
        int counterId = RecordingPos.findCounterIdBySession(counters, sessionId);
        while (NULL_COUNTER_ID == counterId)
        {
            idle();
            archive.checkForErrorResponse();
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

            if (null != aeronAgentInvoker)
            {
                aeronAgentInvoker.invoke();
                if (isAbort || aeron.isClosed())
                {
                    isAbort = true;
                    throw new AgentTerminationException("unexpected Aeron close");
                }
            }

            if (nowMs >= markFileUpdateDeadlineMs)
            {
                ctx.clusterMarkFile().updateActivityTimestamp(nowMs);
                markFileUpdateDeadlineMs = nowMs + MARK_FILE_UPDATE_INTERVAL_MS;
            }

            return true;
        }

        return false;
    }

    private void pollServiceAdapter()
    {
        serviceAdapter.poll();

        if (null != activeLogEvent && null == logAdapter.image())
        {
            final ActiveLogEvent event = activeLogEvent;
            activeLogEvent = null;
            joinActiveLog(event);
        }

        if (NULL_POSITION != terminationPosition && logPosition >= terminationPosition)
        {
            terminate();
        }
    }

    private void terminate()
    {
        isServiceActive = false;
        activeLifecycleCallbackName = "onTerminate";
        try
        {
            service.onTerminate(this);
        }
        catch (final Exception ex)
        {
            ctx.countedErrorHandler().onError(ex);
        }
        finally
        {
            activeLifecycleCallbackName = null;
        }

        try
        {
            int attempts = 5;
            final long id = ackId++;
            while (!consensusModuleProxy.ack(logPosition, clusterTime, id, NULL_VALUE, serviceId))
            {
                if (0 == --attempts)
                {
                    break;
                }
                idle();
            }
        }
        catch (final Exception ex)
        {
            ctx.countedErrorHandler().onError(ex);
        }

        terminationPosition = NULL_VALUE;
        terminateAgent();
    }

    private void terminateAgent()
    {
        try
        {
            ctx.terminationHook().run();
        }
        catch (final Throwable ex)
        {
            ctx.countedErrorHandler().onError(ex);
        }

        throw new ClusterTerminationException();
    }

    private void checkForLifecycleCallback()
    {
        if (null != activeLifecycleCallbackName)
        {
            throw new ClusterException(
                "sending messages or scheduling timers is not allowed from " + activeLifecycleCallbackName);
        }
    }

    private void abort()
    {
        isAbort = true;

        try
        {
            if (!ctx.abortLatch().await(AgentRunner.RETRY_CLOSE_TIMEOUT_MS * 3L, TimeUnit.MILLISECONDS))
            {
                ctx.countedErrorHandler().onError(
                    new TimeoutException("awaiting abort latch", AeronException.Category.WARN));
            }
        }
        catch (final InterruptedException ignore)
        {
            Thread.currentThread().interrupt();
        }
    }
}
