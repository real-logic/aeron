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
package io.aeron.cluster;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.codecs.*;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.service.CommitPos;
import io.aeron.cluster.service.RecoveryState;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.*;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.status.ReadableCounter;
import org.agrona.*;
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.CountersReader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.ChannelUri.SPY_QUALIFIER;
import static io.aeron.CommonContext.*;
import static io.aeron.archive.client.AeronArchive.NULL_LENGTH;
import static io.aeron.cluster.ClusterSession.State.*;
import static io.aeron.cluster.ConsensusModule.Configuration.SESSION_TIMEOUT_MSG;
import static io.aeron.cluster.ConsensusModule.SNAPSHOT_TYPE_ID;
import static io.aeron.cluster.ServiceAckPosition.*;
import static io.aeron.cluster.service.ClusteredService.NULL_SERVICE_ID;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;

class SequencerAgent implements Agent, MemberStatusListener
{
    private boolean isRecovering;
    private final int memberId;
    private final long sessionTimeoutMs;
    private final long leaderHeartbeatIntervalMs;
    private final long leaderHeartbeatTimeoutMs;
    private final long serviceHeartbeatTimeoutMs;
    private long nextSessionId = 1;
    private long leadershipTermId = NULL_VALUE;
    private long expectedAckPosition = 0;
    private long lastAppendedPosition = 0;
    private long followerCommitPosition = 0;
    private long timeOfLastLogUpdateMs = 0;
    private ReadableCounter appendedPosition;
    private Counter commitPosition;
    private ConsensusModule.State state = ConsensusModule.State.INIT;
    private Cluster.Role role;
    private ClusterMember[] clusterMembers;
    private ClusterMember leaderMember;
    private final ClusterMember thisMember;
    private long[] rankedPositions;
    private final ServiceAckPosition[] serviceAckPositions;
    private final Counter clusterRoleCounter;
    private final ClusterMarkFile markFile;
    private final AgentInvoker aeronClientInvoker;
    private final EpochClock epochClock;
    private final CachedEpochClock cachedEpochClock = new CachedEpochClock();
    private final Counter moduleState;
    private final Counter controlToggle;
    private final TimerService timerService;
    private final ConsensusModuleAdapter consensusModuleAdapter;
    private final ServiceProxy serviceProxy;
    private final IngressAdapter ingressAdapter;
    private final EgressPublisher egressPublisher;
    private final LogPublisher logPublisher;
    private LogAdapter logAdapter;
    private final MemberStatusAdapter memberStatusAdapter;
    private final MemberStatusPublisher memberStatusPublisher = new MemberStatusPublisher();
    private final Long2ObjectHashMap<ClusterSession> sessionByIdMap = new Long2ObjectHashMap<>();
    private final ArrayList<ClusterSession> pendingSessions = new ArrayList<>();
    private final ArrayList<ClusterSession> rejectedSessions = new ArrayList<>();
    private final Authenticator authenticator;
    private final SessionProxy sessionProxy;
    private final Aeron aeron;
    private AeronArchive archive;
    private final ConsensusModule.Context ctx;
    private final MutableDirectBuffer tempBuffer;
    private final Counter[] serviceHeartbeats;
    private final IdleStrategy idleStrategy;
    private final RecordingLog recordingLog;
    private RecordingLog.RecoveryPlan recoveryPlan;
    private UnsafeBuffer recoveryPlanBuffer;
    private Election election;

    SequencerAgent(final ConsensusModule.Context ctx)
    {
        this.ctx = ctx;
        this.aeron = ctx.aeron();
        this.epochClock = ctx.epochClock();
        this.sessionTimeoutMs = TimeUnit.NANOSECONDS.toMillis(ctx.sessionTimeoutNs());
        this.leaderHeartbeatIntervalMs = TimeUnit.NANOSECONDS.toMillis(ctx.leaderHeartbeatIntervalNs());
        this.leaderHeartbeatTimeoutMs = TimeUnit.NANOSECONDS.toMillis(ctx.leaderHeartbeatTimeoutNs());
        this.serviceHeartbeatTimeoutMs = TimeUnit.NANOSECONDS.toMillis(ctx.serviceHeartbeatTimeoutNs());
        this.egressPublisher = ctx.egressPublisher();
        this.moduleState = ctx.moduleStateCounter();
        this.controlToggle = ctx.controlToggleCounter();
        this.logPublisher = ctx.logPublisher();
        this.idleStrategy = ctx.idleStrategy();
        this.timerService = new TimerService(this);
        this.clusterMembers = ClusterMember.parse(ctx.clusterMembers());
        this.sessionProxy = new SessionProxy(egressPublisher);
        this.memberId = ctx.clusterMemberId();
        this.clusterRoleCounter = ctx.clusterNodeCounter();
        this.markFile = ctx.clusterMarkFile();
        this.recordingLog = ctx.recordingLog();
        this.tempBuffer = ctx.tempBuffer();
        this.serviceHeartbeats = ctx.serviceHeartbeatCounters();
        this.serviceAckPositions = newArray(ctx.serviceCount());

        aeronClientInvoker = aeron.conductorAgentInvoker();
        aeronClientInvoker.invoke();

        rankedPositions = new long[ClusterMember.quorumThreshold(clusterMembers.length)];
        role(Cluster.Role.FOLLOWER);

        thisMember = clusterMembers[memberId];
        final ChannelUri memberStatusUri = ChannelUri.parse(ctx.memberStatusChannel());
        memberStatusUri.put(ENDPOINT_PARAM_NAME, thisMember.memberFacingEndpoint());

        final int statusStreamId = ctx.memberStatusStreamId();
        memberStatusAdapter = new MemberStatusAdapter(
            aeron.addSubscription(memberStatusUri.toString(), statusStreamId), this);

        ClusterMember.addMemberStatusPublications(clusterMembers, thisMember, memberStatusUri, statusStreamId, aeron);

        final ChannelUri ingressUri = ChannelUri.parse(ctx.ingressChannel());
        if (!ingressUri.containsKey(ENDPOINT_PARAM_NAME))
        {
            ingressUri.put(ENDPOINT_PARAM_NAME, thisMember.clientFacingEndpoint());
        }

        ingressAdapter = new IngressAdapter(
            aeron.addSubscription(ingressUri.toString(), ctx.ingressStreamId()), this, ctx.invalidRequestCounter());

        final ChannelUri archiveUri = ChannelUri.parse(ctx.archiveContext().controlRequestChannel());
        ClusterMember.checkArchiveEndpoint(thisMember, archiveUri);
        archiveUri.put(ENDPOINT_PARAM_NAME, thisMember.archiveEndpoint());
        ctx.archiveContext().controlRequestChannel(archiveUri.toString());

        consensusModuleAdapter = new ConsensusModuleAdapter(
            aeron.addSubscription(ctx.serviceControlChannel(), ctx.consensusModuleStreamId()), this);
        serviceProxy = new ServiceProxy(aeron.addPublication(ctx.serviceControlChannel(), ctx.serviceStreamId()));

        authenticator = ctx.authenticatorSupplier().newAuthenticator(ctx);
    }

    public void onClose()
    {
        CloseHelper.close(archive);

        if (!ctx.ownsAeronClient())
        {
            for (final ClusterSession session : sessionByIdMap.values())
            {
                session.close();
            }

            CloseHelper.close(memberStatusAdapter);
            ClusterMember.closeMemberPublications(clusterMembers);

            logPublisher.disconnect();
            CloseHelper.close(ingressAdapter);
            CloseHelper.close(serviceProxy);
            CloseHelper.close(consensusModuleAdapter);
        }
    }

    public void onStart()
    {
        archive = AeronArchive.connect(ctx.archiveContext().clone());
        recoveryPlan = recordingLog.createRecoveryPlan(archive, ctx.serviceCount());
        recoveryPlanBuffer = new UnsafeBuffer(new byte[recoveryPlan.encodedLength()]);
        recoveryPlan.encode(recoveryPlanBuffer, 0);

        try (Counter ignore = addRecoveryStateCounter(recoveryPlan))
        {
            isRecovering = true;
            if (!recoveryPlan.snapshots.isEmpty())
            {
                recoverFromSnapshot(recoveryPlan.snapshots.get(0), archive);
            }

            awaitServiceAcks();

            if (!recoveryPlan.logs.isEmpty())
            {
                recoverFromLog(recoveryPlan, archive);
            }
            isRecovering = false;
        }

        if (ConsensusModule.State.SUSPENDED != state)
        {
            state(ConsensusModule.State.ACTIVE);
        }

        final long nowMs = epochClock.time();
        cachedEpochClock.update(nowMs);
        timeOfLastLogUpdateMs = nowMs;

        election = new Election(
            true,
            leadershipTermId,
            clusterMembers,
            thisMember,
            memberStatusAdapter,
            memberStatusPublisher,
            recoveryPlan,
            recoveryPlanBuffer,
            ctx,
            archive,
            this);
    }

    public int doWork()
    {
        int workCount = 0;

        boolean isSlowTickCycle = false;
        final long nowMs = epochClock.time();
        if (cachedEpochClock.time() != nowMs)
        {
            cachedEpochClock.update(nowMs);
            isSlowTickCycle = true;
        }

        if (null != election)
        {
            workCount += election.doWork(nowMs);
        }
        else
        {
            if (Cluster.Role.LEADER == role && ConsensusModule.State.ACTIVE == state)
            {
                workCount += ingressAdapter.poll();
            }
            else if (Cluster.Role.FOLLOWER == role &&
                (ConsensusModule.State.ACTIVE == state || ConsensusModule.State.SUSPENDED == state))
            {
                workCount += logAdapter.poll(followerCommitPosition);
            }

            workCount += memberStatusAdapter.poll();
            workCount += updateMemberPosition(nowMs);
            workCount += consensusModuleAdapter.poll();
        }

        if (isSlowTickCycle)
        {
            workCount += slowTickCycle(nowMs);
        }

        return workCount;
    }

    public String roleName()
    {
        return "sequencer";
    }

    public void onServiceAck(
        final long logPosition,
        final long leadershipTermId,
        final long relevantId,
        final int serviceId)
    {
        validateServiceAck(logPosition, leadershipTermId, serviceId);
        serviceAckPositions[serviceId].logPosition(logPosition).relevantId(relevantId);

        if (hasReachedThreshold(logPosition, serviceAckPositions))
        {
            switch (state)
            {
                case SNAPSHOT:
                    final long nowNs = cachedEpochClock.time();
                    takeSnapshot(nowNs, logPosition);
                    state(ConsensusModule.State.ACTIVE);
                    ClusterControl.ToggleState.reset(controlToggle);
                    for (final ClusterSession session : sessionByIdMap.values())
                    {
                        session.timeOfLastActivityMs(nowNs);
                    }
                    break;

                case SHUTDOWN:
                    takeSnapshot(cachedEpochClock.time(), logPosition);
                    recordingLog.commitLogPosition(leadershipTermId, logPosition);
                    state(ConsensusModule.State.CLOSED);
                    ctx.terminationHook().run();
                    break;

                case ABORT:
                    recordingLog.commitLogPosition(leadershipTermId, logPosition);
                    state(ConsensusModule.State.CLOSED);
                    ctx.terminationHook().run();
                    break;
            }
        }
    }

    public void onSessionConnect(
        final long correlationId,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedCredentials)
    {
        final long nowMs = cachedEpochClock.time();
        final long sessionId = nextSessionId++;
        final ClusterSession session = new ClusterSession(sessionId, responseStreamId, responseChannel);
        session.connect(aeron);
        session.lastActivity(nowMs, correlationId);

        if (pendingSessions.size() + sessionByIdMap.size() < ctx.maxConcurrentSessions())
        {
            authenticator.onConnectRequest(sessionId, encodedCredentials, nowMs);
            pendingSessions.add(session);
        }
        else
        {
            rejectedSessions.add(session);
        }
    }

    public void onSessionClose(final long clusterSessionId)
    {
        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null != session)
        {
            session.closeReason(CloseReason.CLIENT_ACTION);
            session.close();

            if (appendClosedSession(session, cachedEpochClock.time()))
            {
                sessionByIdMap.remove(clusterSessionId);
            }
        }
    }

    public ControlledFragmentAssembler.Action onSessionMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final long clusterSessionId,
        final long correlationId)
    {
        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null == session || session.state() == CLOSED)
        {
            return ControlledFragmentHandler.Action.CONTINUE;
        }

        final long nowMs = cachedEpochClock.time();
        if (session.state() == OPEN && logPublisher.appendMessage(buffer, offset, length, nowMs))
        {
            session.lastActivity(nowMs, correlationId);
            return ControlledFragmentHandler.Action.CONTINUE;
        }

        return ControlledFragmentHandler.Action.ABORT;
    }

    public void onSessionKeepAlive(final long clusterSessionId)
    {
        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null != session && session.state() == OPEN)
        {
            session.timeOfLastActivityMs(cachedEpochClock.time());
        }
    }

    public void onChallengeResponse(
        final long correlationId, final long clusterSessionId, final byte[] encodedCredentials)
    {
        for (int lastIndex = pendingSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = pendingSessions.get(i);

            if (session.id() == clusterSessionId && session.state() == CHALLENGED)
            {
                final long nowMs = cachedEpochClock.time();
                session.lastActivity(nowMs, correlationId);
                authenticator.onChallengeResponse(clusterSessionId, encodedCredentials, nowMs);
                break;
            }
        }
    }

    public boolean onTimerEvent(final long correlationId, final long nowMs)
    {
        return Cluster.Role.LEADER != role || logPublisher.appendTimerEvent(correlationId, nowMs);
    }

    public void onScheduleTimer(final long correlationId, final long deadlineMs)
    {
        timerService.scheduleTimer(correlationId, deadlineMs);
    }

    public void onCancelTimer(final long correlationId)
    {
        timerService.cancelTimer(correlationId);
    }

    public void onServiceCloseSession(final long clusterSessionId)
    {
        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null != session)
        {
            session.closeReason(CloseReason.SERVICE_ACTION);
            session.close();

            if (Cluster.Role.LEADER == role && appendClosedSession(session, cachedEpochClock.time()))
            {
                sessionByIdMap.remove(clusterSessionId);
            }
        }
    }

    public void onCanvassPosition(final long logPosition, final long leadershipTermId, final int followerMemberId)
    {
        if (null != election)
        {
            election.onAppendedPosition(logPosition, leadershipTermId, followerMemberId);
        }
    }

    public void onRequestVote(final long logPosition, final long candidateTermId, final int candidateId)
    {
        if (null != election)
        {
            election.onRequestVote(logPosition, candidateTermId, candidateId);
        }
        else if (candidateTermId > this.leadershipTermId)
        {
            // TODO: Create an election
        }
    }

    public void onNewLeadershipTerm(
        final long logPosition, final long leadershipTermId, final int leaderMemberId, final int logSessionId)
    {
        if (null != election)
        {
            election.onNewLeadershipTerm(logPosition, leadershipTermId, leaderMemberId, logSessionId);
        }
        else if (leadershipTermId > this.leadershipTermId)
        {
            // TODO: Follow new leader
        }
    }

    public void onVote(
        final long candidateTermId, final int candidateMemberId, final int followerMemberId, final boolean vote)
    {
        if (null != election)
        {
            election.onVote(candidateTermId, candidateMemberId, followerMemberId, vote);
        }
    }

    public void onAppendedPosition(final long logPosition, final long leadershipTermId, final int followerMemberId)
    {
        if (null != election)
        {
            election.onAppendedPosition(logPosition, leadershipTermId, followerMemberId);
        }
        else if (Cluster.Role.LEADER == role && leadershipTermId == this.leadershipTermId)
        {
            clusterMembers[followerMemberId].logPosition(logPosition);
        }
    }

    public void onCommitPosition(final long logPosition, final long leadershipTermId, final int leaderMemberId)
    {
        if (null != election)
        {
            election.onCommitPosition(logPosition, leadershipTermId, leaderMemberId);
        }
        else if (Cluster.Role.FOLLOWER == role && leadershipTermId == this.leadershipTermId)
        {
            timeOfLastLogUpdateMs = cachedEpochClock.time();
            followerCommitPosition = logPosition;
        }
        else if (Cluster.Role.LEADER == role && leadershipTermId > this.leadershipTermId)
        {
            // TODO: Follow new leader
        }
    }

    public void onQueryResponse(
        final long correlationId,
        final int requestMemberId,
        final int responseMemberId,
        final DirectBuffer data,
        final int offset,
        final int length)
    {
        if (null != election)
        {
            election.onQueryResponse(correlationId, requestMemberId, responseMemberId, data, offset, length);
        }
    }

    public void onRecoveryPlanQuery(final long correlationId, final int leaderMemberId, final int requestMemberId)
    {
        if (null != election)
        {
            election.onRecoveryPlanQuery(correlationId, leaderMemberId, requestMemberId);
        }
        else
        {
            if (leaderMemberId == memberId)
            {
                memberStatusPublisher.queryResponse(
                    clusterMembers[requestMemberId].publication(),
                    correlationId,
                    requestMemberId,
                    memberId,
                    recoveryPlanBuffer,
                    0,
                    recoveryPlanBuffer.capacity());
            }
        }
    }

    void state(final ConsensusModule.State state)
    {
        this.state = state;
        moduleState.set(state.code());
    }

    void role(final Cluster.Role role)
    {
        this.role = role;
        clusterRoleCounter.setOrdered(role.code());
    }

    Cluster.Role role()
    {
        return role;
    }

    void appendedPositionCounter(final ReadableCounter appendedPositionCounter)
    {
        this.appendedPosition = appendedPositionCounter;
    }

    void commitPositionCounter(final Counter commitPositionCounter)
    {
        this.commitPosition = commitPositionCounter;
    }

    @SuppressWarnings("unused")
    void onReplaySessionMessage(
        final long correlationId,
        final long clusterSessionId,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        cachedEpochClock.update(timestamp);
        sessionByIdMap.get(clusterSessionId).lastActivity(timestamp, correlationId);
    }

    void onReplayTimerEvent(final long correlationId, final long timestamp)
    {
        cachedEpochClock.update(timestamp);
        timerService.cancelTimer(correlationId);
    }

    void onReplaySessionOpen(
        final long logPosition,
        final long correlationId,
        final long clusterSessionId,
        final long timestamp,
        final int responseStreamId,
        final String responseChannel)
    {
        cachedEpochClock.update(timestamp);

        final ClusterSession session = new ClusterSession(clusterSessionId, responseStreamId, responseChannel);
        session.open(logPosition);
        session.lastActivity(timestamp, correlationId);

        sessionByIdMap.put(clusterSessionId, session);
        if (clusterSessionId >= nextSessionId)
        {
            nextSessionId = clusterSessionId + 1;
        }
    }

    void onLoadSession(
        final long logPosition,
        final long correlationId,
        final long clusterSessionId,
        final long timestamp,
        final CloseReason closeReason,
        final int responseStreamId,
        final String responseChannel)
    {
        final ClusterSession session = new ClusterSession(clusterSessionId, responseStreamId, responseChannel);
        session.closeReason(closeReason);
        session.open(logPosition);
        session.lastActivity(timestamp, correlationId);

        if (CloseReason.NULL_VAL != closeReason)
        {
            session.close();
        }

        sessionByIdMap.put(clusterSessionId, session);
        if (clusterSessionId >= nextSessionId)
        {
            nextSessionId = clusterSessionId + 1;
        }
    }

    @SuppressWarnings("unused")
    void onReplaySessionClose(
        final long correlationId, final long clusterSessionId, final long timestamp, final CloseReason closeReason)
    {
        cachedEpochClock.update(timestamp);
        sessionByIdMap.remove(clusterSessionId).close();
    }

    @SuppressWarnings("unused")
    void onReplayClusterAction(
        final long logPosition, final long leadershipTermId, final long timestamp, final ClusterAction action)
    {
        cachedEpochClock.update(timestamp);

        switch (action)
        {
            case SUSPEND:
                state(ConsensusModule.State.SUSPENDED);
                break;

            case RESUME:
                state(ConsensusModule.State.ACTIVE);
                break;

            case SNAPSHOT:
                if (!isRecovering)
                {
                    expectedAckPosition = logPosition;
                    state(ConsensusModule.State.SNAPSHOT);
                }
                break;

            case SHUTDOWN:
                if (!isRecovering)
                {
                    expectedAckPosition = logPosition;
                    state(ConsensusModule.State.SHUTDOWN);
                }
                break;

            case ABORT:
                if (!isRecovering)
                {
                    expectedAckPosition = logPosition;
                    state(ConsensusModule.State.ABORT);
                }
                break;
        }
    }

    void onReloadState(final long nextSessionId)
    {
        this.nextSessionId = nextSessionId;
    }

    void becomeLeader()
    {
        role(Cluster.Role.LEADER);
        updateMemberDetails();

        final ChannelUri channelUri = ChannelUri.parse(ctx.logChannel());
        final Publication publication = createLogPublication(channelUri, recoveryPlan, election.logPosition());

        logAdapter = null;
        logPublisher.connect(publication);
        final int logSessionId = publication.sessionId();
        election.logSessionId(logSessionId);

        channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(logSessionId));
        startLogRecording(election.logPosition(), channelUri.toString(), logSessionId, SourceLocation.LOCAL);
        awaitServicesReady(channelUri, logSessionId);
    }

    void updateMemberDetails()
    {
        leadershipTermId = election.leadershipTermId();
        leaderMember = election.leader();
        followerCommitPosition = election.logPosition();

        for (final ClusterMember clusterMember : clusterMembers)
        {
            clusterMember.isLeader(clusterMember.id() == leaderMember.id());
        }

        updateClientConnectDetails(clusterMembers, leaderMember.id());
    }

    void recordLogAsFollower(final String logChannel, final int logSessionId)
    {
        startLogRecording(election.logPosition(), logChannel, logSessionId, SourceLocation.REMOTE);

        final Image image = awaitImage(logSessionId, aeron.addSubscription(logChannel, ctx.logStreamId()));
        logAdapter = new LogAdapter(image, this);
        lastAppendedPosition = 0;
    }

    void awaitServicesReady(final ChannelUri logChannelUri, final int logSessionId)
    {
        final String channel = Cluster.Role.LEADER == role && UDP_MEDIA.equals(logChannelUri.media()) ?
            logChannelUri.prefix(SPY_QUALIFIER).toString() : logChannelUri.toString();
        serviceProxy.joinLog(leadershipTermId, commitPosition.id(), logSessionId, ctx.logStreamId(), false, channel);

        resetToNull(serviceAckPositions);
        awaitServiceAcks();
    }

    void electionComplete()
    {
        election = null;

        if (Cluster.Role.LEADER == role)
        {
            for (final ClusterSession session : sessionByIdMap.values())
            {
                if (session.state() != CLOSED)
                {
                    session.connect(aeron);
                }
            }

            final long nowMs = epochClock.time();
            for (final ClusterSession session : sessionByIdMap.values())
            {
                if (session.state() != CLOSED)
                {
                    session.timeOfLastActivityMs(nowMs);
                }
            }
        }
    }

    void catchupLog(final RecordingCatchUp recordingCatchUp)
    {
        final long fromPosition = recordingCatchUp.fromPosition();
        final long targetPosition = recordingCatchUp.targetPosition();
        final long length = targetPosition - fromPosition;

        final int lastStepIndex = recoveryPlan.logs.size() - 1;
        final RecordingLog.Log log = recoveryPlan.logs.get(lastStepIndex);

        final long originalLeadershipTermId = leadershipTermId;
        leadershipTermId = log.leadershipTermId;
        expectedAckPosition = fromPosition;

        try (Counter counter = CommitPos.allocate(aeron, tempBuffer, leadershipTermId, fromPosition, targetPosition))
        {
            final int streamId = ctx.replayStreamId();
            final ChannelUri channelUri = ChannelUri.parse(ctx.replayChannel());
            final int logSessionId = lastStepIndex + 1;
            channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(logSessionId));
            final String channel = channelUri.toString();

            try (Subscription subscription = aeron.addSubscription(channel, streamId))
            {
                logAdapter = null;
                serviceProxy.joinLog(leadershipTermId, counter.id(), logSessionId, streamId, true, channel);
                resetToNull(serviceAckPositions);
                awaitServiceAcks();

                final int replaySessionId = (int)archive.startReplay(
                    recordingCatchUp.recordingIdToExtend(), fromPosition, length, channel, streamId);

                replayLog(awaitImage(replaySessionId, subscription), targetPosition, counter);

                recordingLog.commitLogPosition(leadershipTermId, targetPosition);
                expectedAckPosition = targetPosition;
            }
        }

        leadershipTermId = originalLeadershipTermId;
    }

    private int slowTickCycle(final long nowMs)
    {
        int workCount = 0;

        markFile.updateActivityTimestamp(nowMs);
        checkServiceHeartbeats(nowMs);
        workCount += aeronClientInvoker.invoke();

        if (Cluster.Role.LEADER == role)
        {
            workCount += checkControlToggle(nowMs);

            if (ConsensusModule.State.ACTIVE == state)
            {
                workCount += processPendingSessions(pendingSessions, nowMs);
                workCount += checkSessions(sessionByIdMap, nowMs);
                workCount += processRejectedSessions(rejectedSessions, nowMs);
                workCount += timerService.poll(nowMs);
            }
        }

        if (null != archive)
        {
            archive.checkForErrorResponse();
        }

        return workCount;
    }

    private void checkServiceHeartbeats(final long nowMs)
    {
        final long heartbeatThreshold = nowMs - serviceHeartbeatTimeoutMs;
        for (final Counter serviceHeartbeat : serviceHeartbeats)
        {
            if (serviceHeartbeat.get() < heartbeatThreshold)
            {
                ctx.errorHandler().onError(new TimeoutException("no heartbeat from clustered service"));
                ctx.terminationHook().run();
            }
        }
    }

    private int checkControlToggle(final long nowMs)
    {
        switch (ClusterControl.ToggleState.get(controlToggle))
        {
            case SUSPEND:
                if (ConsensusModule.State.ACTIVE == state && appendAction(ClusterAction.SUSPEND, nowMs))
                {
                    state(ConsensusModule.State.SUSPENDED);
                    ClusterControl.ToggleState.reset(controlToggle);
                }
                break;

            case RESUME:
                if (ConsensusModule.State.SUSPENDED == state && appendAction(ClusterAction.RESUME, nowMs))
                {
                    state(ConsensusModule.State.ACTIVE);
                    ClusterControl.ToggleState.reset(controlToggle);
                }
                break;

            case SNAPSHOT:
                if (ConsensusModule.State.ACTIVE == state && appendAction(ClusterAction.SNAPSHOT, nowMs))
                {
                    expectedAckPosition = logPosition();
                    state(ConsensusModule.State.SNAPSHOT);
                }
                break;

            case SHUTDOWN:
                if (ConsensusModule.State.ACTIVE == state && appendAction(ClusterAction.SHUTDOWN, nowMs))
                {
                    expectedAckPosition = logPosition();
                    state(ConsensusModule.State.SHUTDOWN);
                }
                break;

            case ABORT:
                if (ConsensusModule.State.ACTIVE == state && appendAction(ClusterAction.ABORT, nowMs))
                {
                    expectedAckPosition = logPosition();
                    state(ConsensusModule.State.ABORT);
                }
                break;

            default:
                return 0;
        }

        return 1;
    }

    private boolean appendAction(final ClusterAction action, final long nowMs)
    {
        final int headersLength = DataHeaderFlyweight.HEADER_LENGTH +
            MessageHeaderEncoder.ENCODED_LENGTH +
            ClusterActionRequestEncoder.BLOCK_LENGTH;

        final long position = logPublisher.position() + BitUtil.align(headersLength, FRAME_ALIGNMENT);

        return logPublisher.appendClusterAction(action, position, leadershipTermId, nowMs);
    }

    private int processPendingSessions(final ArrayList<ClusterSession> pendingSessions, final long nowMs)
    {
        int workCount = 0;

        for (int lastIndex = pendingSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = pendingSessions.get(i);

            if (session.state() == INIT || session.state() == CONNECTED)
            {
                if (session.isResponsePublicationConnected())
                {
                    session.state(CONNECTED);
                    authenticator.onProcessConnectedSession(sessionProxy.session(session), nowMs);
                }
            }

            if (session.state() == CHALLENGED)
            {
                if (session.isResponsePublicationConnected())
                {
                    authenticator.onProcessChallengedSession(sessionProxy.session(session), nowMs);
                }
            }

            if (session.state() == AUTHENTICATED)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                session.timeOfLastActivityMs(nowMs);
                sessionByIdMap.put(session.id(), session);
                appendConnectedSession(session, nowMs);

                workCount += 1;
            }
            else if (session.state() == REJECTED)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                rejectedSessions.add(session);
            }
            else if (nowMs > (session.timeOfLastActivityMs() + sessionTimeoutMs))
            {
                ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                session.close();
            }
        }

        return workCount;
    }

    private int processRejectedSessions(final ArrayList<ClusterSession> rejectedSessions, final long nowMs)
    {
        int workCount = 0;

        for (int lastIndex = rejectedSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = rejectedSessions.get(i);
            String detail = ConsensusModule.Configuration.SESSION_LIMIT_MSG;
            EventCode eventCode = EventCode.ERROR;

            if (session.state() == REJECTED)
            {
                detail = ConsensusModule.Configuration.SESSION_REJECTED_MSG;
                eventCode = EventCode.AUTHENTICATION_REJECTED;
            }

            if (egressPublisher.sendEvent(session, eventCode, detail) ||
                nowMs > (session.timeOfLastActivityMs() + sessionTimeoutMs))
            {
                ArrayListUtil.fastUnorderedRemove(rejectedSessions, i, lastIndex--);
                session.close();
                workCount++;
            }
        }

        return workCount;
    }

    private int checkSessions(final Long2ObjectHashMap<ClusterSession> sessionByIdMap, final long nowMs)
    {
        int workCount = 0;

        for (final Iterator<ClusterSession> i = sessionByIdMap.values().iterator(); i.hasNext(); )
        {
            final ClusterSession session = i.next();

            if (nowMs > (session.timeOfLastActivityMs() + sessionTimeoutMs))
            {
                switch (session.state())
                {
                    case OPEN:
                        egressPublisher.sendEvent(session, EventCode.ERROR, SESSION_TIMEOUT_MSG);
                        session.closeReason(CloseReason.TIMEOUT);
                        session.close();
                        if (appendClosedSession(session, nowMs))
                        {
                            i.remove();
                        }
                        break;

                    case CLOSED:
                        if (appendClosedSession(session, nowMs))
                        {
                            session.close();
                            i.remove();
                        }
                        break;

                    default:
                        session.close();
                        i.remove();
                }

                workCount += 1;
            }
            else if (session.state() == CONNECTED)
            {
                appendConnectedSession(session, nowMs);
                workCount += 1;
            }
        }

        return workCount;
    }

    private void appendConnectedSession(final ClusterSession session, final long nowMs)
    {
        final long resultingPosition = logPublisher.appendConnectedSession(session, nowMs);
        if (resultingPosition > 0)
        {
            session.open(resultingPosition);
        }
    }

    private boolean appendClosedSession(final ClusterSession session, final long nowMs)
    {
        if (logPublisher.appendClosedSession(session, nowMs))
        {
            session.close();
            return true;
        }

        return false;
    }

    private void createPositionCounters(final int logSessionId, final long logPosition)
    {
        final CountersReader counters = aeron.countersReader();
        final int recordingCounterId = awaitRecordingCounter(counters, logSessionId);

        appendedPosition = new ReadableCounter(counters, recordingCounterId);
        commitPosition = CommitPos.allocate(aeron, tempBuffer, leadershipTermId, logPosition, Long.MAX_VALUE);
    }

    private void recoverFromSnapshot(final RecordingLog.Snapshot snapshot, final AeronArchive archive)
    {
        cachedEpochClock.update(snapshot.timestamp);
        expectedAckPosition = snapshot.logPosition;
        leadershipTermId = snapshot.leadershipTermId;

        final String channel = ctx.replayChannel();
        final int streamId = ctx.replayStreamId();
        final int sessionId = (int)archive.startReplay(snapshot.recordingId, 0, NULL_LENGTH, channel, streamId);
        final String replaySubscriptionChannel = ChannelUri.addSessionId(channel, sessionId);

        try (Subscription subscription = aeron.addSubscription(replaySubscriptionChannel, streamId))
        {
            final Image image = awaitImage(sessionId, subscription);
            final SnapshotLoader snapshotLoader = new SnapshotLoader(image, this);

            while (true)
            {
                final int fragments = snapshotLoader.poll();
                if (fragments == 0)
                {
                    if (snapshotLoader.isDone())
                    {
                        break;
                    }

                    if (image.isClosed())
                    {
                        throw new IllegalStateException("snapshot ended unexpectedly");
                    }
                }

                idle(fragments);
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

    private void recoverFromLog(final RecordingLog.RecoveryPlan plan, final AeronArchive archive)
    {
        final int streamId = ctx.replayStreamId();
        final ChannelUri channelUri = ChannelUri.parse(ctx.replayChannel());

        if (!plan.logs.isEmpty())
        {
            final RecordingLog.Log log = plan.logs.get(0);
            final long startPosition = log.startPosition;
            final long stopPosition = log.stopPosition;
            final long length = stopPosition - startPosition;
            leadershipTermId = log.leadershipTermId;

            if (NULL_VALUE == log.logPosition)
            {
                recordingLog.commitLogPosition(leadershipTermId, stopPosition);
            }

            if (plan.hasReplay())
            {
                channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(log.sessionId));
                final String channel = channelUri.toString();
                final long recordingId = log.recordingId;

                try (Counter counter = CommitPos.allocate(
                    aeron, tempBuffer, leadershipTermId, startPosition, stopPosition))
                {
                    serviceProxy.joinLog(leadershipTermId, counter.id(), log.sessionId, streamId, true, channel);

                    try (Subscription subscription = aeron.addSubscription(channel, streamId))
                    {
                        expectedAckPosition = startPosition;
                        resetToNull(serviceAckPositions);
                        awaitServiceAcks();

                        final Image image = awaitImage(
                            (int)archive.startReplay(recordingId, startPosition, length, channel, streamId),
                            subscription);

                        resetToNull(serviceAckPositions);
                        replayLog(image, stopPosition, counter);
                        awaitServiceAcks();

                        int workCount;
                        while (0 != (workCount = timerService.poll(cachedEpochClock.time())))
                        {
                            idle(workCount);
                        }
                    }
                }
            }
        }
    }

    private Counter addRecoveryStateCounter(final RecordingLog.RecoveryPlan plan)
    {
        final int snapshotsCount = plan.snapshots.size();

        if (snapshotsCount > 0)
        {
            final long[] serviceSnapshotRecordingIds = new long[snapshotsCount - 1];
            final RecordingLog.Snapshot snapshot = plan.snapshots.get(0);

            for (int i = 1; i < snapshotsCount; i++)
            {
                final RecordingLog.Snapshot serviceSnapshot = plan.snapshots.get(i);
                serviceSnapshotRecordingIds[serviceSnapshot.serviceId] = serviceSnapshot.recordingId;
            }

            return RecoveryState.allocate(
                aeron,
                tempBuffer,
                snapshot.leadershipTermId,
                snapshot.logPosition,
                snapshot.timestamp,
                plan.hasReplay(),
                serviceSnapshotRecordingIds);
        }

        return RecoveryState.allocate(aeron, tempBuffer, leadershipTermId, 0, 0, plan.hasReplay());
    }

    private void awaitServiceAcks()
    {
        final long logPosition = logPosition();
        while (!hasReachedThreshold(logPosition, serviceAckPositions))
        {
            idle(consensusModuleAdapter.poll());
        }
    }

    private void validateServiceAck(final long logPosition, final long leadershipTermId, final int serviceId)
    {
        if (logPosition != expectedAckPosition || leadershipTermId != this.leadershipTermId)
        {
            throw new IllegalStateException("invalid log state for service ACK" +
                ": serviceId=" + serviceId +
                ", logPosition=" + logPosition + " expected " + expectedAckPosition +
                ", leadershipTermId=" + leadershipTermId + " expected " + this.leadershipTermId);
        }
    }

    private long logPosition()
    {
        return null != logAdapter ? logAdapter.position() : logPublisher.position();
    }

    private void updateClientConnectDetails(final ClusterMember[] members, final int leaderMemberId)
    {
        final StringBuilder builder = new StringBuilder(100);
        builder.append(members[leaderMemberId].clientFacingEndpoint());

        for (int i = 0, length = members.length; i < length; i++)
        {
            if (i != leaderMemberId)
            {
                builder.append(',').append(members[i].clientFacingEndpoint());
            }
        }

        sessionProxy.memberEndpointsDetail(builder.toString());
    }

    private int updateMemberPosition(final long nowMs)
    {
        int workCount = 0;

        if (Cluster.Role.LEADER == role)
        {
            thisMember.logPosition(appendedPosition.get());

            final long quorumPosition = ClusterMember.quorumPosition(clusterMembers, rankedPositions);
            final long commitPosition = this.commitPosition.getWeak();
            if (quorumPosition > commitPosition || nowMs >= (timeOfLastLogUpdateMs + leaderHeartbeatIntervalMs))
            {
                for (final ClusterMember member : clusterMembers)
                {
                    if (member != thisMember)
                    {
                        final Publication publication = member.publication();
                        memberStatusPublisher.commitPosition(publication, quorumPosition, leadershipTermId, memberId);
                    }
                }

                this.commitPosition.setOrdered(quorumPosition);
                timeOfLastLogUpdateMs = nowMs;

                workCount = 1;
            }
        }
        else if (Cluster.Role.FOLLOWER == role)
        {
            final long appendedPosition = this.appendedPosition.get();
            if (appendedPosition != lastAppendedPosition)
            {
                final Publication publication = leaderMember.publication();
                if (memberStatusPublisher.appendedPosition(publication, appendedPosition, leadershipTermId, memberId))
                {
                    lastAppendedPosition = appendedPosition;
                }

                workCount = 1;
            }

            commitPosition.proposeMaxOrdered(logAdapter.position());

            if (nowMs >= (timeOfLastLogUpdateMs + leaderHeartbeatTimeoutMs))
            {
                throw new AgentTerminationException("no heartbeat from cluster leader");
            }
        }

        return workCount;
    }

    private void idle()
    {
        checkInterruptedStatus();
        aeronClientInvoker.invoke();
        idleStrategy.idle();
    }

    private void idle(final int workCount)
    {
        checkInterruptedStatus();
        aeronClientInvoker.invoke();
        idleStrategy.idle(workCount);
    }

    private static void checkInterruptedStatus()
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new RuntimeException("unexpected interrupt");
        }
    }

    private void takeSnapshot(final long timestampMs, final long logPosition)
    {
        try (Publication publication = archive.addRecordedExclusivePublication(
            ctx.snapshotChannel(), ctx.snapshotStreamId()))
        {
            try
            {
                final CountersReader counters = aeron.countersReader();
                final int counterId = awaitRecordingCounter(counters, publication.sessionId());
                final long recordingId = RecordingPos.getRecordingId(counters, counterId);
                final long termBaseLogPosition = recoveryPlan.lastAppendedLogPosition;

                snapshotState(publication, logPosition, leadershipTermId);
                awaitRecordingComplete(recordingId, publication.position(), counters, counterId);

                for (int i = serviceAckPositions.length - 1; i >= 0; i--)
                {
                    final long snapshotRecordingId = serviceAckPositions[i].relevantId();
                    recordingLog.appendSnapshot(
                        snapshotRecordingId, leadershipTermId, termBaseLogPosition, logPosition, timestampMs, i);
                }

                recordingLog.appendSnapshot(
                    recordingId, leadershipTermId, termBaseLogPosition, logPosition, timestampMs, NULL_SERVICE_ID);
            }
            finally
            {
                archive.stopRecording(publication);
            }

            ctx.snapshotCounter().incrementOrdered();
        }
    }

    private void awaitRecordingComplete(
        final long recordingId, final long position, final CountersReader counters, final int counterId)
    {
        idleStrategy.reset();
        do
        {
            idle();

            if (!RecordingPos.isActive(counters, counterId, recordingId))
            {
                throw new IllegalStateException("recording has stopped unexpectedly: " + recordingId);
            }
        }
        while (counters.getCounterValue(counterId) < position);
    }

    private int awaitRecordingCounter(final CountersReader counters, final int sessionId)
    {
        idleStrategy.reset();
        int counterId = RecordingPos.findCounterIdBySession(counters, sessionId);
        while (CountersReader.NULL_COUNTER_ID == counterId)
        {
            idle();
            counterId = RecordingPos.findCounterIdBySession(counters, sessionId);
        }

        return counterId;
    }

    private void snapshotState(final Publication publication, final long logPosition, final long leadershipTermId)
    {
        final ConsensusModuleSnapshotTaker snapshotTaker = new ConsensusModuleSnapshotTaker(
            publication, idleStrategy, aeronClientInvoker);

        snapshotTaker.markBegin(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0);

        for (final ClusterSession session : sessionByIdMap.values())
        {
            if (session.state() == OPEN)
            {
                snapshotTaker.snapshotSession(session);
            }
        }

        aeronClientInvoker.invoke();

        timerService.snapshot(snapshotTaker);
        snapshotTaker.sequencerState(nextSessionId);

        snapshotTaker.markEnd(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0);
    }

    private Publication createLogPublication(
        final ChannelUri channelUri, final RecordingLog.RecoveryPlan plan, final long position)
    {
        if (!plan.logs.isEmpty())
        {
            final RecordingLog.Log log = plan.logs.get(0);
            final int termLength = log.termBufferLength;
            final int initialTermId = log.initialTermId;
            final int bitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);
            final int termId = LogBufferDescriptor.computeTermIdFromPosition(position, bitsToShift, initialTermId);
            final int termOffset = (int)(position & (termLength - 1));

            channelUri.put(MTU_LENGTH_PARAM_NAME, Integer.toString(log.mtuLength));
            channelUri.put(TERM_LENGTH_PARAM_NAME, Integer.toString(termLength));
            channelUri.put(INITIAL_TERM_ID_PARAM_NAME, Integer.toString(initialTermId));
            channelUri.put(TERM_ID_PARAM_NAME, Integer.toString(termId));
            channelUri.put(TERM_OFFSET_PARAM_NAME, Integer.toString(termOffset));
        }

        final Publication publication = aeron.addExclusivePublication(channelUri.toString(), ctx.logStreamId());

        if (!channelUri.containsKey(ENDPOINT_PARAM_NAME) && UDP_MEDIA.equals(channelUri.media()))
        {
            final ChannelUriStringBuilder builder = new ChannelUriStringBuilder().media(UDP_MEDIA);
            for (final ClusterMember member : clusterMembers)
            {
                if (member != thisMember)
                {
                    publication.addDestination(builder.endpoint(member.logEndpoint()).build());
                }
            }
        }

        return publication;
    }

    private void startLogRecording(
        final long position, final String channel, final int sessionId, final SourceLocation sourceLocation)
    {
        if (recoveryPlan.logs.isEmpty())
        {
            archive.startRecording(channel, ctx.logStreamId(), sourceLocation);
        }
        else
        {
            final RecordingLog.Log log = recoveryPlan.logs.get(0);
            archive.extendRecording(log.recordingId, channel, ctx.logStreamId(), sourceLocation);
        }

        createPositionCounters(sessionId, position);

        final long recordingId = RecordingPos.getRecordingId(aeron.countersReader(), appendedPosition.counterId());
        recordingLog.commitLeadershipRecordingId(leadershipTermId, recordingId);
    }

    private void replayLog(final Image image, final long stopPosition, final Counter commitPosition)
    {
        final LogAdapter logAdapter = new LogAdapter(image, this);
        expectedAckPosition = stopPosition;

        while (true)
        {
            int workCount = logAdapter.poll(stopPosition);
            if (workCount == 0)
            {
                if (image.position() == stopPosition)
                {
                    break;
                }

                if (image.isClosed())
                {
                    throw new IllegalStateException("unexpected close of image when replaying log");
                }
            }

            commitPosition.setOrdered(image.position());

            workCount += consensusModuleAdapter.poll();
            workCount += timerService.poll(cachedEpochClock.time());

            idle(workCount);
        }
    }
}
