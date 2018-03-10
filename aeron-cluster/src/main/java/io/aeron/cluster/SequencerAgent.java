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
import io.aeron.cluster.service.*;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.status.ReadableCounter;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.CountersReader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.aeron.ChannelUri.SPY_QUALIFIER;
import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.ClusterMember.NULL_MEMBER_ID;
import static io.aeron.cluster.ClusterSession.State.*;
import static io.aeron.cluster.ConsensusModule.Configuration.SESSION_TIMEOUT_MSG;
import static io.aeron.cluster.ConsensusModule.SNAPSHOT_TYPE_ID;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.agrona.concurrent.status.CountersReader.METADATA_LENGTH;

class SequencerAgent implements Agent, ServiceControlListener
{
    private boolean isRecovering;
    private final int memberId;
    private int votedForMemberId;
    private int serviceAckCount = 0;
    private int logSessionId;
    private final long sessionTimeoutMs;
    private final long heartbeatIntervalMs;
    private final long heartbeatTimeoutMs;
    private long nextSessionId = 1;
    private long baseLogPosition = 0;
    private long leadershipTermId = -1;
    private long lastRecordingPosition = 0;
    private long timeOfLastLogUpdateMs = 0;
    private long followerCommitPosition = 0;
    private long logRecordingId;
    private ReadableCounter logRecordingPosition;
    private Counter commitPosition;
    private ConsensusModule.State state = ConsensusModule.State.INIT;
    private Cluster.Role role;
    private ClusterMember[] clusterMembers;
    private ClusterMember leaderMember;
    private final ClusterMember thisMember;
    private long[] rankedPositions;
    private final Counter clusterRoleCounter;
    private final ClusterMarkFile markFile;
    private final AgentInvoker aeronClientInvoker;
    private final EpochClock epochClock;
    private final CachedEpochClock cachedEpochClock = new CachedEpochClock();
    private final Counter moduleState;
    private final Counter controlToggle;
    private final TimerService timerService;
    private final ServiceControlAdapter serviceControlAdapter;
    private final ServiceControlPublisher serviceControlPublisher;
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
    private final UnsafeBuffer tempBuffer = new UnsafeBuffer(new byte[METADATA_LENGTH]);
    private final IdleStrategy idleStrategy;
    private final RecordingLog recordingLog;
    private RecordingLog.RecoveryPlan recoveryPlan;

    SequencerAgent(
        final ConsensusModule.Context ctx,
        final EgressPublisher egressPublisher,
        final LogPublisher logPublisher)
    {
        this.ctx = ctx;
        this.aeron = ctx.aeron();
        this.epochClock = ctx.epochClock();
        this.sessionTimeoutMs = TimeUnit.NANOSECONDS.toMillis(ctx.sessionTimeoutNs());
        this.heartbeatIntervalMs = TimeUnit.NANOSECONDS.toMillis(ctx.heartbeatIntervalNs());
        this.heartbeatTimeoutMs = TimeUnit.NANOSECONDS.toMillis(ctx.heartbeatTimeoutNs());
        this.egressPublisher = egressPublisher;
        this.moduleState = ctx.moduleStateCounter();
        this.controlToggle = ctx.controlToggleCounter();
        this.logPublisher = logPublisher;
        this.idleStrategy = ctx.idleStrategy();
        this.timerService = new TimerService(this);
        this.clusterMembers = ClusterMember.parse(ctx.clusterMembers());
        this.sessionProxy = new SessionProxy(egressPublisher);
        this.memberId = ctx.clusterMemberId();
        this.votedForMemberId = ctx.appointedLeaderId();
        this.clusterRoleCounter = ctx.clusterNodeCounter();
        this.markFile = ctx.clusterMarkFile();
        this.recordingLog = ctx.recordingLog();

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

        serviceControlAdapter = new ServiceControlAdapter(
            aeron.addSubscription(ctx.serviceControlChannel(), ctx.serviceControlStreamId()), this);
        serviceControlPublisher = new ServiceControlPublisher(
            aeron.addPublication(ctx.serviceControlChannel(), ctx.serviceControlStreamId()));

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
            CloseHelper.close(serviceControlPublisher);
            CloseHelper.close(serviceControlAdapter);
        }
    }

    public void onStart()
    {
        archive = AeronArchive.connect(ctx.archiveContext());
        recoveryPlan = recordingLog.createRecoveryPlan(archive);

        try (Counter ignore = addRecoveryStateCounter(recoveryPlan))
        {
            isRecovering = true;
            if (null != recoveryPlan.snapshotStep)
            {
                recoverFromSnapshot(recoveryPlan.snapshotStep, archive);
            }

            awaitServiceAcks();

            if (recoveryPlan.termSteps.size() > 0)
            {
                recoverFromLog(recoveryPlan.termSteps, archive);
            }

            isRecovering = false;
        }

        if (ConsensusModule.State.SUSPENDED != state)
        {
            state(ConsensusModule.State.ACTIVE);
        }

        leadershipTermId++;

        if (clusterMembers.length > 1)
        {
            electLeader();
        }
        else
        {
            votedForMemberId = memberId;
        }

        if (memberId == votedForMemberId || clusterMembers.length == 1)
        {
            becomeLeader();
        }
        else
        {
            becomeFollower();
        }

        final long nowMs = epochClock.time();
        cachedEpochClock.update(nowMs);
        timeOfLastLogUpdateMs = nowMs;

        recordingLog.appendTerm(logRecordingId, leadershipTermId, baseLogPosition, nowMs, votedForMemberId);
    }

    public int doWork()
    {
        int workCount = 0;

        boolean isSlowTickCycle = false;
        final long nowMs = epochClock.time();
        if (cachedEpochClock.time() != nowMs)
        {
            isSlowTickCycle = true;
            cachedEpochClock.update(nowMs);
        }

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
        final long logPosition, final long leadershipTermId, final int serviceId, final ClusterAction action)
    {
        validateServiceAck(logPosition, leadershipTermId, serviceId, action);

        if (++serviceAckCount == ctx.serviceCount())
        {
            if (isRecovering)
            {
                return;
            }

            final long termPosition = currentTermPosition();
            switch (action)
            {
                case SNAPSHOT:
                    final long nowNs = cachedEpochClock.time();
                    takeSnapshot(nowNs, termPosition);
                    state(ConsensusModule.State.ACTIVE);
                    ClusterControl.ToggleState.reset(controlToggle);

                    for (final ClusterSession session : sessionByIdMap.values())
                    {
                        session.timeOfLastActivityMs(nowNs);
                    }
                    break;

                case SHUTDOWN:
                    takeSnapshot(cachedEpochClock.time(), termPosition);
                    recordingLog.commitLeadershipTermPosition(leadershipTermId, termPosition);
                    state(ConsensusModule.State.CLOSED);
                    ctx.terminationHook().run();
                    break;

                case ABORT:
                    recordingLog.commitLeadershipTermPosition(leadershipTermId, termPosition);
                    state(ConsensusModule.State.CLOSED);
                    ctx.terminationHook().run();
                    break;
            }
        }
        else if (serviceAckCount > ctx.serviceCount())
        {
            throw new IllegalStateException("Service count exceeded: " + serviceAckCount);
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
        final long nowMs = cachedEpochClock.time();
        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null == session || session.state() == CLOSED)
        {
            return ControlledFragmentHandler.Action.CONTINUE;
        }

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
        if (null != session)
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

    public void onAdminQuery(final long correlationId, final long clusterSessionId, final AdminQueryType queryType)
    {
        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null != session && session.state() == OPEN)
        {
            if (session.capability() == ClusterSession.Capability.CLIENT_PLUS_QUERY)
            {
                switch (queryType)
                {
                    case ENDPOINTS:
                        final String endpointsDetail =
                            thisMember.id() + "," +
                            thisMember.clientFacingEndpoint() + "," +
                            thisMember.memberFacingEndpoint() + "," +
                            thisMember.logEndpoint() + "," +
                            thisMember.archiveEndpoint();

                        session.lastActivity(cachedEpochClock.time(), correlationId);
                        session.encodedAdminResponse(endpointsDetail.getBytes(US_ASCII));

                        if (egressPublisher.sendAdminResponse(session, session.encodedAdminResponse()))
                        {
                            session.encodedAdminResponse(null);
                        }
                        break;

                    case RECOVERY_PLAN:
                        session.lastActivity(cachedEpochClock.time(), correlationId);
                        session.encodedAdminResponse(recoveryPlan.encode());

                        if (egressPublisher.sendAdminResponse(session, session.encodedAdminResponse()))
                        {
                            session.encodedAdminResponse(null);
                        }
                        break;
                }
            }
            else
            {
                session.lastActivity(cachedEpochClock.time(), correlationId);
                egressPublisher.sendEvent(session, EventCode.ERROR, "Principal does not have QUERY capability");
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

    void logRecordingPositionCounter(final ReadableCounter logRecordingPosition)
    {
        this.logRecordingPosition = logRecordingPosition;
    }

    void commitPositionCounter(final Counter commitPosition)
    {
        this.commitPosition = commitPosition;
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

    void onReplayTimerEvent(@SuppressWarnings("unused") final long correlationId, final long timestamp)
    {
        cachedEpochClock.update(timestamp);
    }

    void onReplaySessionOpen(
        final long termPosition,
        final long correlationId,
        final long clusterSessionId,
        final long timestamp,
        final int responseStreamId,
        final String responseChannel)
    {
        cachedEpochClock.update(timestamp);

        final ClusterSession session = new ClusterSession(clusterSessionId, responseStreamId, responseChannel);
        session.open(termPosition);
        session.lastActivity(timestamp, correlationId);

        sessionByIdMap.put(clusterSessionId, session);
        if (clusterSessionId >= nextSessionId)
        {
            nextSessionId = clusterSessionId + 1;
        }
    }

    void onLoadSession(
        final long termPosition,
        final long correlationId,
        final long clusterSessionId,
        final long timestamp,
        final CloseReason closeReason,
        final int responseStreamId,
        final String responseChannel)
    {
        final ClusterSession session = new ClusterSession(clusterSessionId, responseStreamId, responseChannel);
        session.closeReason(closeReason);
        session.open(termPosition);
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
        final long correlationId,
        final long clusterSessionId,
        final long timestamp,
        final CloseReason closeReason)
    {
        cachedEpochClock.update(timestamp);
        sessionByIdMap.remove(clusterSessionId).close();
    }

    @SuppressWarnings("unused")
    void onReplayClusterAction(
        final long logPosition, final long leadershipTermId, final long timestamp, final ClusterAction action)
    {
        cachedEpochClock.update(timestamp);
        final long termPosition = logPosition - baseLogPosition;

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
                    serviceAckCount = 0;
                    state(ConsensusModule.State.SNAPSHOT);
                }
                break;

            case SHUTDOWN:
                if (!isRecovering)
                {
                    serviceAckCount = 0;
                    state(ConsensusModule.State.SHUTDOWN);
                }
                break;

            case ABORT:
                if (!isRecovering)
                {
                    serviceAckCount = 0;
                    state(ConsensusModule.State.ABORT);
                }
                break;
        }
    }

    void onReloadState(final long nextSessionId)
    {
        this.nextSessionId = nextSessionId;
    }

    void onRequestVote(
        final long candidateTermId,
        final long lastBaseLogPosition,
        final long lastTermPosition,
        final int candidateId)
    {
        if (Cluster.Role.FOLLOWER == role &&
            candidateTermId == leadershipTermId &&
            lastBaseLogPosition == recoveryPlan.lastLogPosition)
        {
            final boolean vote = lastTermPosition >= recoveryPlan.lastTermPositionAppended;
            sendVote(candidateTermId, lastBaseLogPosition, lastTermPosition, candidateId, vote);

            if (!vote)
            {
                // TODO: become candidate in new election
                throw new IllegalStateException("Invalid member for cluster leader: " + candidateId);
            }
            else
            {
                votedForMemberId = candidateId;
                if (recoveryPlan.lastTermPositionAppended < lastTermPosition)
                {
                    // TODO: need to catch up with leader
                }
            }
        }
        else
        {
            sendVote(candidateTermId, lastBaseLogPosition, lastTermPosition, candidateId, false);
        }
    }

    void onVote(
        final long candidateTermId,
        final long lastBaseLogPosition,
        final long lastTermPosition,
        final int candidateMemberId,
        final int followerMemberId,
        final boolean vote)
    {
        if (Cluster.Role.CANDIDATE == role &&
            candidateTermId == leadershipTermId &&
            lastBaseLogPosition == recoveryPlan.lastLogPosition &&
            lastTermPosition == recoveryPlan.lastTermPositionAppended &&
            candidateMemberId == memberId)
        {
            if (vote)
            {
                clusterMembers[followerMemberId].votedForId(candidateMemberId);
            }
            else
            {
                // TODO: Have to deal with failed candidacy
                throw new IllegalStateException("Rejected vote from: " + followerMemberId);
            }
        }
    }

    void onAppendedPosition(final long termPosition, final long leadershipTermId, final int followerMemberId)
    {
        if (leadershipTermId == this.leadershipTermId)
        {
            clusterMembers[followerMemberId].termPosition(termPosition);
        }
    }

    void onCommitPosition(
        final long termPosition, final long leadershipTermId, final int leaderMemberId, final int logSessionId)
    {
        if (leadershipTermId == this.leadershipTermId)
        {
            if (leaderMemberId != votedForMemberId)
            {
                throw new IllegalStateException("Commit position not for current leader: expected=" +
                    this.votedForMemberId + " received=" + leaderMemberId);
            }

            if (0 == termPosition)
            {
                this.logSessionId = logSessionId;
            }

            timeOfLastLogUpdateMs = cachedEpochClock.time();
            followerCommitPosition = termPosition;
        }
    }

    private int slowTickCycle(final long nowMs)
    {
        int workCount = 0;

        markFile.updateActivityTimestamp(nowMs);
        workCount += aeronClientInvoker.invoke();
        workCount += serviceControlAdapter.poll();

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
                serviceAckCount = 0;
                if (ConsensusModule.State.ACTIVE == state && appendAction(ClusterAction.SNAPSHOT, nowMs))
                {
                    state(ConsensusModule.State.SNAPSHOT);
                }
                break;

            case SHUTDOWN:
                serviceAckCount = 0;
                if (ConsensusModule.State.ACTIVE == state && appendAction(ClusterAction.SHUTDOWN, nowMs))
                {
                    state(ConsensusModule.State.SHUTDOWN);
                }
                break;

            case ABORT:
                serviceAckCount = 0;
                if (ConsensusModule.State.ACTIVE == state && appendAction(ClusterAction.ABORT, nowMs))
                {
                    state(ConsensusModule.State.ABORT);
                }
                break;

            default:
                return 0;
        }

        return 1;
    }

    private void sendVote(
        final long candidateTermId,
        final long lastBaseLogPosition,
        final long lastTermPosition,
        final int candidateId,
        final boolean vote)
    {
        idleStrategy.reset();

        while (!memberStatusPublisher.vote(
            clusterMembers[candidateId].publication(),
            candidateTermId,
            lastBaseLogPosition,
            lastTermPosition,
            candidateId,
            memberId,
            vote))
        {
            idle();
        }
    }

    private boolean appendAction(final ClusterAction action, final long nowMs)
    {
        final long position = baseLogPosition +
            logPublisher.position() +
            MessageHeaderEncoder.ENCODED_LENGTH +
            ClusterActionRequestEncoder.BLOCK_LENGTH;

        return logPublisher.appendClusterAction(action, leadershipTermId, position, nowMs);
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
            final ClusterSession.State state = session.state();

            if (nowMs > (session.timeOfLastActivityMs() + sessionTimeoutMs))
            {
                switch (state)
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
            else if (state == CONNECTED)
            {
                appendConnectedSession(session, nowMs);
                workCount += 1;
            }
            else if (state == OPEN && session.encodedAdminResponse() != null)
            {
                if (egressPublisher.sendAdminResponse(session, session.encodedAdminResponse()))
                {
                    session.encodedAdminResponse(null);
                }
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

    private void electLeader()
    {
        if (ctx.appointedLeaderId() == memberId)
        {
            role(Cluster.Role.CANDIDATE);
            ClusterMember.becomeCandidate(clusterMembers, memberId);
            votedForMemberId = memberId;

            requestVotes(clusterMembers, recoveryPlan.lastLogPosition, recoveryPlan.lastTermPositionAppended);

            do
            {
                idle(memberStatusAdapter.poll());
            }
            while (ClusterMember.awaitingVotes(clusterMembers));

            leaderMember = thisMember;
        }
        else
        {
            votedForMemberId = NULL_MEMBER_ID;
            while (NULL_MEMBER_ID == votedForMemberId)
            {
                idle(memberStatusAdapter.poll());
            }
        }
    }

    private void requestVotes(
        final ClusterMember[] clusterMembers, final long lastLogPosition, final long lastTermPosition)
    {
        idleStrategy.reset();
        for (final ClusterMember member : clusterMembers)
        {
            if (member != thisMember)
            {
                while (!memberStatusPublisher.requestVote(
                    member.publication(),
                    leadershipTermId,
                    lastLogPosition,
                    lastTermPosition,
                    memberId))
                {
                    idle();
                }
            }
        }
    }

    private void becomeLeader()
    {
        updateMemberDetails(votedForMemberId);
        role(Cluster.Role.LEADER);

        final ChannelUri channelUri = ChannelUri.parse(ctx.logChannel());
        final Publication publication = aeron.addExclusivePublication(ctx.logChannel(), ctx.logStreamId());
        if (!channelUri.containsKey(CommonContext.ENDPOINT_PARAM_NAME))
        {
            final ChannelUriStringBuilder builder = new ChannelUriStringBuilder().media("udp");
            for (final ClusterMember member : clusterMembers)
            {
                if (member != thisMember)
                {
                    publication.addDestination(builder.endpoint(member.logEndpoint()).build());
                }
            }
        }

        logAdapter = null;
        logPublisher.connect(publication);
        logSessionId = publication.sessionId();

        channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(logSessionId));
        final String recordingChannel = channelUri.toString();
        archive.startRecording(recordingChannel, ctx.logStreamId(), SourceLocation.LOCAL);

        createPositionCounters();

        awaitServicesReady(channelUri, true);
        awaitFollowersReady();

        final long nowMs = epochClock.time();
        for (final ClusterSession session : sessionByIdMap.values())
        {
            if (session.state() != CLOSED)
            {
                session.connect(aeron);
                session.timeOfLastActivityMs(nowMs);
            }
        }
    }

    private void becomeFollower()
    {
        leaderMember = clusterMembers[votedForMemberId];

        updateMemberDetails(votedForMemberId);
        role(Cluster.Role.FOLLOWER);

        awaitLogSessionIdFromLeader();

        final ChannelUri channelUri = ChannelUri.parse(ctx.logChannel());
        channelUri.put(CommonContext.ENDPOINT_PARAM_NAME, thisMember.logEndpoint());
        channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(logSessionId));
        final String logChannel = channelUri.toString();

        final int streamId = ctx.logStreamId();
        archive.startRecording(logChannel, streamId, SourceLocation.REMOTE);
        final Image image = awaitImage(logSessionId, aeron.addSubscription(logChannel, streamId));
        logAdapter = new LogAdapter(image, this);

        createPositionCounters();
        awaitServicesReady(channelUri, false);
        notifyLeaderThatFollowerIsReady();
    }

    private void awaitLogSessionIdFromLeader()
    {
        followerCommitPosition = NULL_POSITION;
        while (NULL_POSITION == followerCommitPosition)
        {
            final int fragments = memberStatusAdapter.poll();
            idle(fragments);
        }
    }

    private void notifyLeaderThatFollowerIsReady()
    {
        idleStrategy.reset();
        final Publication publication = leaderMember.publication();

        while (!memberStatusPublisher.appendedPosition(
            publication, 0L, leadershipTermId, memberId))
        {
            idle();
        }

        lastRecordingPosition = 0;
    }

    private void awaitFollowersReady()
    {
        ClusterMember.resetTermPositions(clusterMembers, -1);
        clusterMembers[memberId].termPosition(0);

        do
        {
            final long nowMs = epochClock.time();
            if (nowMs > (timeOfLastLogUpdateMs + heartbeatIntervalMs))
            {
                timeOfLastLogUpdateMs = nowMs;

                for (final ClusterMember member : clusterMembers)
                {
                    if (member != thisMember)
                    {
                        memberStatusPublisher.commitPosition(
                            member.publication(), baseLogPosition, leadershipTermId, memberId, logSessionId);
                    }
                }
            }

            idle(memberStatusAdapter.poll());
        }
        while (!ClusterMember.hasReachedPosition(clusterMembers, 0));
    }

    private void createPositionCounters()
    {
        final CountersReader counters = aeron.countersReader();
        final int recordingCounterId = awaitRecordingCounter(counters, logSessionId);

        logRecordingPosition = new ReadableCounter(counters, recordingCounterId);
        logRecordingId = RecordingPos.getRecordingId(counters, logRecordingPosition.counterId());

        commitPosition = CommitPos.allocate(
            aeron, tempBuffer, logRecordingId, baseLogPosition, leadershipTermId, logSessionId);
    }

    private void awaitServicesReady(final ChannelUri channelUri, final boolean isLeader)
    {
        serviceAckCount = 0;

        final String channel = isLeader ? channelUri.prefix(SPY_QUALIFIER).toString() : channelUri.toString();
        serviceControlPublisher.joinLog(
            leadershipTermId, commitPosition.id(), logSessionId, ctx.logStreamId(), channel);

        awaitServiceAcks();
    }

    private void updateMemberDetails(final int leaderMemberId)
    {
        for (final ClusterMember clusterMember : clusterMembers)
        {
            clusterMember.isLeader(clusterMember.id() == leaderMemberId);
        }

        updateClusterMemberDetails(clusterMembers);
    }

    private void recoverFromSnapshot(final RecordingLog.ReplayStep snapshotStep, final AeronArchive archive)
    {
        final RecordingLog.Entry snapshot = snapshotStep.entry;

        cachedEpochClock.update(snapshot.timestamp);
        baseLogPosition = snapshot.logPosition + snapshot.termPosition;
        leadershipTermId = snapshot.leadershipTermId;

        final long recordingId = snapshot.recordingId;
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
                        throw new IllegalStateException("Snapshot ended unexpectedly");
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

    private void recoverFromLog(final List<RecordingLog.ReplayStep> steps, final AeronArchive archive)
    {
        final int streamId = ctx.replayStreamId();
        final ChannelUri channelUri = ChannelUri.parse(ctx.replayChannel());

        for (int i = 0, size = steps.size(); i < size; i++)
        {
            final RecordingLog.ReplayStep step = steps.get(i);
            final RecordingLog.Entry entry = step.entry;
            final long startPosition = step.recordingStartPosition;
            final long stopPosition = step.recordingStopPosition;
            final long length = stopPosition - startPosition;
            final long logPosition = entry.logPosition;

            baseLogPosition = logPosition;
            leadershipTermId = entry.leadershipTermId;

            channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(i));
            final String channel = channelUri.toString();
            final long recordingId = entry.recordingId;

            try (Counter counter = CommitPos.allocate(
                aeron, tempBuffer, recordingId, logPosition, leadershipTermId, i);
                Subscription subscription = aeron.addSubscription(channel, streamId))
            {
                serviceAckCount = 0;
                serviceControlPublisher.joinLog(leadershipTermId, counter.id(), i, streamId, channel);
                awaitServiceAcks();

                final int sessionId = (int)archive.startReplay(recordingId, startPosition, length, channel, streamId);

                if (i != sessionId)
                {
                    throw new IllegalStateException("Session id not for iteration: " + sessionId);
                }

                final Image image = awaitImage(sessionId, subscription);

                serviceAckCount = 0;
                replayTerm(image, stopPosition, counter);
                awaitServiceAcks();

                final long termPosition = image.position();
                if (step.entry.termPosition < termPosition)
                {
                    recordingLog.commitLeadershipTermPosition(leadershipTermId, termPosition);
                }

                baseLogPosition = entry.logPosition + termPosition;
            }
        }
    }

    private Counter addRecoveryStateCounter(final RecordingLog.RecoveryPlan plan)
    {
        final int termCount = plan.termSteps.size();
        final RecordingLog.ReplayStep snapshotStep = plan.snapshotStep;

        if (null != snapshotStep)
        {
            final RecordingLog.Entry snapshot = snapshotStep.entry;

            return RecoveryState.allocate(
                aeron, tempBuffer, snapshot.leadershipTermId, snapshot.termPosition, snapshot.timestamp, termCount);
        }

        return RecoveryState.allocate(aeron, tempBuffer, leadershipTermId, NULL_POSITION, 0, termCount);
    }

    private void awaitServiceAcks()
    {
        while (true)
        {
            final int fragmentsRead = serviceControlAdapter.poll();
            if (serviceAckCount >= ctx.serviceCount())
            {
                break;
            }

            idle(fragmentsRead);
        }
    }

    private void validateServiceAck(
        final long logPosition, final long leadershipTermId, final int serviceId, final ClusterAction action)
    {
        final long currentLogPosition = baseLogPosition + currentTermPosition();
        if (logPosition != currentLogPosition || leadershipTermId != this.leadershipTermId)
        {
            throw new IllegalStateException("invalid log state:" +
                " serviceId=" + serviceId +
                ", logPosition=" + logPosition + " current is " + currentLogPosition +
                ", leadershipTermId=" + leadershipTermId + " current is " + this.leadershipTermId);
        }

        if (!state.isValid(action))
        {
            throw new IllegalStateException("invalid service ACK given state " + state + ", action " + action);
        }
    }

    private long currentTermPosition()
    {
        return null != logAdapter ? logAdapter.position() : logPublisher.position();
    }

    private void updateClusterMemberDetails(final ClusterMember[] members)
    {
        int leaderIndex = 0;
        for (int i = 0, length = members.length; i < length; i++)
        {
            if (members[i].isLeader())
            {
                leaderIndex = i;
                break;
            }
        }

        final StringBuilder builder = new StringBuilder(100);
        builder.append(members[leaderIndex].clientFacingEndpoint());

        for (int i = 0, length = members.length; i < length; i++)
        {
            if (i != leaderIndex)
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
            thisMember.termPosition(logRecordingPosition.get());

            final long position = ClusterMember.quorumPosition(clusterMembers, rankedPositions);
            if (position > commitPosition.getWeak() || nowMs >= (timeOfLastLogUpdateMs + heartbeatIntervalMs))
            {
                for (final ClusterMember member : clusterMembers)
                {
                    if (member != thisMember)
                    {
                        memberStatusPublisher.commitPosition(
                            member.publication(), position, leadershipTermId, memberId, logSessionId);
                    }
                }

                commitPosition.setOrdered(position);
                timeOfLastLogUpdateMs = nowMs;

                workCount = 1;
            }
        }
        else if (Cluster.Role.FOLLOWER == role)
        {
            final long recordingPosition = logRecordingPosition.get();
            if (recordingPosition != lastRecordingPosition)
            {
                final Publication publication = leaderMember.publication();
                if (memberStatusPublisher.appendedPosition(
                    publication, recordingPosition, leadershipTermId, memberId))
                {
                    lastRecordingPosition = recordingPosition;
                }

                workCount = 1;
            }

            commitPosition.proposeMaxOrdered(logAdapter.position());

            if (nowMs >= (timeOfLastLogUpdateMs + heartbeatTimeoutMs))
            {
                throw new AgentTerminationException("No heartbeat detected from cluster leader");
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
            throw new RuntimeException("Unexpected interrupt");
        }
    }

    private void takeSnapshot(final long timestampMs, final long termPosition)
    {
        final String channel = ctx.snapshotChannel();
        final int streamId = ctx.snapshotStreamId();

        try (Publication publication = archive.addRecordedExclusivePublication(channel, streamId))
        {
            try
            {
                final CountersReader counters = aeron.countersReader();
                final int counterId = awaitRecordingCounter(counters, publication.sessionId());
                final long recordingId = RecordingPos.getRecordingId(counters, counterId);

                snapshotState(publication, baseLogPosition + termPosition, leadershipTermId);
                awaitRecordingComplete(recordingId, publication.position(), counters, counterId);
                recordingLog.appendSnapshot(recordingId, leadershipTermId, baseLogPosition, termPosition, timestampMs);
            }
            finally
            {
                archive.stopRecording(publication);
            }
            ctx.snapshotCounter().incrementOrdered();
        }
    }

    private void awaitRecordingComplete(
        final long recordingId, final long completePosition, final CountersReader counters, final int counterId)
    {
        idleStrategy.reset();
        do
        {
            idle();

            if (!RecordingPos.isActive(counters, counterId, recordingId))
            {
                throw new IllegalStateException("Recording has stopped unexpectedly: " + recordingId);
            }
        }
        while (counters.getCounterValue(counterId) < completePosition);
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

    private void replayTerm(final Image image, final long finalTermPosition, final Counter replayPosition)
    {
        logAdapter = new LogAdapter(image, this);

        while (true)
        {
            int workCount = logAdapter.poll(finalTermPosition);
            if (workCount == 0)
            {
                if (image.isClosed())
                {
                    if (!image.isEndOfStream())
                    {
                        throw new IllegalStateException("Unexpected close of image when replaying");
                    }

                    break;
                }
            }

            replayPosition.setOrdered(image.position());

            workCount += serviceControlAdapter.poll();
            workCount += timerService.poll(cachedEpochClock.time());

            idle(workCount);
        }
    }
}
