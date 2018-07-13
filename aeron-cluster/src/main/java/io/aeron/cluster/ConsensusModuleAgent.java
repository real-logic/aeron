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
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.service.CommitPos;
import io.aeron.cluster.service.RecoveryState;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.security.Authenticator;
import io.aeron.status.ReadableCounter;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.CountersReader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.ChannelUri.SPY_QUALIFIER;
import static io.aeron.CommonContext.*;
import static io.aeron.archive.client.AeronArchive.NULL_LENGTH;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.cluster.ClusterSession.State.*;
import static io.aeron.cluster.ConsensusModule.Configuration.*;
import static io.aeron.cluster.ServiceAck.hasReachedPosition;
import static io.aeron.cluster.ServiceAck.newArray;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static java.lang.Long.MAX_VALUE;

class ConsensusModuleAgent implements Agent, MemberStatusListener
{
    private final int memberId;
    private final long sessionTimeoutMs;
    private final long leaderHeartbeatIntervalMs;
    private final long leaderHeartbeatTimeoutMs;
    private final long serviceHeartbeatTimeoutMs;
    private long nextSessionId = 1;
    private long leadershipTermId = NULL_VALUE;
    private long expectedAckPosition = 0;
    private long serviceAckId = 0;
    private long lastAppendedPosition = 0;
    private long followerCommitPosition = 0;
    private long timeOfLastLogUpdateMs = 0;
    private long cachedTimeMs;
    private long clusterTimeMs;
    private long lastRecordingId = RecordingPos.NULL_RECORDING_ID;
    private int logPublicationInitialTermId = NULL_VALUE;
    private int logPublicationTermBufferLength = NULL_VALUE;
    private int logPublicationMtuLength = NULL_VALUE;
    private ReadableCounter appendedPosition;
    private Counter commitPosition;
    private ConsensusModule.State state = ConsensusModule.State.INIT;
    private Cluster.Role role;
    private ClusterMember[] clusterMembers;
    private ClusterMember leaderMember;
    private final ClusterMember thisMember;
    private long[] rankedPositions;
    private final ServiceAck[] serviceAcks;
    private final Counter clusterRoleCounter;
    private final ClusterMarkFile markFile;
    private final AgentInvoker aeronClientInvoker;
    private final EpochClock epochClock;
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
    private final LongHashSet missedTimersSet = new LongHashSet();
    private final Authenticator authenticator;
    private final ClusterSessionProxy sessionProxy;
    private final Aeron aeron;
    private AeronArchive archive;
    private final ConsensusModule.Context ctx;
    private final MutableDirectBuffer tempBuffer;
    private final Counter[] serviceHeartbeats;
    private final IdleStrategy idleStrategy;
    private final RecordingLog recordingLog;
    private RecordingLog.RecoveryPlan recoveryPlan;
    private Election election;
    private String logRecordingChannel;
    private String liveLogDestination;
    private String clientFacingEndpoints;

    ConsensusModuleAgent(final ConsensusModule.Context ctx)
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
        this.sessionProxy = new ClusterSessionProxy(egressPublisher);
        this.memberId = ctx.clusterMemberId();
        this.clusterRoleCounter = ctx.clusterNodeCounter();
        this.markFile = ctx.clusterMarkFile();
        this.recordingLog = ctx.recordingLog();
        this.tempBuffer = ctx.tempBuffer();
        this.serviceHeartbeats = ctx.serviceHeartbeatCounters();
        this.serviceAcks = newArray(ctx.serviceCount());

        aeronClientInvoker = aeron.conductorAgentInvoker();
        aeronClientInvoker.invoke();

        rankedPositions = new long[ClusterMember.quorumThreshold(clusterMembers.length)];
        role(Cluster.Role.FOLLOWER);

        thisMember = clusterMembers[memberId];
        leaderMember = thisMember;
        final ChannelUri memberStatusUri = ChannelUri.parse(ctx.memberStatusChannel());
        memberStatusUri.put(ENDPOINT_PARAM_NAME, thisMember.memberFacingEndpoint());

        final int statusStreamId = ctx.memberStatusStreamId();
        memberStatusAdapter = new MemberStatusAdapter(
            aeron.addSubscription(memberStatusUri.toString(), statusStreamId), this);

        ClusterMember.addMemberStatusPublications(clusterMembers, thisMember, memberStatusUri, statusStreamId, aeron);

        ingressAdapter = new IngressAdapter(this, ctx.invalidRequestCounter());

        final ChannelUri archiveUri = ChannelUri.parse(ctx.archiveContext().controlRequestChannel());
        ClusterMember.checkArchiveEndpoint(thisMember, archiveUri);
        archiveUri.put(ENDPOINT_PARAM_NAME, thisMember.archiveEndpoint());
        ctx.archiveContext().controlRequestChannel(archiveUri.toString());

        consensusModuleAdapter = new ConsensusModuleAdapter(
            aeron.addSubscription(ctx.serviceControlChannel(), ctx.consensusModuleStreamId()), this);
        serviceProxy = new ServiceProxy(
            aeron.addPublication(ctx.serviceControlChannel(), ctx.serviceStreamId()));

        authenticator = ctx.authenticatorSupplier().get();
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

        try (Counter ignore = addRecoveryStateCounter(recoveryPlan))
        {
            if (!recoveryPlan.snapshots.isEmpty())
            {
                recoverFromSnapshot(recoveryPlan.snapshots.get(0), archive);
            }

            awaitServiceAcks(expectedAckPosition);
        }

        if (ConsensusModule.State.SUSPENDED != state)
        {
            state(ConsensusModule.State.ACTIVE);
        }

        timeOfLastLogUpdateMs = cachedTimeMs = epochClock.time();
        leadershipTermId = recoveryPlan.lastLeadershipTermId;

        election = new Election(
            true,
            leadershipTermId,
            recoveryPlan.appendedLogPosition,
            clusterMembers,
            thisMember,
            memberStatusAdapter,
            memberStatusPublisher,
            ctx,
            this);
    }

    public int doWork()
    {
        int workCount = 0;

        boolean isSlowTickCycle = false;
        final long nowMs = epochClock.time();
        if (cachedTimeMs != nowMs)
        {
            cachedTimeMs = nowMs;
            if (Cluster.Role.LEADER == role)
            {
                clusterTimeMs = nowMs;
            }

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
                workCount += timerService.poll(nowMs);
            }
            else if (Cluster.Role.FOLLOWER == role &&
                (ConsensusModule.State.ACTIVE == state || ConsensusModule.State.SUSPENDED == state))
            {
                final int count = logAdapter.poll(followerCommitPosition);
                if (0 == count && logAdapter.isImageClosed())
                {
                    enterElection(nowMs);

                    return 1;
                }

                workCount += count;
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
        return "consensus-module";
    }

    public void onSessionConnect(
        final long correlationId,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedCredentials)
    {
        final long sessionId = nextSessionId++;
        final ClusterSession session = new ClusterSession(sessionId, responseStreamId, responseChannel);
        session.connect(aeron);
        session.lastActivity(clusterTimeMs, correlationId);

        if (pendingSessions.size() + sessionByIdMap.size() < ctx.maxConcurrentSessions())
        {
            authenticator.onConnectRequest(sessionId, encodedCredentials, clusterTimeMs);
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
            session.close(CloseReason.CLIENT_ACTION);

            if (logPublisher.appendSessionClose(session, clusterTimeMs))
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

        if (session.state() == OPEN && logPublisher.appendMessage(buffer, offset, length, clusterTimeMs))
        {
            session.lastActivity(clusterTimeMs, correlationId);
            return ControlledFragmentHandler.Action.CONTINUE;
        }

        return ControlledFragmentHandler.Action.ABORT;
    }

    public void onSessionKeepAlive(final long clusterSessionId)
    {
        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null != session && session.state() == OPEN)
        {
            session.timeOfLastActivityMs(clusterTimeMs);
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
                session.lastActivity(clusterTimeMs, correlationId);
                authenticator.onChallengeResponse(clusterSessionId, encodedCredentials, clusterTimeMs);
                break;
            }
        }
    }

    public boolean onTimerEvent(final long correlationId, final long nowMs)
    {
        return Cluster.Role.LEADER != role || logPublisher.appendTimer(correlationId, nowMs);
    }

    public void onCanvassPosition(final long logLeadershipTermId, final long logPosition, final int followerMemberId)
    {
        if (null != election)
        {
            election.onCanvassPosition(logLeadershipTermId, logPosition, followerMemberId);
        }
        else if (Cluster.Role.LEADER == role)
        {
            memberStatusPublisher.newLeadershipTerm(
                clusterMembers[followerMemberId].publication(),
                this.leadershipTermId,
                recordingLog.getTermEntry(this.leadershipTermId).termBaseLogPosition,
                this.leadershipTermId,
                thisMember.id(),
                logPublisher.sessionId());
        }
    }

    public void onRequestVote(
        final long logLeadershipTermId, final long logPosition, final long candidateTermId, final int candidateId)
    {
        if (null != election)
        {
            election.onRequestVote(logLeadershipTermId, logPosition, candidateTermId, candidateId);
        }
        else if (candidateTermId > this.leadershipTermId)
        {
            enterElection(cachedTimeMs);
            election.onRequestVote(logLeadershipTermId, logPosition, candidateTermId, candidateId);
        }
    }

    public void onVote(
        final long candidateTermId,
        final long logLeadershipTermId,
        final long logPosition,
        final int candidateMemberId,
        final int followerMemberId,
        final boolean vote)
    {
        if (null != election)
        {
            election.onVote(
                candidateTermId, logLeadershipTermId, logPosition, candidateMemberId, followerMemberId, vote);
        }
    }

    public void onNewLeadershipTerm(
        final long logLeadershipTermId,
        final long logPosition,
        final long leadershipTermId,
        final int leaderMemberId,
        final int logSessionId)
    {
        if (null != election)
        {
            election.onNewLeadershipTerm(
                logLeadershipTermId, logPosition, leadershipTermId, leaderMemberId, logSessionId);
        }
        else if (leadershipTermId > this.leadershipTermId)
        {
            enterElection(cachedTimeMs);
        }
    }

    public void onAppendedPosition(final long leadershipTermId, final long logPosition, final int followerMemberId)
    {
        if (null != election)
        {
            election.onAppendedPosition(leadershipTermId, logPosition, followerMemberId);
        }
        else if (Cluster.Role.LEADER == role && leadershipTermId == this.leadershipTermId)
        {
            clusterMembers[followerMemberId].logPosition(logPosition);
        }
    }

    public void onCommitPosition(final long leadershipTermId, final long logPosition, final int leaderMemberId)
    {
        if (null != election)
        {
            election.onCommitPosition(leadershipTermId, logPosition, leaderMemberId);
        }
        else if (Cluster.Role.FOLLOWER == role && leadershipTermId == this.leadershipTermId)
        {
            timeOfLastLogUpdateMs = cachedTimeMs;
            followerCommitPosition = logPosition;
        }
        else if (leadershipTermId > this.leadershipTermId)
        {
            enterElection(cachedTimeMs);
        }
    }

    public void onCatchupPosition(final long leadershipTermId, final long logPosition, final int followerMemberId)
    {
        if (Cluster.Role.LEADER == role && leadershipTermId == this.leadershipTermId)
        {
            final String replayChannel = new ChannelUriStringBuilder()
                .media(CommonContext.UDP_MEDIA)
                .endpoint(clusterMembers[followerMemberId].transferEndpoint())
                .isSessionIdTagged(true)
                .sessionId(ConsensusModule.Configuration.LOG_PUBLICATION_SESSION_ID_TAG)
                .build();

            final ClusterMember member = clusterMembers[followerMemberId];

            if (member.catchupReplaySessionId() == Aeron.NULL_VALUE)
            {
                member.catchupReplaySessionId(archive.startReplay(
                    logRecordingId(), logPosition, Long.MAX_VALUE, replayChannel, ctx.logStreamId()));
            }
        }
    }

    public void onStopCatchup(final int replaySessionId, final int followerMemberId)
    {
        final ClusterMember member = clusterMembers[followerMemberId];

        if (member.catchupReplaySessionId() == replaySessionId)
        {
            archive.stopReplay(member.catchupReplaySessionId());
            member.catchupReplaySessionId(Aeron.NULL_VALUE);
        }
    }

    public void onRecoveryPlanQuery(final long correlationId, final int requestMemberId, final int leaderMemberId)
    {
        if (leaderMemberId == memberId)
        {
            memberStatusPublisher.recoveryPlan(
                clusterMembers[requestMemberId].publication(),
                correlationId,
                requestMemberId,
                leaderMemberId,
                recoveryPlan);
        }
    }

    public void onRecoveryPlan(final RecoveryPlanDecoder recoveryPlanDecoder)
    {
    }

    public void onRecordingLogQuery(
        final long correlationId,
        final int requestMemberId,
        final int leaderMemberId,
        final long fromLeadershipTermId,
        final int count,
        final boolean includeSnapshots)
    {
        if (leaderMemberId == memberId)
        {
            memberStatusPublisher.recordingLog(
                clusterMembers[requestMemberId].publication(),
                correlationId,
                requestMemberId,
                leaderMemberId,
                recordingLog,
                fromLeadershipTermId,
                count,
                includeSnapshots);
        }
    }

    public void onRecordingLog(final RecordingLogDecoder recordingLogDecoder)
    {
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

    long prepareForElection(final long logPosition)
    {
        final RecordingExtent recordingExtent = new RecordingExtent();

        long recordingId = RecordingPos.getRecordingId(aeron.countersReader(), appendedPosition.counterId());
        if (RecordingPos.NULL_RECORDING_ID == recordingId)
        {
            recordingId = recordingLog.getTermEntry(leadershipTermId).recordingId;
        }

        stopLogRecording();

        idleStrategy.reset();
        while (true)
        {
            if (0 == archive.listRecording(recordingId, recordingExtent))
            {
                throw new ClusterException("recording not found id=" + recordingId);
            }

            if (AeronArchive.NULL_POSITION != recordingExtent.stopPosition)
            {
                break;
            }

            idle();
        }

        if (recordingExtent.stopPosition > logPosition)
        {
            archive.truncateRecording(recordingId, logPosition);
        }

        lastAppendedPosition = recordingExtent.stopPosition;
        followerCommitPosition = recordingExtent.stopPosition;

        lastRecordingId = recordingId;
        logPublicationInitialTermId = recordingExtent.initialTermId;
        logPublicationTermBufferLength = recordingExtent.termBufferLength;
        logPublicationMtuLength = recordingExtent.mtuLength;

        commitPosition.setOrdered(recordingExtent.stopPosition);
        clearSessionsAfter(recordingExtent.stopPosition);

        return recordingExtent.stopPosition;
    }

    void stopLogRecording()
    {
        if (null != logRecordingChannel)
        {
            archive.stopRecording(logRecordingChannel, ctx.logStreamId());
            logRecordingChannel = null;
        }

        if (null != logAdapter && null != liveLogDestination)
        {
            logAdapter.removeDestination(liveLogDestination);
            liveLogDestination = null;
        }
    }

    void appendedPositionCounter(final ReadableCounter appendedPositionCounter)
    {
        this.appendedPosition = appendedPositionCounter;
    }

    void commitPositionCounter(final Counter commitPositionCounter)
    {
        this.commitPosition = commitPositionCounter;
    }

    void clearSessionsAfter(final long logPosition)
    {
        for (final Iterator<ClusterSession> i = sessionByIdMap.values().iterator(); i.hasNext(); )
        {
            final ClusterSession session = i.next();
            if (session.openedLogPosition() >= logPosition)
            {
                i.remove();
                session.close();
            }
        }

        for (final ClusterSession session : pendingSessions)
        {
            session.close();
        }

        pendingSessions.clear();
    }

    void onServiceCloseSession(final long clusterSessionId)
    {
        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null != session)
        {
            session.close(CloseReason.SERVICE_ACTION);

            if (Cluster.Role.LEADER == role && logPublisher.appendSessionClose(session, clusterTimeMs))
            {
                sessionByIdMap.remove(clusterSessionId);
            }
        }
    }

    void onScheduleTimer(final long correlationId, final long deadlineMs)
    {
        timerService.scheduleTimer(correlationId, deadlineMs);
    }

    void onCancelTimer(final long correlationId)
    {
        timerService.cancelTimer(correlationId);
    }

    void onServiceAck(final long logPosition, final long ackId, final long relevantId, final int serviceId)
    {
        validateServiceAck(logPosition, ackId, serviceId);
        serviceAcks[serviceId].logPosition(logPosition).ackId(ackId).relevantId(relevantId);

        if (hasReachedPosition(logPosition, serviceAckId, serviceAcks))
        {
            switch (state)
            {
                case SNAPSHOT:
                    ++serviceAckId;
                    takeSnapshot(clusterTimeMs, logPosition);
                    state(ConsensusModule.State.ACTIVE);
                    ClusterControl.ToggleState.reset(controlToggle);
                    for (final ClusterSession session : sessionByIdMap.values())
                    {
                        session.timeOfLastActivityMs(clusterTimeMs);
                    }
                    break;

                case SHUTDOWN:
                    takeSnapshot(clusterTimeMs, logPosition);
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
        clusterTimeMs = timestamp;
        sessionByIdMap.get(clusterSessionId).lastActivity(timestamp, correlationId);
    }

    void onReplayTimerEvent(final long correlationId, final long timestamp)
    {
        clusterTimeMs = timestamp;

        if (!timerService.cancelTimer(correlationId))
        {
            missedTimersSet.add(correlationId);
        }
    }

    void onReplaySessionOpen(
        final long logPosition,
        final long correlationId,
        final long clusterSessionId,
        final long timestamp,
        final int responseStreamId,
        final String responseChannel)
    {
        clusterTimeMs = timestamp;

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
        sessionByIdMap.put(clusterSessionId, new ClusterSession(
            clusterSessionId, responseStreamId, responseChannel, logPosition, timestamp, correlationId, closeReason));

        if (clusterSessionId >= nextSessionId)
        {
            nextSessionId = clusterSessionId + 1;
        }
    }

    @SuppressWarnings("unused")
    void onReplaySessionClose(
        final long correlationId, final long clusterSessionId, final long timestamp, final CloseReason closeReason)
    {
        clusterTimeMs = timestamp;
        sessionByIdMap.remove(clusterSessionId).close();
    }

    @SuppressWarnings("unused")
    void onReplayClusterAction(
        final long leadershipTermId, final long logPosition, final long timestamp, final ClusterAction action)
    {
        clusterTimeMs = timestamp;

        switch (action)
        {
            case SUSPEND:
                state(ConsensusModule.State.SUSPENDED);
                break;

            case RESUME:
                state(ConsensusModule.State.ACTIVE);
                break;

            case SNAPSHOT:
                replayClusterAction(leadershipTermId, logPosition, ConsensusModule.State.SNAPSHOT);
                break;

            case SHUTDOWN:
                replayClusterAction(leadershipTermId, logPosition, ConsensusModule.State.SHUTDOWN);
                break;

            case ABORT:
                replayClusterAction(leadershipTermId, logPosition, ConsensusModule.State.ABORT);
                break;
        }
    }

    @SuppressWarnings("unused")
    void onReplayNewLeadershipTermEvent(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final int leaderMemberId,
        final int logSessionId)
    {
        clusterTimeMs = timestamp;
        this.leadershipTermId = leadershipTermId;

        if (null != election && null != appendedPosition)
        {
            final long recordingId = RecordingPos.getRecordingId(aeron.countersReader(), appendedPosition.counterId());
            election.onReplayNewLeadershipTermEvent(recordingId, leadershipTermId, logPosition, cachedTimeMs);
        }
    }

    void onReloadState(final long nextSessionId)
    {
        this.nextSessionId = nextSessionId;
    }

    int createLogPublicationSessionId()
    {
        closeExistingLog();
        updateMemberDetails();

        final ChannelUri channelUri = ChannelUri.parse(ctx.logChannel());
        final Publication publication = createLogPublication(channelUri, recoveryPlan, election.logPosition());

        logPublisher.connect(publication);

        return publication.sessionId();
    }

    void becomeLeader(final long leadershipTermId, final int logSessionId)
    {
        this.leadershipTermId = leadershipTermId;

        final ChannelUri channelUri = ChannelUri.parse(ctx.logChannel());
        channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(logSessionId));
        startLogRecording(channelUri.toString(), SourceLocation.LOCAL);
        createAppendPosition(logSessionId);
        commitPosition = CommitPos.allocate(aeron, tempBuffer, leadershipTermId, election.logPosition(), MAX_VALUE);
        awaitServicesReady(channelUri, logSessionId, election.logPosition());

        final ChannelUri ingressUri = ChannelUri.parse(ctx.ingressChannel());
        if (!ingressUri.containsKey(ENDPOINT_PARAM_NAME))
        {
            ingressUri.put(ENDPOINT_PARAM_NAME, thisMember.clientFacingEndpoint());
        }

        ingressAdapter.subscription(aeron.addSubscription(
            ingressUri.toString(), ctx.ingressStreamId(), null, this::onUnavailableIngressImage));

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
                session.hasNewLeaderEventPending(true);
            }
        }
    }

    void updateMemberDetails()
    {
        leaderMember = election.leader();
        followerCommitPosition = election.logPosition();
        sessionProxy.leaderMemberId(leaderMember.id());

        for (final ClusterMember clusterMember : clusterMembers)
        {
            clusterMember.isLeader(clusterMember.id() == leaderMember.id());
        }

        updateClientFacingEndpoints(clusterMembers);
    }

    void liveLogDestination(final String liveLogDestination)
    {
        this.liveLogDestination = liveLogDestination;
    }

    Subscription createAndRecordLogSubscriptionAsFollower(final String logChannel, final long logPosition)
    {
        closeExistingLog();
        final Subscription subscription = aeron.addSubscription(logChannel, ctx.logStreamId());
        startLogRecording(logChannel, SourceLocation.REMOTE);
        commitPosition = CommitPos.allocate(aeron, tempBuffer, leadershipTermId, logPosition, MAX_VALUE);

        return subscription;
    }

    boolean pollImageAndLogAdapter(final Subscription subscription, final int logSessionId)
    {
        boolean result = false;

        if (null == logAdapter)
        {
            final Image image = subscription.imageBySessionId(logSessionId);
            if (null != image)
            {
                logAdapter = new LogAdapter(image, this);
                lastAppendedPosition = 0;
                createAppendPosition(logSessionId);

                result = true;
            }
        }
        else
        {
            result = true;
        }

        return result;
    }

    void awaitImageAndCreateFollowerLogAdapter(final Subscription subscription, final int logSessionId)
    {
        leadershipTermId = election.leadershipTermId();
        idleStrategy.reset();
        while (!pollImageAndLogAdapter(subscription, logSessionId))
        {
            idle();
        }
    }

    void awaitServicesReady(final ChannelUri logChannelUri, final int logSessionId, final long logPosition)
    {
        final String channel = Cluster.Role.LEADER == role && UDP_MEDIA.equals(logChannelUri.media()) ?
            logChannelUri.prefix(SPY_QUALIFIER).toString() : logChannelUri.toString();
        serviceProxy.joinLog(leadershipTermId, commitPosition.id(), logSessionId, ctx.logStreamId(), channel);

        expectedAckPosition = logPosition;
        awaitServiceAcks(logPosition);
    }

    ReplayFromLog replayFromLog(final long electionCommitPosition)
    {
        final RecordingLog.RecoveryPlan plan = recoveryPlan;
        ReplayFromLog replayFromLog = null;

        if (!plan.logs.isEmpty())
        {
            final RecordingLog.Log log = plan.logs.get(0);
            final long startPosition = log.startPosition;
            final long stopPosition = Math.min(log.stopPosition, electionCommitPosition);
            leadershipTermId = log.leadershipTermId;

            if (log.logPosition < 0)
            {
                recordingLog.commitLogPosition(leadershipTermId, stopPosition);
            }

            if (plan.hasReplay())
            {
                replayFromLog = new ReplayFromLog(
                    archive,
                    log.recordingId,
                    startPosition,
                    stopPosition,
                    leadershipTermId,
                    log.sessionId,
                    this,
                    ctx);
            }
        }

        return replayFromLog;
    }

    void awaitServicesReadyForReplay(
        final String channel,
        final int streamId,
        final int logSessionId,
        final int counterId,
        final long leadershipTermId,
        final long startPosition)
    {
        serviceProxy.joinLog(leadershipTermId, counterId, logSessionId, streamId, channel);
        expectedAckPosition = startPosition;
        awaitServiceAcks(startPosition);
    }

    void awaitServicesReplayComplete(final long stopPosition)
    {
        expectedAckPosition = stopPosition;
        awaitServiceAcks(stopPosition);

        while (0 != timerService.poll(clusterTimeMs) ||
            (timerService.currentTickTimeMs() < clusterTimeMs && timerService.timerCount() > 0))
        {
            idle();
        }
    }

    void replayLogPoll(final LogAdapter logAdapter, final long stopPosition, final Counter commitPosition)
    {
        final int workCount = logAdapter.poll(stopPosition);
        if (0 == workCount)
        {
            if (logAdapter.position() == stopPosition)
            {
                while (!missedTimersSet.isEmpty())
                {
                    idle();
                    cancelMissedTimers();
                }
            }

            if (logAdapter.isImageClosed())
            {
                throw new ClusterException("unexpected close of image when replaying log");
            }
        }

        commitPosition.setOrdered(logAdapter.position());

        consensusModuleAdapter.poll();
        cancelMissedTimers();
    }

    long logRecordingId()
    {
        if (!recoveryPlan.logs.isEmpty())
        {
            return recoveryPlan.logs.get(0).recordingId;
        }

        return RecordingPos.getRecordingId(aeron.countersReader(), appendedPosition.counterId());
    }

    long logStopPosition(final long leadershipTermId)
    {
        return recordingLog.getTermEntry(leadershipTermId).logPosition;
    }

    void truncateLogEntry(final long leadershipTermId, final long logPosition)
    {
        final long recordingId = logRecordingId();

        archive.truncateRecording(recordingId, logPosition);
        recordingLog.commitLogPosition(leadershipTermId, logPosition);
    }

    boolean electionComplete(final long nowMs)
    {
        boolean result = false;

        if (Cluster.Role.LEADER == role)
        {
            if (logPublisher.appendNewLeadershipTermEvent(
                leadershipTermId, election.logPosition(), nowMs, memberId, logPublisher.sessionId()))
            {
                timeOfLastLogUpdateMs = cachedTimeMs - leaderHeartbeatIntervalMs;
                election = null;
                result = true;
            }
        }
        else
        {
            timeOfLastLogUpdateMs = cachedTimeMs;
            election = null;
            result = true;
        }

        cancelMissedTimers();
        if (missedTimersSet.capacity() > LongHashSet.DEFAULT_INITIAL_CAPACITY)
        {
            missedTimersSet.compact();
        }

        return result;
    }

    void catchupLogPoll(final Subscription logSubscription, final int logSessionId, final long stopPosition)
    {
        if (pollImageAndLogAdapter(logSubscription, logSessionId))
        {
            expectedAckPosition = stopPosition;

            final Image image = logAdapter.image();
            if (logAdapter.poll(stopPosition) == 0)
            {
                if (image.position() == stopPosition)
                {
                    while (!missedTimersSet.isEmpty())
                    {
                        idle();
                        cancelMissedTimers();
                    }
                }

                if (image.isClosed())
                {
                    throw new ClusterException("unexpected close of image when replaying log");
                }
            }

            final long appendedPosition = this.appendedPosition.get();
            if (appendedPosition != lastAppendedPosition)
            {
                final Publication publication = election.leader().publication();
                if (memberStatusPublisher.appendedPosition(publication, leadershipTermId, appendedPosition, memberId))
                {
                    lastAppendedPosition = appendedPosition;
                }
            }

            commitPosition.setOrdered(image.position());

            consensusModuleAdapter.poll();
            cancelMissedTimers();
        }
    }

    boolean hasAppendReachedPosition(final Subscription logSubscription, final int logSessionId, final long position)
    {
        return pollImageAndLogAdapter(logSubscription, logSessionId) && (appendedPosition.get() >= position);
    }

    boolean hasAppendReachedLivePosition(
        final Subscription logSubscription, final int logSessionId, final long position)
    {
        boolean result = false;

        if (pollImageAndLogAdapter(logSubscription, logSessionId))
        {
            final long appendPosition = appendedPosition.get();
            final int termBufferLength = logAdapter.image().termBufferLength();

            result = (appendPosition >= (position - (termBufferLength / 2)));
        }

        return result;
    }

    void stopAllCatchups()
    {
        for (final ClusterMember member : clusterMembers)
        {
            if (member.catchupReplaySessionId() != Aeron.NULL_VALUE)
            {
                archive.stopReplay(member.catchupReplaySessionId());
                member.catchupReplaySessionId(Aeron.NULL_VALUE);
            }
        }
    }

    private int slowTickCycle(final long nowMs)
    {
        int workCount = 0;

        markFile.updateActivityTimestamp(nowMs);
        checkServiceHeartbeats(nowMs);
        workCount += aeronClientInvoker.invoke();

        if (Cluster.Role.LEADER == role && null == election)
        {
            workCount += checkControlToggle(nowMs);

            if (ConsensusModule.State.ACTIVE == state)
            {
                workCount += processPendingSessions(pendingSessions, nowMs);
                workCount += checkSessions(sessionByIdMap, nowMs);
                workCount += processRejectedSessions(rejectedSessions, nowMs);
            }
        }
        else
        {
            cancelMissedTimers();
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

        return logPublisher.appendClusterAction(leadershipTermId, position, nowMs, action);
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
                    authenticator.onConnectedSession(sessionProxy.session(session), nowMs);
                }
            }

            if (session.state() == CHALLENGED)
            {
                if (session.isResponsePublicationConnected())
                {
                    authenticator.onChallengedSession(sessionProxy.session(session), nowMs);
                }
            }

            if (session.state() == AUTHENTICATED)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                session.timeOfLastActivityMs(nowMs);
                sessionByIdMap.put(session.id(), session);
                appendSessionOpen(session, nowMs);

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
                ctx.timedOutClientCounter().incrementOrdered();
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

            if (egressPublisher.sendEvent(session, leaderMember.id(), eventCode, detail) ||
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
                        if (session.isResponsePublicationConnected())
                        {
                            egressPublisher.sendEvent(session, leaderMember.id(), EventCode.ERROR, SESSION_TIMEOUT_MSG);
                        }

                        session.close(CloseReason.TIMEOUT);
                        if (logPublisher.appendSessionClose(session, nowMs))
                        {
                            i.remove();
                            ctx.timedOutClientCounter().incrementOrdered();
                        }
                        break;

                    case CLOSED:
                        if (logPublisher.appendSessionClose(session, nowMs))
                        {
                            i.remove();
                            if (session.closeReason() == CloseReason.TIMEOUT)
                            {
                                ctx.timedOutClientCounter().incrementOrdered();
                            }
                        }
                        break;

                    default:
                        i.remove();
                        session.close();
                }

                workCount += 1;
            }
            else if (session.state() == CONNECTED)
            {
                appendSessionOpen(session, nowMs);
                workCount += 1;
            }
            else if (session.hasNewLeaderEventPending())
            {
                sendNewLeaderEvent(session);
                workCount += 1;
            }
        }

        return workCount;
    }

    private void sendNewLeaderEvent(final ClusterSession session)
    {
        if (egressPublisher.newLeader(session, leaderMember.id(), clientFacingEndpoints))
        {
            session.hasNewLeaderEventPending(false);
        }
    }

    private void appendSessionOpen(final ClusterSession session, final long nowMs)
    {
        final long resultingPosition = logPublisher.appendSessionOpen(session, nowMs);
        if (resultingPosition > 0)
        {
            session.open(resultingPosition);
        }
    }

    private void createAppendPosition(final int logSessionId)
    {
        final CountersReader counters = aeron.countersReader();
        final int recordingCounterId = awaitRecordingCounter(counters, logSessionId);

        appendedPosition = new ReadableCounter(counters, recordingCounterId);
    }

    private void recoverFromSnapshot(final RecordingLog.Snapshot snapshot, final AeronArchive archive)
    {
        clusterTimeMs = snapshot.timestamp;
        expectedAckPosition = snapshot.logPosition;
        leadershipTermId = snapshot.leadershipTermId;

        final String channel = ctx.replayChannel();
        final int streamId = ctx.replayStreamId();
        final int sessionId = (int)archive.startReplay(snapshot.recordingId, 0, NULL_LENGTH, channel, streamId);
        final String replaySubscriptionChannel = ChannelUri.addSessionId(channel, sessionId);

        try (Subscription subscription = aeron.addSubscription(replaySubscriptionChannel, streamId))
        {
            final Image image = awaitImage(sessionId, subscription);
            final ConsensusModuleSnapshotLoader snapshotLoader = new ConsensusModuleSnapshotLoader(image, this);

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
                        throw new ClusterException("snapshot ended unexpectedly");
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

    private void awaitServiceAcks(final long logPosition)
    {
        while (!hasReachedPosition(logPosition, serviceAckId, serviceAcks))
        {
            idle(consensusModuleAdapter.poll());
        }

        ++serviceAckId;
    }

    private long logPosition()
    {
        return null != logAdapter ? logAdapter.position() : logPublisher.position();
    }

    private void validateServiceAck(final long logPosition, final long ackId, final int serviceId)
    {
        if (logPosition != expectedAckPosition || ackId != serviceAckId)
        {
            throw new ClusterException("invalid service ACK" +
                ": serviceId=" + serviceId +
                ", logPosition=" + logPosition + " expected " + expectedAckPosition +
                ", ackId=" + ackId + " expected " + serviceAckId);
        }
    }

    private void updateClientFacingEndpoints(final ClusterMember[] members)
    {
        final StringBuilder builder = new StringBuilder(100);

        for (int i = 0, length = members.length; i < length; i++)
        {
            if (0 != i)
            {
                builder.append(',');
            }

            final ClusterMember member = members[i];
            builder.append(member.id()).append('=').append(member.clientFacingEndpoint());
        }

        clientFacingEndpoints = builder.toString();
    }

    private int updateMemberPosition(final long nowMs)
    {
        int workCount = 0;

        if (Cluster.Role.LEADER == role)
        {
            thisMember.logPosition(appendedPosition.get());

            if (commitPosition.proposeMaxOrdered(ClusterMember.quorumPosition(clusterMembers, rankedPositions)) ||
                nowMs >= (timeOfLastLogUpdateMs + leaderHeartbeatIntervalMs))
            {
                final long commitPosition = this.commitPosition.getWeak();
                for (final ClusterMember member : clusterMembers)
                {
                    if (member != thisMember)
                    {
                        final Publication publication = member.publication();
                        memberStatusPublisher.commitPosition(publication, leadershipTermId, commitPosition, memberId);
                    }
                }

                timeOfLastLogUpdateMs = nowMs;
                workCount += 1;
            }
        }
        else
        {
            final long appendedPosition = this.appendedPosition.get();
            final Publication publication = leaderMember.publication();

            if (appendedPosition != lastAppendedPosition &&
                memberStatusPublisher.appendedPosition(publication, leadershipTermId, appendedPosition, memberId))
            {
                lastAppendedPosition = appendedPosition;
                workCount += 1;
            }

            commitPosition.proposeMaxOrdered(logAdapter.position());

            if (nowMs >= (timeOfLastLogUpdateMs + leaderHeartbeatTimeoutMs))
            {
                enterElection(nowMs);
                workCount += 1;
            }
        }

        return workCount;
    }

    private void enterElection(final long nowMs)
    {
        ingressAdapter.close();

        commitPosition.proposeMaxOrdered(followerCommitPosition);

        election = new Election(
            false,
            leadershipTermId,
            commitPosition.getWeak(),
            clusterMembers,
            thisMember,
            memberStatusAdapter,
            memberStatusPublisher,
            ctx,
            this);

        election.doWork(nowMs);
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
            throw new TimeoutException("unexpected interrupt");
        }
    }

    private void takeSnapshot(final long timestampMs, final long logPosition)
    {
        try (Publication publication = aeron.addExclusivePublication(ctx.snapshotChannel(), ctx.snapshotStreamId()))
        {
            final String channel = ChannelUri.addSessionId(ctx.snapshotChannel(), publication.sessionId());
            final long subscriptionId = archive.startRecording(channel, ctx.snapshotStreamId(), LOCAL);
            try
            {
                final CountersReader counters = aeron.countersReader();
                final int counterId = awaitRecordingCounter(counters, publication.sessionId());
                final long recordingId = RecordingPos.getRecordingId(counters, counterId);
                final long termBaseLogPosition = recordingLog.getTermEntry(leadershipTermId).termBaseLogPosition;

                snapshotState(publication, logPosition, leadershipTermId);
                awaitRecordingComplete(recordingId, publication.position(), counters, counterId);

                for (int serviceId = serviceAcks.length - 1; serviceId >= 0; serviceId--)
                {
                    final long snapshotId = serviceAcks[serviceId].relevantId();
                    recordingLog.appendSnapshot(
                        snapshotId, leadershipTermId, termBaseLogPosition, logPosition, timestampMs, serviceId);
                }

                recordingLog.appendSnapshot(
                    recordingId, leadershipTermId, termBaseLogPosition, logPosition, timestampMs, SERVICE_ID);

                recordingLog.force();
            }
            finally
            {
                archive.stopRecording(subscriptionId);
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
                throw new ClusterException("recording has stopped unexpectedly: " + recordingId);
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
            if (session.state() == OPEN || session.state() == CLOSED)
            {
                snapshotTaker.snapshotSession(session);
            }
        }

        aeronClientInvoker.invoke();

        timerService.snapshot(snapshotTaker);
        snapshotTaker.consensusModuleState(nextSessionId);

        snapshotTaker.markEnd(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0);
    }

    private Publication createLogPublication(
        final ChannelUri channelUri, final RecordingLog.RecoveryPlan plan, final long position)
    {
        channelUri.put(TAGS_PARAM_NAME, ConsensusModule.Configuration.LOG_PUBLICATION_TAGS);

        if (!plan.logs.isEmpty())
        {
            final RecordingLog.Log log = plan.logs.get(0);
            logPublicationInitialTermId = log.initialTermId;
            logPublicationTermBufferLength = log.termBufferLength;
            logPublicationMtuLength = log.mtuLength;
        }

        if (NULL_VALUE != logPublicationInitialTermId)
        {
            channelUri.initialPosition(position, logPublicationInitialTermId, logPublicationTermBufferLength);
            channelUri.put(MTU_LENGTH_PARAM_NAME, Integer.toString(logPublicationMtuLength));
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

    private void startLogRecording(final String channel, final SourceLocation sourceLocation)
    {
        if (!recoveryPlan.logs.isEmpty())
        {
            lastRecordingId = recoveryPlan.logs.get(0).recordingId;
        }

        if (RecordingPos.NULL_RECORDING_ID == lastRecordingId)
        {
            archive.startRecording(channel, ctx.logStreamId(), sourceLocation);
        }
        else
        {

            archive.extendRecording(lastRecordingId, channel, ctx.logStreamId(), sourceLocation);
        }

        logRecordingChannel = channel;
    }

    private void replayClusterAction(
        final long leadershipTermId, final long logPosition, final ConsensusModule.State newState)
    {
        if (null == election || election.notReplaying())
        {
            this.leadershipTermId = leadershipTermId;
            expectedAckPosition = logPosition;
            state(newState);
        }
    }

    private void closeExistingLog()
    {
        logPublisher.disconnect();
        CloseHelper.close(logAdapter);
        logAdapter = null;
    }

    private void cancelMissedTimers()
    {
        missedTimersSet.removeIf(timerService::cancelTimer);
    }

    private void onUnavailableIngressImage(final Image image)
    {
        ingressAdapter.freeSessionBuffer(image.sessionId());
    }
}
