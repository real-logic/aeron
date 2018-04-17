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

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.Counter;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.service.RecordingLog;
import io.aeron.cluster.service.Cluster;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.ClusterMember.NULL_MEMBER_ID;

/**
 * Election process to determine a new cluster leader.
 */
class Election implements MemberStatusListener, AutoCloseable
{
    public static final int ELECTION_STATE_TYPE_ID = 207;

    enum State
    {
        INIT(0),
        CANVASS(1),
        NOMINATE(2),
        FOLLOWER_BALLOT(3),
        CANDIDATE_BALLOT(4),
        FOLLOWER_AWAITING_RESULT(5),
        FOLLOWER_TRANSITION(6),
        LEADER_TRANSITION(7),
        FOLLOWER_READY(8),
        LEADER_READY(9),
        FAILED(10);

        static final State[] STATES;

        static
        {
            final State[] states = values();
            STATES = new State[states.length];
            for (final State state : states)
            {
                final int code = state.code();
                if (null != STATES[code])
                {
                    throw new IllegalStateException("Code already in use: " + code);
                }

                STATES[code] = state;
            }
        }

        private final int code;

        State(final int code)
        {
            this.code = code;
        }

        public int code()
        {
            return code;
        }

        public static State get(final int code)
        {
            if (code < 0 || code > (STATES.length - 1))
            {
                throw new IllegalStateException("Invalid state counter code: " + code);
            }

            return STATES[code];
        }
    }

    private final boolean isStartup;
    private final long statusIntervalMs;
    private final long leaderHeartbeatIntervalMs;
    private final ClusterMember[] clusterMembers;
    private final ClusterMember thisMember;
    private final MemberStatusAdapter memberStatusAdapter;
    private final MemberStatusPublisher memberStatusPublisher;
    private final ConsensusModule.Context ctx;
    private final RecordingLog.RecoveryPlan recoveryPlan;
    private final UnsafeBuffer recoveryPlanBuffer;
    private final AeronArchive localArchive;
    private final SequencerAgent sequencerAgent;
    private final Random random;

    private long timeOfLastStateChangeMs;
    private long timeOfLastUpdateMs;
    private long nominationDeadlineMs;
    private long leadershipTermId;
    private int logSessionId = CommonContext.NULL_SESSION_ID;
    private ClusterMember leaderMember = null;
    private State state = State.INIT;
    private Counter stateCounter;
    private RecordingCatchUp recordingCatchUp;

    Election(
        final boolean isStartup,
        final long leadershipTermId,
        final ClusterMember[] clusterMembers,
        final ClusterMember thisMember,
        final MemberStatusAdapter memberStatusAdapter,
        final MemberStatusPublisher memberStatusPublisher,
        final RecordingLog.RecoveryPlan recoveryPlan,
        final UnsafeBuffer recoveryPlanBuffer,
        final ConsensusModule.Context ctx,
        final AeronArchive localArchive,
        final SequencerAgent sequencerAgent)
    {
        this.isStartup = isStartup;
        this.statusIntervalMs = TimeUnit.NANOSECONDS.toMillis(ctx.statusIntervalNs());
        this.leaderHeartbeatIntervalMs = TimeUnit.NANOSECONDS.toMillis(ctx.leaderHeartbeatIntervalNs());
        this.leadershipTermId = leadershipTermId;
        this.clusterMembers = clusterMembers;
        this.thisMember = thisMember;
        this.memberStatusAdapter = memberStatusAdapter;
        this.memberStatusPublisher = memberStatusPublisher;
        this.recoveryPlan = recoveryPlan;
        this.recoveryPlanBuffer = recoveryPlanBuffer;
        this.ctx = ctx;
        this.localArchive = localArchive;
        this.sequencerAgent = sequencerAgent;
        this.random = ctx.random();
    }

    public void close()
    {
        CloseHelper.close(recordingCatchUp);
        CloseHelper.close(stateCounter);
    }

    public int doWork(final long nowMs)
    {
        int workCount = State.INIT == state ? init(nowMs) : 0;
        workCount += memberStatusAdapter.poll();

        switch (state)
        {
            case CANVASS:
                workCount += canvass(nowMs);
                break;

            case NOMINATE:
                workCount += nominate(nowMs);
                break;

            case FOLLOWER_BALLOT:
                workCount += followerBallot(nowMs);
                break;

            case CANDIDATE_BALLOT:
                workCount += candidateBallot(nowMs);
                break;

            case FOLLOWER_AWAITING_RESULT:
                workCount += followerAwaitingResult(nowMs);
                break;

            case FOLLOWER_TRANSITION:
                workCount += followerTransition(nowMs);
                break;

            case LEADER_TRANSITION:
                workCount += leaderTransition(nowMs);
                break;

            case FOLLOWER_READY:
                workCount += followerReady(nowMs);
                break;

            case LEADER_READY:
                workCount += leaderReady(nowMs);
                break;

            case FAILED:
                break;
        }

        return workCount;
    }

    public void onRequestVote(
        final long candidateTermId,
        final long lastBaseLogPosition,
        final long lastTermPosition,
        final int candidateId)
    {
        switch (state)
        {
            case NOMINATE:
            case FOLLOWER_BALLOT:
                if (candidateTermId == (leadershipTermId + 1))
                {
                    final boolean voteFor = lastTermPosition >= recoveryPlan.lastTermPositionAppended;
                    final long nowMs = ctx.epochClock().time();
                    if (voteFor)
                    {
                        final long logPosition = lastBaseLogPosition + lastTermPosition;
                        ctx.recordingLog().appendTerm(candidateTermId, logPosition, nowMs, candidateId);
                        state(State.FOLLOWER_AWAITING_RESULT, nowMs);
                    }
                    else
                    {
                        state(State.FAILED, nowMs);
                    }

                    memberStatusPublisher.placeVote(
                        clusterMembers[candidateId].publication(),
                        candidateTermId,
                        candidateId,
                        thisMember.id(),
                        voteFor);
                }
                break;

            case CANDIDATE_BALLOT:
                if (candidateTermId >= leadershipTermId)
                {
                    state(State.FAILED, ctx.epochClock().time());
                }
                break;
        }
    }

    public void onVote(
        final long candidateTermId, final int candidateMemberId, final int followerMemberId, final boolean vote)
    {
        if (Cluster.Role.CANDIDATE == sequencerAgent.role() &&
            candidateTermId == leadershipTermId &&
            candidateMemberId == thisMember.id())
        {
            clusterMembers[followerMemberId].votedForId(candidateMemberId);

            if (!vote)
            {
                state(State.FAILED, ctx.epochClock().time());
            }
        }
    }

    public void onNewLeadershipTerm(
        final long lastBaseLogPosition,
        final long lastTermPosition,
        final long leadershipTermId,
        final int leaderMemberId,
        final int logSessionId)
    {
        if (leadershipTermId == (this.leadershipTermId + 1))
        {
            leaderMember = clusterMembers[leaderMemberId];
            this.leadershipTermId = leadershipTermId;
            this.logSessionId = logSessionId;

            if (recoveryPlan.lastTermPositionAppended < lastTermPosition && null == recordingCatchUp)
            {
                recordingCatchUp = ctx.recordingCatchUpSupplier().catchUp(
                    localArchive,
                    memberStatusPublisher,
                    clusterMembers,
                    leaderMemberId,
                    thisMember.id(),
                    recoveryPlan,
                    ctx);
            }

            state(State.FOLLOWER_TRANSITION, ctx.epochClock().time());
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
        if (null != recordingCatchUp)
        {
            recordingCatchUp.onLeaderRecoveryPlan(
                correlationId, requestMemberId, responseMemberId, data, offset, length);
        }
    }

    public void onRecoveryPlanQuery(final long correlationId, final int leaderMemberId, final int requestMemberId)
    {
        if (leaderMemberId == thisMember.id())
        {
            memberStatusPublisher.queryResponse(
                clusterMembers[requestMemberId].publication(),
                correlationId,
                requestMemberId,
                thisMember.id(),
                recoveryPlanBuffer,
                0,
                recoveryPlanBuffer.capacity());
        }
    }

    public void onAppendedPosition(final long termPosition, final long leadershipTermId, final int followerMemberId)
    {
        switch (state)
        {
            case CANVASS:
                clusterMembers[followerMemberId]
                    .termPosition(termPosition)
                    .leadershipTermId(leadershipTermId);

                if (leadershipTermId > thisMember.leadershipTermId() || termPosition > thisMember.termPosition())
                {
                    state(State.FOLLOWER_BALLOT, ctx.epochClock().time());
                }
                else if (ClusterMember.isCertainCandidate(clusterMembers, thisMember))
                {
                    nominationDeadlineMs = ctx.epochClock().time() + random.nextInt((int)statusIntervalMs);
                    state(State.NOMINATE, ctx.epochClock().time());
                }
                break;

            case LEADER_READY:
                if (leadershipTermId == this.leadershipTermId)
                {
                    clusterMembers[followerMemberId].termPosition(termPosition);
                }
                break;
        }
    }

    public void onCommitPosition(final long termPosition, final long leadershipTermId, final int leaderMemberId)
    {
    }

    State state()
    {
        return state;
    }

    ClusterMember leader()
    {
        return leaderMember;
    }

    long leadershipTermId()
    {
        return leadershipTermId;
    }

    void logSessionId(final int logSessionId)
    {
        this.logSessionId = logSessionId;
    }

    private int init(final long nowMs)
    {
        stateCounter = ctx.aeron().addCounter(0, "Election State");

        if (clusterMembers.length == 1)
        {
            ++leadershipTermId;

            sequencerAgent.role(Cluster.Role.LEADER);
            leaderMember = thisMember;


            final long logPosition = recoveryPlan.lastTermBaseLogPosition + recoveryPlan.lastTermPositionAppended;
            ctx.recordingLog().appendTerm(leadershipTermId, logPosition, nowMs, thisMember.id());

            state(State.LEADER_TRANSITION, nowMs);
        }
        else if (ctx.appointedLeaderId() != NULL_MEMBER_ID)
        {
            final int memberId = thisMember.id();
            if (ctx.appointedLeaderId() == memberId)
            {
                nominationDeadlineMs = nowMs;
                state(State.NOMINATE, nowMs);
            }
            else
            {
                state(State.FOLLOWER_BALLOT, nowMs);
            }
        }
        else
        {
            thisMember
                .leadershipTermId(recoveryPlan.lastLeadershipTermId)
                .termPosition(recoveryPlan.lastTermPositionAppended);
            state(State.CANVASS, nowMs);
        }

        return 1;
    }

    private int canvass(final long nowMs)
    {
        int workCount = 0;

        if (nowMs >= (timeOfLastUpdateMs + statusIntervalMs))
        {
            timeOfLastUpdateMs = nowMs;
            for (final ClusterMember member : clusterMembers)
            {
                if (member != thisMember)
                {
                    memberStatusPublisher.appendedPosition(
                        member.publication(),
                        recoveryPlan.lastTermPositionAppended,
                        recoveryPlan.lastLeadershipTermId,
                        thisMember.id());
                }
            }

            workCount += 1;
        }

        final long canvassDeadlineMs = isStartup ?
            TimeUnit.NANOSECONDS.toMillis(ctx.startupStatusTimeoutNs()) :
            TimeUnit.NANOSECONDS.toMillis(ctx.electionTimeoutNs());

        if (nowMs >= (timeOfLastStateChangeMs + canvassDeadlineMs) &&
            ClusterMember.isQuorumCandidate(clusterMembers, thisMember))
        {
            nominationDeadlineMs = nowMs + random.nextInt((int)statusIntervalMs);
            state(State.NOMINATE, nowMs);
            workCount += 1;
        }

        return workCount;
    }

    private int nominate(final long nowMs)
    {
        if (State.NOMINATE == state && nowMs >= nominationDeadlineMs)
        {
            ++leadershipTermId;
            sequencerAgent.role(Cluster.Role.CANDIDATE);

            final int memberId = thisMember.id();
            ClusterMember.becomeCandidate(clusterMembers, memberId);

            final long logPosition = recoveryPlan.lastTermBaseLogPosition + recoveryPlan.lastTermPositionAppended;
            ctx.recordingLog().appendTerm(leadershipTermId, logPosition, nowMs, memberId);

            state(State.CANDIDATE_BALLOT, nowMs);
            return 1;
        }

        return 0;
    }

    private int followerBallot(final long nowMs)
    {
        return memberStatusAdapter.poll();
    }

    private int candidateBallot(final long nowMs)
    {
        int workCount = 0;

        if (!ClusterMember.awaitingVotes(clusterMembers))
        {
            state(State.LEADER_TRANSITION, nowMs);
            leaderMember = thisMember;
            workCount += 1;
        }
        else
        {
            for (final ClusterMember member : clusterMembers)
            {
                if (!member.isBallotSent())
                {
                    workCount += 1;
                    member.isBallotSent(memberStatusPublisher.requestVote(
                        member.publication(),
                        recoveryPlan.lastTermBaseLogPosition,
                        recoveryPlan.lastTermPositionAppended,
                        leadershipTermId,
                        thisMember.id()));
                }
            }
        }

        return workCount;
    }

    private int followerAwaitingResult(final long nowMs)
    {
        return memberStatusAdapter.poll();
    }

    private int followerTransition(final long nowMs)
    {
        int workCount = 1;

        if (null == recordingCatchUp)
        {
            sequencerAgent.updateFollowersMemberDetails();

            final ChannelUri channelUri = followerLogChannel(ctx.logChannel(), thisMember, logSessionId);

            sequencerAgent.recordFollowerActiveLog(channelUri.toString(), logSessionId);
            sequencerAgent.awaitFollowerServicesReady(channelUri, logSessionId);
            state(State.FOLLOWER_READY, nowMs);
        }
        else
        {
            if (recordingCatchUp.isInInit())
            {
                sequencerAgent.updateFollowersMemberDetails();
            }

            if (!recordingCatchUp.isCaughtUp())
            {
                workCount += memberStatusAdapter.poll();
                workCount += recordingCatchUp.doWork();
            }
            else
            {
                recordingCatchUp.close();

                sequencerAgent.catchupLog(recordingCatchUp);
                recordingCatchUp = null;

                final ChannelUri channelUri = followerLogChannel(ctx.logChannel(), thisMember, logSessionId);

                sequencerAgent.recordFollowerActiveLog(channelUri.toString(), logSessionId);
                sequencerAgent.awaitFollowerServicesReady(channelUri, logSessionId);
                state(State.FOLLOWER_READY, nowMs);
            }
        }

        return workCount;
    }

    private int leaderTransition(final long nowMs)
    {
        sequencerAgent.becomeLeader(nowMs);
        ClusterMember.resetTermPositions(clusterMembers, NULL_POSITION);
        clusterMembers[thisMember.id()].termPosition(0);
        state(State.LEADER_READY, nowMs);

        return 1;
    }

    private int followerReady(final long nowMs)
    {
        final Publication publication = leaderMember.publication();

        if (memberStatusPublisher.appendedPosition(publication, 0, leadershipTermId, thisMember.id()))
        {
            sequencerAgent.electionComplete(Cluster.Role.FOLLOWER);
            close();

            return 1;
        }

        return 0;
    }

    private int leaderReady(final long nowMs)
    {
        int workCount = 0;

        if (ClusterMember.hasReachedPosition(clusterMembers, 0))
        {
            sequencerAgent.electionComplete(Cluster.Role.LEADER);
            close();

            workCount += 1;
        }
        else if (nowMs > (timeOfLastUpdateMs + leaderHeartbeatIntervalMs))
        {
            timeOfLastUpdateMs = nowMs;

            for (final ClusterMember member : clusterMembers)
            {
                if (member != thisMember)
                {
                    memberStatusPublisher.newLeadershipTerm(
                        member.publication(),
                        recoveryPlan.lastTermBaseLogPosition,
                        recoveryPlan.lastTermPositionAppended,
                        leadershipTermId,
                        thisMember.id(),
                        logSessionId);
                }
            }

            workCount += 1;
        }

        return workCount;
    }

    private void state(final State state, final long nowMs)
    {
        //System.out.println(this.state + " -> " + state);
        timeOfLastStateChangeMs = nowMs;
        this.state = state;
        stateCounter.setOrdered(state.code());
    }

    private ChannelUri followerLogChannel(final String logChannel, final ClusterMember member, final int sessionId)
    {
        final ChannelUri channelUri = ChannelUri.parse(logChannel);
        channelUri.put(CommonContext.ENDPOINT_PARAM_NAME, member.logEndpoint());
        channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(sessionId));

        return channelUri;
    }
}
