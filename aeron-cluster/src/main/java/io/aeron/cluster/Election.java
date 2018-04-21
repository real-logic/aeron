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
        CANDIDATE_BALLOT(3),
        FOLLOWER_BALLOT(4),
        FOLLOWER_TRANSITION(5),
        LEADER_TRANSITION(6),
        LEADER_READY(7),
        FOLLOWER_READY(8);

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
                    throw new IllegalStateException("code already in use: " + code);
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
                throw new IllegalStateException("invalid state counter code: " + code);
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

    private long logPosition;
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
        logPosition = recoveryPlan.lastTermBaseLogPosition + recoveryPlan.lastTermPositionAppended;
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

            case CANDIDATE_BALLOT:
                workCount += candidateBallot(nowMs);
                break;

            case FOLLOWER_BALLOT:
                workCount += followerBallot(nowMs);
                break;

            case FOLLOWER_TRANSITION:
                workCount += followerTransition(nowMs);
                break;

            case LEADER_TRANSITION:
                workCount += leaderTransition(nowMs);
                break;

            case LEADER_READY:
                workCount += leaderReady(nowMs);
                break;

            case FOLLOWER_READY:
                workCount += followerReady(nowMs);
                break;
        }

        return workCount;
    }

    public void onRequestVote(final long logPosition, final long candidateTermId, final int candidateId)
    {
        if (logPosition < this.logPosition)
        {
            memberStatusPublisher.placeVote(
                clusterMembers[candidateId].publication(),
                candidateTermId,
                candidateId,
                thisMember.id(),
                false);

            return;
        }

        switch (state)
        {
            case CANVASS:
            case NOMINATE:
                if (candidateTermId == (leadershipTermId + 1))
                {
                    leadershipTermId = candidateTermId;
                    final long nowMs = ctx.epochClock().time();
                    ctx.recordingLog().appendTerm(candidateTermId, logPosition, nowMs, candidateId);
                    state(State.FOLLOWER_BALLOT, nowMs);

                    memberStatusPublisher.placeVote(
                        clusterMembers[candidateId].publication(),
                        candidateTermId,
                        candidateId,
                        thisMember.id(),
                        true);
                }
                break;

            case FOLLOWER_BALLOT:
            case CANDIDATE_BALLOT:
                if (candidateTermId > leadershipTermId)
                {
                    state(State.CANVASS, ctx.epochClock().time());
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
                state(State.CANVASS, ctx.epochClock().time());
            }
        }
    }

    public void onNewLeadershipTerm(
        final long logPosition, final long leadershipTermId, final int leaderMemberId, final int logSessionId)
    {
        if (leadershipTermId >= this.leadershipTermId && logPosition >= this.logPosition)
        {
            leaderMember = clusterMembers[leaderMemberId];
            this.leadershipTermId = leadershipTermId;
            this.logSessionId = logSessionId;

            if (this.logPosition < logPosition && null == recordingCatchUp)
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

    public void onAppendedPosition(final long logPosition, final long leadershipTermId, final int followerMemberId)
    {
        clusterMembers[followerMemberId]
            .logPosition(logPosition)
            .leadershipTermId(leadershipTermId);
    }

    public void onCommitPosition(final long termPosition, final long leadershipTermId, final int leaderMemberId)
    {
        // TODO: Need to catch up with current leader.
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

    long logPosition()
    {
        return logPosition;
    }

    private int init(final long nowMs)
    {
        stateCounter = ctx.aeron().addCounter(0, "Election State");

        if (clusterMembers.length == 1)
        {
            ++leadershipTermId;
            leaderMember = thisMember;
            ctx.recordingLog().appendTerm(leadershipTermId, logPosition, nowMs, thisMember.id());
            state(State.LEADER_TRANSITION, nowMs);
        }
        else if (ctx.appointedLeaderId() == thisMember.id())
        {
            nominationDeadlineMs = nowMs;
            state(State.NOMINATE, nowMs);
        }
        else
        {
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
                        logPosition,
                        leadershipTermId,
                        thisMember.id());
                }
            }

            workCount += 1;
        }

        if (ctx.appointedLeaderId() != NULL_MEMBER_ID)
        {
            return  workCount;
        }

        final long canvassDeadlineMs = isStartup ?
            TimeUnit.NANOSECONDS.toMillis(ctx.startupStatusTimeoutNs()) :
            TimeUnit.NANOSECONDS.toMillis(ctx.electionTimeoutNs());

        if (ClusterMember.isUnanimousCandidate(clusterMembers, thisMember))
        {
            nominationDeadlineMs = nowMs + random.nextInt((int)statusIntervalMs);
            state(State.NOMINATE, nowMs);
            workCount += 1;
        }
        else if (nowMs >= (timeOfLastStateChangeMs + canvassDeadlineMs) &&
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
        if (nowMs >= nominationDeadlineMs)
        {
            ++leadershipTermId;
            sequencerAgent.role(Cluster.Role.CANDIDATE);
            ClusterMember.becomeCandidate(clusterMembers, thisMember.id());
            ctx.recordingLog().appendTerm(leadershipTermId, logPosition, nowMs, thisMember.id());

            state(State.CANDIDATE_BALLOT, nowMs);
            return 1;
        }

        return 0;
    }

    private int candidateBallot(final long nowMs)
    {
        int workCount = 0;

        if (ClusterMember.hasUnanimousVote(clusterMembers, thisMember))
        {
            state(State.LEADER_TRANSITION, nowMs);
            leaderMember = thisMember;
            workCount += 1;
        }
        else if (nowMs >= (timeOfLastStateChangeMs + TimeUnit.NANOSECONDS.toMillis(ctx.electionTimeoutNs())))
        {
            if (ClusterMember.hasMajorityVote(clusterMembers, thisMember))
            {
                state(State.LEADER_TRANSITION, nowMs);
                leaderMember = thisMember;
            }
            else
            {
                state(State.CANVASS, nowMs);
            }

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
                        member.publication(), logPosition, leadershipTermId, thisMember.id()));
                }
            }
        }

        return workCount;
    }

    private int followerBallot(final long nowMs)
    {
        int workCount = 0;

        if (nowMs >= (timeOfLastStateChangeMs + TimeUnit.NANOSECONDS.toMillis(ctx.electionTimeoutNs())))
        {
            state(State.CANVASS, nowMs);
            workCount += 1;
        }

        return workCount;
    }

    private int followerTransition(final long nowMs)
    {
        int workCount = 1;

        if (null == recordingCatchUp)
        {
            sequencerAgent.updateFollowersMemberDetails();

            final ChannelUri channelUri = followerLogChannel(ctx.logChannel(), thisMember.logEndpoint(), logSessionId);

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
                logPosition = recordingCatchUp.targetPosition();
                sequencerAgent.catchupLog(recordingCatchUp);
                recordingCatchUp = null;

                final ChannelUri channelUri = followerLogChannel(
                    ctx.logChannel(), thisMember.logEndpoint(), logSessionId);

                sequencerAgent.recordFollowerActiveLog(channelUri.toString(), logSessionId);
                sequencerAgent.awaitFollowerServicesReady(channelUri, logSessionId);
                state(State.FOLLOWER_READY, nowMs);
            }
        }

        return workCount;
    }

    private int leaderTransition(final long nowMs)
    {
        sequencerAgent.becomeLeader();
        ClusterMember.resetLogPositions(clusterMembers, NULL_POSITION);
        clusterMembers[thisMember.id()].logPosition(logPosition);
        state(State.LEADER_READY, nowMs);

        return 1;
    }

    private int followerReady(final long nowMs)
    {
        int workCount = 1;
        final Publication publication = leaderMember.publication();

        if (memberStatusPublisher.appendedPosition(publication, logPosition, leadershipTermId, thisMember.id()))
        {
            sequencerAgent.electionComplete();
            close();

            workCount += 0;
        }
        else if (nowMs >= (timeOfLastStateChangeMs + TimeUnit.NANOSECONDS.toMillis(ctx.electionTimeoutNs())))
        {
            state(State.CANVASS, nowMs);
            workCount += 1;
        }

        return workCount;
    }

    private int leaderReady(final long nowMs)
    {
        int workCount = 0;

        if (ClusterMember.haveVotersReachedPosition(clusterMembers, logPosition, leadershipTermId))
        {
            sequencerAgent.electionComplete();
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
                        member.publication(), logPosition, leadershipTermId, thisMember.id(), logSessionId);
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

        if (State.CANVASS == state)
        {
            ClusterMember.reset(clusterMembers);
            thisMember.leadershipTermId(leadershipTermId).logPosition(logPosition);
            sequencerAgent.role(Cluster.Role.FOLLOWER);
        }
    }

    private static ChannelUri followerLogChannel(final String logChannel, final String logEndpoint, final int sessionId)
    {
        final ChannelUri channelUri = ChannelUri.parse(logChannel);
        channelUri.put(CommonContext.ENDPOINT_PARAM_NAME, logEndpoint);
        channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(sessionId));

        return channelUri;
    }
}
