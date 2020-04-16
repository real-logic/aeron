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
package io.aeron.cluster;

import io.aeron.*;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.service.Cluster;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.status.AtomicCounter;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.ClusterMember.compareLog;
import static io.aeron.exceptions.AeronException.Category.WARN;

/**
 * Election process to determine a new cluster leader and catch up followers.
 */
public class Election
{
    /**
     * The type id of the {@link Counter} used for the election state.
     */
    static final int ELECTION_STATE_TYPE_ID = 207;

    public enum State
    {
        INIT(0),
        CANVASS(1),

        NOMINATE(2),
        CANDIDATE_BALLOT(3),
        FOLLOWER_BALLOT(4),

        LEADER_REPLAY(5),
        LEADER_TRANSITION(6),
        LEADER_READY(7),

        FOLLOWER_REPLAY(8),
        FOLLOWER_CATCHUP_TRANSITION(9),
        FOLLOWER_CATCHUP(10),
        FOLLOWER_TRANSITION(11),
        FOLLOWER_READY(12),

        CLOSED(13);

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
                    throw new ClusterException("code already in use: " + code);
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
                throw new ClusterException("invalid state counter code: " + code);
            }

            return STATES[code];
        }

        public static State get(final AtomicCounter counter)
        {
            return get((int)counter.get());
        }
    }

    private final boolean isNodeStartup;
    private boolean isLeaderStartup;
    private boolean isExtendedCanvass;
    private boolean hasReplay;
    private int logSessionId = CommonContext.NULL_SESSION_ID;
    private long timeOfLastStateChangeNs;
    private long timeOfLastUpdateNs;
    private long nominationDeadlineNs;
    private long logPosition;
    private long catchupPosition = NULL_POSITION;
    private long leadershipTermId;
    private long logLeadershipTermId;
    private long candidateTermId = NULL_VALUE;
    private ClusterMember leaderMember = null;
    private State state = State.INIT;
    private Subscription logSubscription;
    private String liveLogDestination;
    private LogReplay logReplay = null;
    private final ClusterMember[] clusterMembers;
    private final ClusterMember thisMember;
    private final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap;
    private final MemberStatusAdapter memberStatusAdapter;
    private final MemberStatusPublisher memberStatusPublisher;
    private final ConsensusModule.Context ctx;
    private final ConsensusModuleAgent consensusModuleAgent;
    private final Random random;

    public Election(
        final boolean isNodeStartup,
        final long leadershipTermId,
        final long logPosition,
        final ClusterMember[] clusterMembers,
        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap,
        final ClusterMember thisMember,
        final MemberStatusAdapter memberStatusAdapter,
        final MemberStatusPublisher memberStatusPublisher,
        final ConsensusModule.Context ctx,
        final ConsensusModuleAgent consensusModuleAgent)
    {
        this.isNodeStartup = isNodeStartup;
        this.hasReplay = isNodeStartup;
        this.isExtendedCanvass = isNodeStartup;
        this.logPosition = logPosition;
        this.logLeadershipTermId = leadershipTermId;
        this.leadershipTermId = leadershipTermId;
        this.clusterMembers = clusterMembers;
        this.clusterMemberByIdMap = clusterMemberByIdMap;
        this.thisMember = thisMember;
        this.memberStatusAdapter = memberStatusAdapter;
        this.memberStatusPublisher = memberStatusPublisher;
        this.ctx = ctx;
        this.consensusModuleAgent = consensusModuleAgent;
        this.random = ctx.random();

        ctx.electionStateCounter().setOrdered(State.INIT.code());
    }

    public ClusterMember leader()
    {
        return leaderMember;
    }

    public long leadershipTermId()
    {
        return leadershipTermId;
    }

    public long logPosition()
    {
        return logPosition;
    }

    public boolean isLeaderStartup()
    {
        return isLeaderStartup;
    }

    int doWork(final long nowNs)
    {
        int workCount = State.INIT == state ? init(nowNs) : 0;
        workCount += memberStatusAdapter.poll();

        try
        {
            switch (state)
            {
                case CANVASS:
                    workCount += canvass(nowNs);
                    break;

                case NOMINATE:
                    workCount += nominate(nowNs);
                    break;

                case CANDIDATE_BALLOT:
                    workCount += candidateBallot(nowNs);
                    break;

                case FOLLOWER_BALLOT:
                    workCount += followerBallot(nowNs);
                    break;

                case LEADER_REPLAY:
                    workCount += leaderReplay(nowNs);
                    break;

                case LEADER_TRANSITION:
                    workCount += leaderTransition(nowNs);
                    break;

                case LEADER_READY:
                    workCount += leaderReady(nowNs);
                    break;

                case FOLLOWER_REPLAY:
                    workCount += followerReplay(nowNs);
                    break;

                case FOLLOWER_CATCHUP_TRANSITION:
                    workCount += followerCatchupTransition(nowNs);
                    break;

                case FOLLOWER_CATCHUP:
                    workCount += followerCatchup(nowNs);
                    break;

                case FOLLOWER_TRANSITION:
                    workCount += followerTransition(nowNs);
                    break;

                case FOLLOWER_READY:
                    workCount += followerReady(nowNs);
                    break;
            }
        }
        catch (final AgentTerminationException ex)
        {
            throw ex;
        }
        catch (final Exception ex)
        {
            ctx.countedErrorHandler().onError(ex);
            logPosition = ctx.commitPositionCounter().get();
            state(State.INIT, nowNs);
        }

        return workCount;
    }

    void onCanvassPosition(final long logLeadershipTermId, final long logPosition, final int followerMemberId)
    {
        final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
        if (null != follower)
        {
            follower
                .leadershipTermId(logLeadershipTermId)
                .logPosition(logPosition);

            if (State.LEADER_READY == state && logLeadershipTermId < leadershipTermId)
            {
                final RecordingLog.Entry termEntry = ctx.recordingLog().getTermEntry(logLeadershipTermId + 1);
                memberStatusPublisher.newLeadershipTerm(
                    follower.publication(),
                    logLeadershipTermId,
                    termEntry.termBaseLogPosition,
                    leadershipTermId,
                    this.logPosition,
                    termEntry.timestamp,
                    thisMember.id(),
                    logSessionId,
                    isLeaderStartup);
            }
            else if ((State.LEADER_TRANSITION == state || State.LEADER_REPLAY == state) &&
                logLeadershipTermId < leadershipTermId)
            {
                final RecordingLog.Entry termEntry = ctx.recordingLog().findTermEntry(logLeadershipTermId + 1);
                memberStatusPublisher.newLeadershipTerm(
                    follower.publication(),
                    null != termEntry ? logLeadershipTermId : this.logLeadershipTermId,
                    null != termEntry ? termEntry.termBaseLogPosition : this.logPosition,
                    leadershipTermId,
                    this.logPosition,
                    null != termEntry ? termEntry.timestamp : ctx.clusterClock().time(),
                    thisMember.id(),
                    logSessionId,
                    isLeaderStartup);
            }
            else if (logLeadershipTermId > leadershipTermId && (State.CANVASS != state && State.INIT != state))
            {
                state(State.CANVASS, ctx.clusterClock().timeNanos());
            }
        }
    }

    void onRequestVote(
        final long logLeadershipTermId, final long logPosition, final long candidateTermId, final int candidateId)
    {
        if (isPassiveMember() || candidateId == thisMember.id())
        {
            return;
        }

        if (candidateTermId <= leadershipTermId || candidateTermId <= this.candidateTermId)
        {
            placeVote(candidateTermId, candidateId, false);
        }
        else if (compareLog(this.logLeadershipTermId, this.logPosition, logLeadershipTermId, logPosition) > 0)
        {
            this.candidateTermId = candidateTermId;
            ctx.clusterMarkFile().candidateTermId(candidateTermId, ctx.fileSyncLevel());
            state(State.CANVASS, ctx.clusterClock().timeNanos());

            placeVote(candidateTermId, candidateId, false);
        }
        else
        {
            this.candidateTermId = candidateTermId;
            ctx.clusterMarkFile().candidateTermId(candidateTermId, ctx.fileSyncLevel());
            state(State.FOLLOWER_BALLOT, ctx.clusterClock().timeNanos());

            placeVote(candidateTermId, candidateId, true);
        }
    }

    void onVote(
        final long candidateTermId,
        final long logLeadershipTermId,
        final long logPosition,
        final int candidateMemberId,
        final int followerMemberId,
        final boolean vote)
    {
        final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);

        if (State.CANDIDATE_BALLOT == state &&
            candidateTermId == this.candidateTermId &&
            candidateMemberId == thisMember.id() &&
            null != follower)
        {
            follower
                .candidateTermId(candidateTermId)
                .leadershipTermId(logLeadershipTermId)
                .logPosition(logPosition)
                .vote(vote ? Boolean.TRUE : Boolean.FALSE);
        }
    }

    void onNewLeadershipTerm(
        final long logLeadershipTermId,
        final long logTruncatePosition,
        final long leadershipTermId,
        final long logPosition,
        @SuppressWarnings("unused") final long timestamp,
        final int leaderMemberId,
        final int logSessionId,
        final boolean isStartup)
    {
        final ClusterMember leader = clusterMemberByIdMap.get(leaderMemberId);
        if (null == leader || (leaderMemberId == thisMember.id() && leadershipTermId == this.leadershipTermId))
        {
            return;
        }

        if (leadershipTermId > this.leadershipTermId &&
            logLeadershipTermId == this.logLeadershipTermId &&
            logTruncatePosition < this.logPosition)
        {
            consensusModuleAgent.truncateLogEntry(logLeadershipTermId, logTruncatePosition);
            consensusModuleAgent.prepareForNewLeadership(logTruncatePosition);
            leaderMember = leader;
            this.isLeaderStartup = isStartup;
            this.leadershipTermId = leadershipTermId;
            this.candidateTermId = Math.max(leadershipTermId, candidateTermId);
            this.logSessionId = logSessionId;
            this.logPosition = logTruncatePosition;
            catchupPosition = logPosition;
            state(State.FOLLOWER_REPLAY, ctx.clusterClock().timeNanos());
        }
        else if (logLeadershipTermId == this.logLeadershipTermId && leadershipTermId == candidateTermId &&
            (State.FOLLOWER_BALLOT == state || State.CANDIDATE_BALLOT == state || State.CANVASS == state))
        {
            leaderMember = leader;
            this.isLeaderStartup = isStartup;
            this.leadershipTermId = leadershipTermId;
            this.logSessionId = logSessionId;
            catchupPosition = logPosition > this.logPosition ? logPosition : NULL_POSITION;
            state(State.FOLLOWER_REPLAY, ctx.clusterClock().timeNanos());
        }
        else if (0 != compareLog(this.logLeadershipTermId, this.logPosition, logLeadershipTermId, logPosition))
        {
            if (NULL_POSITION == catchupPosition)
            {
                if (logPosition >= this.logPosition && leadershipTermId >= candidateTermId)
                {
                    leaderMember = leader;
                    this.isLeaderStartup = isStartup;
                    this.leadershipTermId = leadershipTermId;
                    this.candidateTermId = leadershipTermId;
                    this.logSessionId = logSessionId;
                    catchupPosition = logPosition;
                    state(State.FOLLOWER_REPLAY, ctx.clusterClock().timeNanos());
                }
            }
        }
    }

    void onAppendPosition(final long leadershipTermId, final long logPosition, final int followerMemberId)
    {
        final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
        if (null != follower && leadershipTermId == this.leadershipTermId)
        {
            follower
                .logPosition(logPosition)
                .leadershipTermId(leadershipTermId)
                .timeOfLastAppendPositionNs(ctx.clusterClock().timeNanos());

            consensusModuleAgent.trackCatchupCompletion(follower, leadershipTermId);
        }
    }

    void onCommitPosition(final long leadershipTermId, final long logPosition, final int leaderMemberId)
    {
        if (State.FOLLOWER_CATCHUP == state &&
            leadershipTermId == this.leadershipTermId &&
            leaderMemberId == leaderMember.id() &&
            NULL_POSITION != catchupPosition)
        {
            catchupPosition = Math.max(catchupPosition, logPosition);
        }
        else if (leadershipTermId > this.leadershipTermId)
        {
            state(State.INIT, ctx.clusterClock().timeNanos());
        }
    }

    void onReplayNewLeadershipTermEvent(
        final long logRecordingId,
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition)
    {
        if (State.FOLLOWER_CATCHUP == state)
        {
            boolean hasUpdates = false;
            final RecordingLog recordingLog = ctx.recordingLog();

            for (long termId = logLeadershipTermId; termId <= leadershipTermId; termId++)
            {
                if (!recordingLog.isUnknown(termId - 1))
                {
                    recordingLog.commitLogPosition(termId - 1, termBaseLogPosition);
                    hasUpdates = true;
                }

                if (recordingLog.isUnknown(termId))
                {
                    recordingLog.appendTerm(logRecordingId, termId, termBaseLogPosition, timestamp);
                    hasUpdates = true;
                }
            }

            if (hasUpdates)
            {
                recordingLog.force(ctx.fileSyncLevel());
            }

            logLeadershipTermId = leadershipTermId;
            this.logPosition = logPosition;
        }
    }

    private int init(final long nowNs)
    {
        if (!isNodeStartup)
        {
            cleanupReplay();
            consensusModuleAgent.prepareForNewLeadership(logPosition);
        }

        candidateTermId = Math.max(ctx.clusterMarkFile().candidateTermId(), leadershipTermId);

        if (clusterMembers.length == 1 && thisMember == clusterMembers[0])
        {
            candidateTermId = Math.max(leadershipTermId + 1, candidateTermId + 1);
            leadershipTermId = candidateTermId;
            leaderMember = thisMember;
            state(State.LEADER_REPLAY, nowNs);
        }
        else
        {
            state(State.CANVASS, nowNs);
        }

        return 1;
    }

    private int canvass(final long nowNs)
    {
        int workCount = 0;

        if (nowNs >= (timeOfLastUpdateNs + ctx.electionStatusIntervalNs()))
        {
            timeOfLastUpdateNs = nowNs;
            for (final ClusterMember member : clusterMembers)
            {
                if (member != thisMember)
                {
                    memberStatusPublisher.canvassPosition(
                        member.publication(), leadershipTermId, logPosition, thisMember.id());
                }
            }

            workCount += 1;
        }

        if (isPassiveMember() || (ctx.appointedLeaderId() != NULL_VALUE && ctx.appointedLeaderId() != thisMember.id()))
        {
            return workCount;
        }

        final long canvassDeadlineNs =
            timeOfLastStateChangeNs + (isExtendedCanvass ? ctx.startupCanvassTimeoutNs() : ctx.electionTimeoutNs());

        if (ClusterMember.isUnanimousCandidate(clusterMembers, thisMember) ||
            (ClusterMember.isQuorumCandidate(clusterMembers, thisMember) && nowNs >= canvassDeadlineNs))
        {
            final long delayNs = (long)(random.nextDouble() * (ctx.electionTimeoutNs() >> 1));
            nominationDeadlineNs = nowNs + delayNs;
            state(State.NOMINATE, nowNs);
            workCount += 1;
        }

        return workCount;
    }

    private int nominate(final long nowNs)
    {
        if (nowNs >= nominationDeadlineNs)
        {
            candidateTermId = Math.max(leadershipTermId + 1, candidateTermId + 1);
            ClusterMember.becomeCandidate(clusterMembers, candidateTermId, thisMember.id());
            ctx.clusterMarkFile().candidateTermId(candidateTermId, ctx.fileSyncLevel());
            state(State.CANDIDATE_BALLOT, nowNs);
            return 1;
        }

        return 0;
    }

    private int candidateBallot(final long nowNs)
    {
        int workCount = 0;

        if (ClusterMember.hasWonVoteOnFullCount(clusterMembers, candidateTermId) ||
            ClusterMember.hasMajorityVoteWithCanvassMembers(clusterMembers, candidateTermId))
        {
            leaderMember = thisMember;
            leadershipTermId = candidateTermId;
            state(State.LEADER_REPLAY, nowNs);
            workCount += 1;
        }
        else if (nowNs >= (timeOfLastStateChangeNs + ctx.electionTimeoutNs()))
        {
            if (ClusterMember.hasMajorityVote(clusterMembers, candidateTermId))
            {
                leaderMember = thisMember;
                leadershipTermId = candidateTermId;
                state(State.LEADER_REPLAY, nowNs);
            }
            else
            {
                state(State.CANVASS, nowNs);
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
                        member.publication(), logLeadershipTermId, logPosition, candidateTermId, thisMember.id()));
                }
            }
        }

        return workCount;
    }

    private int followerBallot(final long nowNs)
    {
        int workCount = 0;

        if (nowNs >= (timeOfLastStateChangeNs + ctx.electionTimeoutNs()))
        {
            state(State.CANVASS, nowNs);
            workCount += 1;
        }

        return workCount;
    }

    private int leaderReplay(final long nowNs)
    {
        int workCount = 0;

        if (null == logReplay)
        {
            logSessionId = consensusModuleAgent.addNewLogPublication().sessionId();

            ClusterMember.resetLogPositions(clusterMembers, NULL_POSITION);
            thisMember.logPosition(logPosition).leadershipTermId(leadershipTermId);

            if (!hasReplay || (logReplay = consensusModuleAgent.newLogReplay(logPosition)) == null)
            {
                hasReplay = false;
                state(State.LEADER_TRANSITION, nowNs);
                workCount = 1;
            }
        }
        else
        {
            workCount += logReplay.doWork(nowNs);
            if (logReplay.isDone())
            {
                cleanupReplay();
                state(State.LEADER_TRANSITION, nowNs);
            }
            else if (nowNs > (timeOfLastUpdateNs + ctx.leaderHeartbeatIntervalNs()))
            {
                timeOfLastUpdateNs = nowNs;
                final long timestamp = ctx.clusterClock().time();

                for (final ClusterMember member : clusterMembers)
                {
                    if (member != thisMember)
                    {
                        publishNewLeadershipTerm(member.publication(), leadershipTermId, timestamp);
                    }
                }

                workCount += 1;
            }
        }

        return workCount;
    }

    private int leaderTransition(final long nowNs)
    {
        isLeaderStartup = isNodeStartup;
        consensusModuleAgent.becomeLeader(leadershipTermId, logPosition, logSessionId, isLeaderStartup);

        final long recordingId = consensusModuleAgent.logRecordingId();
        final long timestamp = ctx.clusterClock().timeUnit().convert(nowNs, TimeUnit.NANOSECONDS);
        final RecordingLog recordingLog = ctx.recordingLog();

        for (long termId = logLeadershipTermId + 1; termId <= leadershipTermId; termId++)
        {
            if (recordingLog.isUnknown(termId))
            {
                recordingLog.appendTerm(recordingId, termId, logPosition, timestamp);
            }
        }

        recordingLog.force(ctx.fileSyncLevel());
        state(State.LEADER_READY, nowNs);

        return 1;
    }

    private int leaderReady(final long nowNs)
    {
        int workCount = 0;

        if (ClusterMember.haveVotersReachedPosition(clusterMembers, logPosition, leadershipTermId))
        {
            if (consensusModuleAgent.electionComplete())
            {
                consensusModuleAgent.updateMemberDetails(this);
                state(State.CLOSED, nowNs);
            }

            workCount += 1;
        }
        else if (nowNs > (timeOfLastUpdateNs + ctx.leaderHeartbeatIntervalNs()))
        {
            timeOfLastUpdateNs = nowNs;
            final long timestamp = ctx.recordingLog().getTermTimestamp(leadershipTermId);

            for (final ClusterMember member : clusterMembers)
            {
                if (member != thisMember)
                {
                    publishNewLeadershipTerm(member.publication(), leadershipTermId, timestamp);
                }
            }

            workCount += 1;
        }

        return workCount;
    }

    private int followerReplay(final long nowNs)
    {
        int workCount = 0;

        final State nextState = NULL_POSITION != catchupPosition ?
            State.FOLLOWER_CATCHUP_TRANSITION : State.FOLLOWER_TRANSITION;

        if (null == logReplay)
        {
            if (!hasReplay || (logReplay = consensusModuleAgent.newLogReplay(logPosition)) == null)
            {
                hasReplay = false;
                state(nextState, nowNs);
                workCount = 1;
            }
        }
        else
        {
            workCount += logReplay.doWork(nowNs);
            if (logReplay.isDone())
            {
                cleanupReplay();
                state(nextState, nowNs);
            }
        }

        return workCount;
    }

    private int followerCatchupTransition(final long nowNs)
    {
        if (null == logSubscription)
        {
            final String logChannel = followerLogChannel(
                ctx.logChannel(), logSessionId, consensusModuleAgent.logSubscriptionTags());

            logSubscription = consensusModuleAgent.createAndRecordLogSubscriptionAsFollower(logChannel);
            consensusModuleAgent.awaitServicesReady(logChannel, logSessionId, logPosition, isLeaderStartup);

            final String replayDestination = new ChannelUriStringBuilder()
                .media(CommonContext.UDP_MEDIA)
                .endpoint(thisMember.transferEndpoint())
                .build();

            logSubscription.asyncAddDestination(replayDestination);
            consensusModuleAgent.replayLogDestination(replayDestination);
        }

        if (sendCatchupPosition(leadershipTermId, logPosition))
        {
            timeOfLastUpdateNs = nowNs;
            consensusModuleAgent.catchupInitiated(nowNs);
            state(State.FOLLOWER_CATCHUP, nowNs);
        }

        return 1;
    }

    private int followerCatchup(final long nowNs)
    {
        int workCount = consensusModuleAgent.catchupPoll(logSubscription, logSessionId, catchupPosition, nowNs);

        if (null == liveLogDestination &&
            consensusModuleAgent.hasAppendReachedLivePosition(logSubscription, logSessionId, catchupPosition))
        {
            liveLogDestination = addLiveLogDestination();
        }

        if (consensusModuleAgent.hasAppendReachedPosition(logSubscription, logSessionId, catchupPosition))
        {
            logPosition = catchupPosition;
            timeOfLastUpdateNs = 0;
            state(State.FOLLOWER_TRANSITION, nowNs);
            workCount += 1;
        }
        else if (nowNs > (timeOfLastUpdateNs + ctx.leaderHeartbeatIntervalNs()))
        {
            if (consensusModuleAgent.hasCatchupStalled(nowNs, ctx.leaderHeartbeatTimeoutNs()))
            {
                ctx.countedErrorHandler().onError(new ClusterException("no catchup progress", WARN));
                logPosition = ctx.commitPositionCounter().get();
                state(State.INIT, nowNs);
                workCount += 1;
            }
            else if (consensusModuleAgent.hasReplayDestination() && sendCatchupPosition(leadershipTermId, logPosition))
            {
                timeOfLastUpdateNs = nowNs;
                workCount += 1;
            }
        }

        return workCount;
    }

    private int followerTransition(final long nowNs)
    {
        if (null == logSubscription)
        {
            final String logChannel = followerLogChannel(
                ctx.logChannel(), logSessionId, consensusModuleAgent.logSubscriptionTags());

            logSubscription = consensusModuleAgent.createAndRecordLogSubscriptionAsFollower(logChannel);
            consensusModuleAgent.awaitServicesReady(logChannel, logSessionId, logPosition, isLeaderStartup);
        }

        if (null == liveLogDestination)
        {
            liveLogDestination = addLiveLogDestination();
        }

        consensusModuleAgent.awaitFollowerLogImage(logSubscription, logSessionId);

        final long timestamp = ctx.clusterClock().timeUnit().convert(nowNs, TimeUnit.NANOSECONDS);
        final long recordingId = consensusModuleAgent.logRecordingId();
        boolean hasUpdates = false;

        for (long termId = logLeadershipTermId + 1; termId <= leadershipTermId; termId++)
        {
            if (ctx.recordingLog().isUnknown(termId))
            {
                ctx.recordingLog().appendTerm(recordingId, termId, logPosition, timestamp);
                hasUpdates = true;
            }
        }

        if (hasUpdates)
        {
            ctx.recordingLog().force(ctx.fileSyncLevel());
        }

        state(State.FOLLOWER_READY, nowNs);

        return 1;
    }

    private int followerReady(final long nowNs)
    {
        final ExclusivePublication publication = leaderMember.publication();

        if (memberStatusPublisher.appendPosition(publication, leadershipTermId, logPosition, thisMember.id()))
        {
            if (consensusModuleAgent.electionComplete())
            {
                consensusModuleAgent.updateMemberDetails(this);
                state(State.CLOSED, nowNs);
            }
        }
        else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
        {
            if (null != liveLogDestination)
            {
                logSubscription.asyncRemoveDestination(liveLogDestination);
                liveLogDestination = null;
                consensusModuleAgent.liveLogDestination(null);
            }

            state(State.CANVASS, nowNs);
        }

        return 1;
    }

    private void placeVote(final long candidateTermId, final int candidateId, final boolean vote)
    {
        final ClusterMember candidate = clusterMemberByIdMap.get(candidateId);
        if (null != candidate)
        {
            memberStatusPublisher.placeVote(
                candidate.publication(),
                candidateTermId,
                logLeadershipTermId,
                logPosition,
                candidateId,
                thisMember.id(),
                vote);
        }
    }

    private void publishNewLeadershipTerm(
        final ExclusivePublication publication, final long leadershipTermId, final long timestamp)
    {
        memberStatusPublisher.newLeadershipTerm(
            publication,
            logLeadershipTermId,
            logPosition,
            leadershipTermId,
            logPosition,
            timestamp,
            thisMember.id(),
            logSessionId,
            isLeaderStartup);
    }

    private boolean sendCatchupPosition(final long leadershipTermId, final long logPosition)
    {
        return memberStatusPublisher.catchupPosition(
            leaderMember.publication(), leadershipTermId, logPosition, thisMember.id());
    }

    private String addLiveLogDestination()
    {
        final ChannelUri channelUri = followerLogDestination(ctx.logChannel(), thisMember.logEndpoint());
        final String liveLogDestination = channelUri.toString();

        logSubscription.asyncAddDestination(liveLogDestination);
        consensusModuleAgent.liveLogDestination(liveLogDestination);

        return liveLogDestination;
    }

    private static String followerLogChannel(final String logChannel, final int sessionId, final String tags)
    {
        final ChannelUri channelUri = ChannelUri.parse(logChannel);
        channelUri.remove(CommonContext.MDC_CONTROL_PARAM_NAME);
        channelUri.put(CommonContext.MDC_CONTROL_MODE_PARAM_NAME, CommonContext.MDC_CONTROL_MODE_MANUAL);
        channelUri.put(CommonContext.GROUP_PARAM_NAME, "true");
        channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(sessionId));
        channelUri.put(CommonContext.TAGS_PARAM_NAME, tags);
        channelUri.put(CommonContext.ALIAS_PARAM_NAME, "log");

        return channelUri.toString();
    }

    private static ChannelUri followerLogDestination(final String logChannel, final String logEndpoint)
    {
        final ChannelUri channelUri = ChannelUri.parse(logChannel);
        channelUri.remove(CommonContext.MDC_CONTROL_PARAM_NAME);
        channelUri.put(CommonContext.ENDPOINT_PARAM_NAME, logEndpoint);

        return channelUri;
    }

    private void state(final State newState, final long nowNs)
    {
        stateChange(state, newState, thisMember.id());

        if (State.CANVASS == state)
        {
            isExtendedCanvass = false;
        }

        if (State.CANVASS == newState)
        {
            resetCatchup();
        }

        switch (newState)
        {
            case INIT:
            case CANVASS:
            case NOMINATE:
            case FOLLOWER_BALLOT:
            case FOLLOWER_CATCHUP_TRANSITION:
            case FOLLOWER_CATCHUP:
            case FOLLOWER_REPLAY:
            case FOLLOWER_TRANSITION:
            case FOLLOWER_READY:
                consensusModuleAgent.role(Cluster.Role.FOLLOWER);
                break;

            case CANDIDATE_BALLOT:
                consensusModuleAgent.role(Cluster.Role.CANDIDATE);
                break;

            case LEADER_TRANSITION:
            case LEADER_READY:
                consensusModuleAgent.role(Cluster.Role.LEADER);
                break;
        }

        state = newState;
        ctx.electionStateCounter().setOrdered(newState.code());
        timeOfLastStateChangeNs = nowNs;
    }

    private void resetCatchup()
    {
        consensusModuleAgent.stopAllCatchups();
        catchupPosition = NULL_POSITION;

        ClusterMember.reset(clusterMembers);
        thisMember.leadershipTermId(leadershipTermId).logPosition(logPosition);
    }

    private void cleanupReplay()
    {
        if (null != logReplay)
        {
            logReplay.close();
            logReplay = null;
            hasReplay = false;
        }
    }

    private boolean isPassiveMember()
    {
        return null == ClusterMember.findMember(clusterMembers, thisMember.id());
    }

    @SuppressWarnings("unused")
    void stateChange(final State oldState, final State newState, final int memberId)
    {
        //System.out.println("Election: memberId=" + memberId + " " + oldState + " -> " + newState +
        //    " leadershipTermId=" + leadershipTermId + " logPosition=" + logPosition);
    }
}
