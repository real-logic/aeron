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
    }

    private boolean isNodeStartup;
    private boolean isLeaderStartup;
    private boolean isExtendedCanvass;
    private boolean shouldReplay;
    private final ClusterMember[] clusterMembers;
    private final ClusterMember thisMember;
    private final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap;
    private final MemberStatusAdapter memberStatusAdapter;
    private final MemberStatusPublisher memberStatusPublisher;
    private final ConsensusModule.Context ctx;
    private final ConsensusModuleAgent consensusModuleAgent;
    private final Random random;

    private long timeOfLastStateChangeNs;
    private long timeOfLastUpdateNs;
    private long nominationDeadlineNs;
    private long nowNs;
    private long logPosition;
    private long catchupLogPosition = NULL_POSITION;
    private long leadershipTermId;
    private long logLeadershipTermId;
    private long candidateTermId = NULL_VALUE;
    private int logSessionId = CommonContext.NULL_SESSION_ID;
    private ClusterMember leaderMember = null;
    private State state = State.INIT;
    private Subscription logSubscription;
    private String liveLogDestination;
    private LogReplay logReplay = null;

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
        this.shouldReplay = isNodeStartup;
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
        this.nowNs = nowNs;
        int workCount = State.INIT == state ? init() : 0;
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
            state(State.INIT);
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
                final long timestamp = ctx.recordingLog().getTermTimestamp(leadershipTermId);
                if (this.logLeadershipTermId == logLeadershipTermId)
                {
                    publishNewLeadershipTerm(follower.publication(), leadershipTermId, timestamp);
                }
                else
                {
                    memberStatusPublisher.newLeadershipTerm(
                        follower.publication(),
                        logLeadershipTermId,
                        leadershipTermId,
                        this.logPosition,
                        timestamp,
                        thisMember.id(),
                        logSessionId,
                        isLeaderStartup);
                }
            }
            else if (State.CANVASS != state && logLeadershipTermId > leadershipTermId)
            {
                state(State.CANVASS);
            }
        }
    }

    void onRequestVote(
        final long logLeadershipTermId, final long logPosition, final long candidateTermId, final int candidateId)
    {
        if (isPassiveMember())
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
            state(State.CANVASS);

            placeVote(candidateTermId, candidateId, false);
        }
        else
        {
            this.candidateTermId = candidateTermId;
            ctx.clusterMarkFile().candidateTermId(candidateTermId, ctx.fileSyncLevel());
            state(State.FOLLOWER_BALLOT);

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
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final int leaderMemberId,
        final int logSessionId,
        final boolean isStartup)
    {
        final ClusterMember leader = clusterMemberByIdMap.get(leaderMemberId);
        if (null == leader)
        {
            return;
        }

        leaderMember = leader;
        this.isLeaderStartup = isStartup;

        if (this.logPosition > logPosition && logLeadershipTermId == this.logLeadershipTermId)
        {
            this.leadershipTermId = leadershipTermId;
            this.logSessionId = logSessionId;
            consensusModuleAgent.truncateLogEntry(logLeadershipTermId, logPosition);
            consensusModuleAgent.prepareForNewLeadership(logPosition);
            this.logPosition = logPosition;
            state(State.FOLLOWER_REPLAY);
        }
        else if (leadershipTermId == candidateTermId &&
            (State.FOLLOWER_BALLOT == state || State.CANDIDATE_BALLOT == state || State.CANVASS == state))
        {
            this.leadershipTermId = leadershipTermId;
            this.logSessionId = logSessionId;
            catchupLogPosition = logPosition;
            state(State.FOLLOWER_REPLAY);
        }
        else if (0 != compareLog(this.logLeadershipTermId, this.logPosition, logLeadershipTermId, logPosition))
        {
            if (NULL_POSITION == catchupLogPosition)
            {
                if (this.logPosition <= logPosition)
                {
                    this.leadershipTermId = leadershipTermId;
                    this.logSessionId = logSessionId;
                    candidateTermId = leadershipTermId;
                    catchupLogPosition = logPosition;
                    state(State.FOLLOWER_REPLAY);
                }
            }
        }
    }

    void onAppendPosition(final long leadershipTermId, final long logPosition, final int followerMemberId)
    {
        final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);

        if (null != follower)
        {
            follower
                .logPosition(logPosition)
                .leadershipTermId(leadershipTermId)
                .timeOfLastAppendPositionNs(nowNs);

            consensusModuleAgent.trackCatchupCompletion(follower);
        }
    }

    @SuppressWarnings("unused")
    void onCommitPosition(final long leadershipTermId, final long logPosition, final int leaderMemberId)
    {
        if (State.FOLLOWER_BALLOT == state && leadershipTermId > this.leadershipTermId)
        {
            if (this.logPosition > logPosition)
            {
                consensusModuleAgent.truncateLogEntry(logLeadershipTermId, logPosition);
            }
            else
            {
                catchupLogPosition = logPosition;
                state(State.FOLLOWER_REPLAY);
            }
        }
        else if (State.FOLLOWER_CATCHUP == state && NULL_POSITION != catchupLogPosition)
        {
            catchupLogPosition = Math.max(catchupLogPosition, logPosition);
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

    private int init()
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
            leaderMember = thisMember;
            state(State.LEADER_REPLAY);
        }
        else
        {
            state(State.CANVASS);
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
            state(State.NOMINATE);
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
            state(State.CANDIDATE_BALLOT);
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
            state(State.LEADER_REPLAY);
            workCount += 1;
        }
        else if (nowNs >= (timeOfLastStateChangeNs + ctx.electionTimeoutNs()))
        {
            if (ClusterMember.hasMajorityVote(clusterMembers, candidateTermId))
            {
                leaderMember = thisMember;
                state(State.LEADER_REPLAY);
            }
            else
            {
                state(State.CANVASS);
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
                        member.publication(), leadershipTermId, logPosition, candidateTermId, thisMember.id()));
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
            state(State.CANVASS);
            workCount += 1;
        }

        return workCount;
    }

    private int leaderReplay(final long nowNs)
    {
        int workCount = 0;

        if (null == logReplay)
        {
            consensusModuleAgent.notifiedCommitPosition(logPosition);
            logSessionId = consensusModuleAgent.addNewLogPublication().sessionId();

            ClusterMember.resetLogPositions(clusterMembers, NULL_POSITION);
            thisMember.logPosition(logPosition).leadershipTermId(candidateTermId);

            if (!shouldReplay || (logReplay = consensusModuleAgent.newLogReplay(logPosition)) == null)
            {
                shouldReplay = false;
                state(State.LEADER_TRANSITION);
                workCount = 1;
            }
        }
        else
        {
            workCount += logReplay.doWork(nowNs);
            if (logReplay.isDone())
            {
                cleanupReplay();
                state(State.LEADER_TRANSITION);
            }
            else if (nowNs > (timeOfLastUpdateNs + ctx.leaderHeartbeatIntervalNs()))
            {
                timeOfLastUpdateNs = nowNs;
                final long timestamp = ctx.clusterClock().timeUnit().convert(nowNs, TimeUnit.NANOSECONDS);

                for (final ClusterMember member : clusterMembers)
                {
                    if (member != thisMember)
                    {
                        publishNewLeadershipTerm(member.publication(), candidateTermId, timestamp);
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
        consensusModuleAgent.becomeLeader(candidateTermId, logPosition, logSessionId, isLeaderStartup);

        final long recordingId = consensusModuleAgent.logRecordingId();
        final long timestamp = ctx.clusterClock().timeUnit().convert(nowNs, TimeUnit.NANOSECONDS);
        final RecordingLog recordingLog = ctx.recordingLog();

        for (long termId = leadershipTermId + 1; termId <= candidateTermId; termId++)
        {
            recordingLog.appendTerm(recordingId, termId, logPosition, timestamp);
        }

        recordingLog.force(ctx.fileSyncLevel());
        leadershipTermId = candidateTermId;

        state(State.LEADER_READY);

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
                state(State.CLOSED);
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

        final State nextState = NULL_POSITION != catchupLogPosition ?
            State.FOLLOWER_CATCHUP_TRANSITION : State.FOLLOWER_TRANSITION;

        if (null == logReplay)
        {
            if (!shouldReplay || (logReplay = consensusModuleAgent.newLogReplay(logPosition)) == null)
            {
                shouldReplay = false;
                state(nextState);
                workCount = 1;
            }
        }
        else
        {
            workCount += logReplay.doWork(nowNs);
            if (logReplay.isDone())
            {
                cleanupReplay();
                state(nextState);
            }
        }

        return workCount;
    }

    private int followerCatchupTransition(final long nowNs)
    {
        if (null == logSubscription)
        {
            final ChannelUri logChannelUri = followerLogChannel(
                ctx.logChannel(), logSessionId, consensusModuleAgent.logSubscriptionTags());

            logSubscription = consensusModuleAgent.createAndRecordLogSubscriptionAsFollower(logChannelUri.toString());
            consensusModuleAgent.awaitServicesReady(logChannelUri, logSessionId, logPosition, isLeaderStartup);

            final String replayDestination = new ChannelUriStringBuilder()
                .media(CommonContext.UDP_MEDIA)
                .endpoint(thisMember.transferEndpoint())
                .build();

            logSubscription.asyncAddDestination(replayDestination);
            consensusModuleAgent.replayLogDestination(replayDestination);
        }

        if (catchupPosition(leadershipTermId, logPosition))
        {
            timeOfLastUpdateNs = nowNs;
            consensusModuleAgent.catchupInitiated(nowNs);
            state(State.FOLLOWER_CATCHUP);
        }

        return 1;
    }

    private int followerCatchup(final long nowNs)
    {
        int workCount = 0;

        consensusModuleAgent.catchupLogPoll(logSubscription, logSessionId, catchupLogPosition, nowNs);

        if (null == liveLogDestination &&
            consensusModuleAgent.hasAppendReachedLivePosition(logSubscription, logSessionId, catchupLogPosition))
        {
            addLiveLogDestination();
        }

        if (consensusModuleAgent.hasAppendReachedPosition(logSubscription, logSessionId, catchupLogPosition))
        {
            logPosition = catchupLogPosition;
            timeOfLastUpdateNs = 0;
            state(State.FOLLOWER_TRANSITION);
            workCount += 1;
        }
        else if (nowNs > (timeOfLastUpdateNs + ctx.leaderHeartbeatIntervalNs()))
        {
            if (consensusModuleAgent.hasCatchupStalled(nowNs, ctx.leaderHeartbeatTimeoutNs()))
            {
                ctx.countedErrorHandler().onError(new ClusterException("no catchup progress", WARN));
                logPosition = ctx.commitPositionCounter().get();
                state(State.INIT);
                workCount += 1;
            }
            else if (consensusModuleAgent.hasReplayDestination() && catchupPosition(leadershipTermId, logPosition))
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
            final ChannelUri logChannelUri = followerLogChannel(
                ctx.logChannel(), logSessionId, consensusModuleAgent.logSubscriptionTags());

            logSubscription = consensusModuleAgent.createAndRecordLogSubscriptionAsFollower(logChannelUri.toString());
            consensusModuleAgent.awaitServicesReady(logChannelUri, logSessionId, logPosition, isLeaderStartup);
        }

        if (null == liveLogDestination)
        {
            addLiveLogDestination();
        }

        consensusModuleAgent.awaitImageAndCreateFollowerLogAdapter(logSubscription, logSessionId);

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

        state(State.FOLLOWER_READY);

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
                state(State.CLOSED);
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

            state(State.CANVASS);
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
            leadershipTermId,
            logPosition,
            timestamp,
            thisMember.id(),
            logSessionId,
            isLeaderStartup);
    }

    private boolean catchupPosition(final long leadershipTermId, final long logPosition)
    {
        return memberStatusPublisher.catchupPosition(
            leaderMember.publication(), leadershipTermId, logPosition, thisMember.id());
    }

    private void addLiveLogDestination()
    {
        final ChannelUri channelUri = followerLogDestination(ctx.logChannel(), thisMember.logEndpoint());
        liveLogDestination = channelUri.toString();

        logSubscription.asyncAddDestination(liveLogDestination);
        consensusModuleAgent.liveLogDestination(liveLogDestination);
    }

    private static ChannelUri followerLogChannel(final String logChannel, final int sessionId, final String tags)
    {
        final ChannelUri channelUri = ChannelUri.parse(logChannel);
        channelUri.remove(CommonContext.MDC_CONTROL_PARAM_NAME);
        channelUri.put(CommonContext.MDC_CONTROL_MODE_PARAM_NAME, CommonContext.MDC_CONTROL_MODE_MANUAL);
        channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(sessionId));
        channelUri.put(CommonContext.TAGS_PARAM_NAME, tags);
        channelUri.put(CommonContext.ALIAS_PARAM_NAME, "log");

        return channelUri;
    }

    private static ChannelUri followerLogDestination(final String logChannel, final String logEndpoint)
    {
        final ChannelUri channelUri = ChannelUri.parse(logChannel);
        channelUri.remove(CommonContext.MDC_CONTROL_PARAM_NAME);
        channelUri.put(CommonContext.ENDPOINT_PARAM_NAME, logEndpoint);

        return channelUri;
    }

    private void state(final State newState)
    {
        stateChange(state, newState, thisMember.id());

        if (State.CANVASS == state)
        {
            isExtendedCanvass = false;
        }

        if (State.CANVASS == newState)
        {
            resetCatchupAndLogPosition();
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

    private void resetCatchupAndLogPosition()
    {
        consensusModuleAgent.stopAllCatchups();
        catchupLogPosition = NULL_POSITION;

        ClusterMember.reset(clusterMembers);
        thisMember.leadershipTermId(leadershipTermId).logPosition(logPosition);
    }

    private void cleanupReplay()
    {
        if (null != logReplay)
        {
            logReplay.close();
            logReplay = null;
            shouldReplay = false;
        }
    }

    private boolean isPassiveMember()
    {
        return null == ClusterMember.findMember(clusterMembers, thisMember.id());
    }

    @SuppressWarnings("unused")
    void stateChange(final State oldState, final State newState, final int memberId)
    {
        //System.out.println("Election: memberId=" + memberId + " " + oldState + " -> " + newState);
    }
}
