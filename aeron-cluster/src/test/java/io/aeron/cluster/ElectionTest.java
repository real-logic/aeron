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

import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;
import org.agrona.concurrent.CachedEpochClock;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.Election.NOMINATION_TIMEOUT_MULTIPLIER;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@SuppressWarnings("MethodLength")
public class ElectionTest
{
    public static final long RECORDING_ID = 1L;
    private final Aeron aeron = mock(Aeron.class);
    private final Counter electionStateCounter = mock(Counter.class);
    private final RecordingLog recordingLog = mock(RecordingLog.class);
    private final ClusterMarkFile clusterMarkFile = mock(ClusterMarkFile.class);
    private final MemberStatusAdapter memberStatusAdapter = mock(MemberStatusAdapter.class);
    private final MemberStatusPublisher memberStatusPublisher = mock(MemberStatusPublisher.class);
    private final ConsensusModuleAgent consensusModuleAgent = mock(ConsensusModuleAgent.class);

    private final ConsensusModule.Context ctx = new ConsensusModule.Context()
        .epochClock(new CachedEpochClock())
        .aeron(aeron)
        .recordingLog(recordingLog)
        .random(new Random())
        .clusterMarkFile(clusterMarkFile);

    private final long electionStatusIntervalMs = TimeUnit.NANOSECONDS.toMillis(ctx.electionStatusIntervalNs());
    private final long electionTimeoutMs = TimeUnit.NANOSECONDS.toMillis(ctx.electionTimeoutNs());
    private final long startupCanvassTimeoutMs = TimeUnit.NANOSECONDS.toMillis(ctx.startupCanvassTimeoutNs());

    @Before
    public void before()
    {
        when(aeron.addCounter(anyInt(), anyString())).thenReturn(electionStateCounter);
        when(consensusModuleAgent.logRecordingId()).thenReturn(RECORDING_ID);
        when(clusterMarkFile.candidateTermId()).thenReturn((long)Aeron.NULL_VALUE);
    }

    @Test
    public void shouldElectSingleNodeClusterLeader()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = ClusterMember.parse(
            "0,clientEndpoint,memberEndpoint,logEndpoint,transferEndpoint,archiveEndpoint");

        final ClusterMember thisMember = clusterMembers[0];
        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, thisMember);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 0;
        election.doWork(t1);
        election.doWork(t1);
        verify(consensusModuleAgent).becomeLeader(anyLong(), anyInt());
        verify(recordingLog).appendTerm(RECORDING_ID, 0L, 0L, t1);
        assertThat(election.state(), is(Election.State.LEADER_READY));
    }

    @Test
    public void shouldElectAppointedLeader()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[0];
        final CachedEpochClock clock = (CachedEpochClock)ctx.epochClock();

        ctx.appointedLeaderId(candidateMember.id());

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember);

        assertThat(election.state(), is(Election.State.INIT));

        final long candidateTermId = leadershipTermId + 1;
        final long t1 = 1;
        clock.update(t1);
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onCanvassPosition(leadershipTermId, 0, 1);
        election.onCanvassPosition(leadershipTermId, 0, 2);

        final long t2 = 2;
        clock.update(t2);
        election.doWork(t2);
        assertThat(election.state(), is(Election.State.NOMINATE));

        final long t3 = t2 + electionStatusIntervalMs * NOMINATION_TIMEOUT_MULTIPLIER;
        clock.update(t3);
        election.doWork(t3);
        election.doWork(t3);
        verify(memberStatusPublisher).requestVote(
            clusterMembers[1].publication(),
            leadershipTermId,
            logPosition,
            candidateTermId,
            candidateMember.id());
        verify(memberStatusPublisher).requestVote(
            clusterMembers[2].publication(),
            leadershipTermId,
            logPosition,
            candidateTermId,
            candidateMember.id());
        assertThat(election.state(), is(Election.State.CANDIDATE_BALLOT));
        verify(consensusModuleAgent).role(Cluster.Role.CANDIDATE);

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(
            candidateTermId, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[1].id(), true);
        election.onVote(
            candidateTermId, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[2].id(), true);

        final long t4 = t3 + 1;
        clock.update(t3);
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.LEADER_REPLAY));

        final long t5 = t4 + 1;
        clock.update(t5);
        election.doWork(t5);
        election.doWork(t5);
        verify(consensusModuleAgent).becomeLeader(anyLong(), anyInt());
        verify(recordingLog).appendTerm(RECORDING_ID, 0L, 0L, t5);

        assertThat(clusterMembers[1].logPosition(), is(NULL_POSITION));
        assertThat(clusterMembers[2].logPosition(), is(NULL_POSITION));
        assertThat(election.state(), is(Election.State.LEADER_READY));
        assertThat(election.leadershipTermId(), is(candidateTermId));

        final long t6 = t5 + TimeUnit.NANOSECONDS.toMillis(ctx.leaderHeartbeatIntervalNs());
        clock.update(t6);
        final int logSessionId = -7;
        election.logSessionId(logSessionId);
        election.doWork(t6);
        verify(memberStatusPublisher).newLeadershipTerm(
            clusterMembers[1].publication(),
            leadershipTermId,
            logPosition,
            candidateTermId,
            candidateMember.id(),
            logSessionId);
        verify(memberStatusPublisher).newLeadershipTerm(
            clusterMembers[2].publication(),
            leadershipTermId,
            logPosition,
            candidateTermId,
            candidateMember.id(),
            logSessionId);
        assertThat(election.state(), is(Election.State.LEADER_READY));

        when(consensusModuleAgent.electionComplete(anyLong())).thenReturn(true);

        final long t7 = t6 + 1;
        clock.update(t7);
        election.onAppendedPosition(candidateTermId, 0, clusterMembers[1].id());
        election.onAppendedPosition(candidateTermId, 0, clusterMembers[2].id());
        election.doWork(t7);
        final InOrder inOrder = inOrder(consensusModuleAgent, electionStateCounter);
        inOrder.verify(consensusModuleAgent).electionComplete(t7);
        inOrder.verify(electionStateCounter).close();
    }

    @Test
    public void shouldVoteForAppointedLeader()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final int candidateId = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];
        final CachedEpochClock clock = (CachedEpochClock)ctx.epochClock();

        ctx.appointedLeaderId(candidateId).epochClock(clock);

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        final long candidateTermId = leadershipTermId + 1;
        final long t2 = 2;
        clock.update(t2);
        election.onRequestVote(leadershipTermId, logPosition, candidateTermId, candidateId);
        verify(memberStatusPublisher).placeVote(
            clusterMembers[candidateId].publication(),
            candidateTermId,
            leadershipTermId,
            logPosition,
            candidateId,
            followerMember.id(),
            true);
        election.doWork(t2);
        assertThat(election.state(), is(Election.State.FOLLOWER_BALLOT));

        final int logSessionId = -7;
        election.onNewLeadershipTerm(leadershipTermId, logPosition, candidateTermId, candidateId, logSessionId);
        assertThat(election.state(), is(Election.State.FOLLOWER_REPLAY));

        when(consensusModuleAgent.createAndRecordLogSubscriptionAsFollower(anyString(), anyLong()))
            .thenReturn(mock(Subscription.class));
        final long t3 = 3;
        election.doWork(t3);
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.FOLLOWER_READY));

        when(memberStatusPublisher.appendedPosition(any(), anyLong(), anyLong(), anyInt())).thenReturn(Boolean.TRUE);
        when(consensusModuleAgent.electionComplete(anyLong())).thenReturn(true);

        final long t4 = 4;
        election.doWork(t4);
        final InOrder inOrder = inOrder(memberStatusPublisher, consensusModuleAgent, electionStateCounter);
        inOrder.verify(memberStatusPublisher).appendedPosition(
            clusterMembers[candidateId].publication(), candidateTermId, 0, followerMember.id());
        inOrder.verify(consensusModuleAgent).electionComplete(t4);
        inOrder.verify(electionStateCounter).close();
    }

    @Test
    public void shouldCanvassMembersForSuccessfulLeadershipBid()
    {
        final long logPosition = 0;
        final long leadershipTermId = Aeron.NULL_VALUE;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        final long t2 = t1 + electionStatusIntervalMs;
        election.doWork(t2);
        verify(memberStatusPublisher).canvassPosition(
            clusterMembers[0].publication(), leadershipTermId, logPosition, followerMember.id());
        verify(memberStatusPublisher).canvassPosition(
            clusterMembers[2].publication(), leadershipTermId, logPosition, followerMember.id());
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onCanvassPosition(leadershipTermId, 0, 0);
        election.onCanvassPosition(leadershipTermId, 0, 2);

        final long t3 = t2 + 1;
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.NOMINATE));
    }

    @Test
    public void shouldCanvassMembersForUnSuccessfulLeadershipBid()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[0];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        final long t2 = t1 + electionStatusIntervalMs;
        election.doWork(t2);
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onCanvassPosition(leadershipTermId + 1, 0, 1);
        election.onCanvassPosition(leadershipTermId, 0, 2);

        final long t3 = t2 + 1;
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.CANVASS));
    }

    @Test
    public void shouldVoteForCandidateDuringNomination()
    {
        final long logPosition = 0;
        final long leadershipTermId = Aeron.NULL_VALUE;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        final long t2 = t1 + electionStatusIntervalMs;
        election.doWork(t2);
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onCanvassPosition(leadershipTermId, 0, 0);
        election.onCanvassPosition(leadershipTermId, 0, 2);

        final long t3 = t2 + 1;
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.NOMINATE));

        final long t4 = t3 + 1;
        final long candidateTermId = leadershipTermId + 1;
        election.onRequestVote(leadershipTermId, logPosition, candidateTermId, 0);
        election.doWork(t4);
        assertThat(election.state(), is(Election.State.FOLLOWER_BALLOT));
    }

    @Test
    public void shouldTimeoutCanvassWithMajority()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onAppendedPosition(leadershipTermId, 0, 0);
        assertThat(election.state(), is(Election.State.CANVASS));

        final long t2 = t1 + 1;
        election.doWork(t2);
        assertThat(election.state(), is(Election.State.CANVASS));

        final long t3 = t2 + startupCanvassTimeoutMs;
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.NOMINATE));
    }

    @Test
    public void shouldWinCandidateBallotWithMajority()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[1];

        final Election election = newElection(false, leadershipTermId, logPosition, clusterMembers, candidateMember);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onCanvassPosition(leadershipTermId, 0, 0);
        election.onCanvassPosition(leadershipTermId, 0, 2);

        final long t2 = t1 + 1;
        election.doWork(t2);
        assertThat(election.state(), is(Election.State.NOMINATE));

        final long t3 = t2 + electionStatusIntervalMs * NOMINATION_TIMEOUT_MULTIPLIER;
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.CANDIDATE_BALLOT));

        final long t4 = t3 + electionTimeoutMs;
        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(
            leadershipTermId + 1, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[2].id(), true);
        election.doWork(t4);
        assertThat(election.state(), is(Election.State.LEADER_REPLAY));
    }

    @Test
    public void shouldElectCandidateWithFullVote()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onCanvassPosition(leadershipTermId, 0, 0);
        election.onCanvassPosition(leadershipTermId, 0, 2);

        final long t2 = t1 + 1;
        election.doWork(t2);
        assertThat(election.state(), is(Election.State.NOMINATE));

        final long t3 = t2 + electionStatusIntervalMs * NOMINATION_TIMEOUT_MULTIPLIER;
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.CANDIDATE_BALLOT));

        final long t4 = t3 + 1;
        final long candidateTermId = leadershipTermId + 1;
        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(
            candidateTermId, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[0].id(), false);
        election.onVote(
            candidateTermId, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[2].id(), true);
        election.doWork(t4);
        assertThat(election.state(), is(Election.State.LEADER_REPLAY));
    }

    @Test
    public void shouldTimeoutCandidateBallotWithoutMajority()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onCanvassPosition(leadershipTermId, 0, 0);
        election.onCanvassPosition(leadershipTermId, 0, 2);

        final long t2 = t1 + 1;
        election.doWork(t2);
        assertThat(election.state(), is(Election.State.NOMINATE));

        final long t3 = t2 + electionStatusIntervalMs * NOMINATION_TIMEOUT_MULTIPLIER;
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.CANDIDATE_BALLOT));

        final long t4 = t3 + electionTimeoutMs;
        election.doWork(t4);
        assertThat(election.state(), is(Election.State.CANVASS));
        assertThat(election.leadershipTermId(), is(leadershipTermId));
        assertThat(election.candidateTermId(), is(leadershipTermId + 1));
    }

    @Test
    public void shouldTimeoutFailedCandidateBallotOnSplitVoteThenSucceedOnRetry()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onCanvassPosition(leadershipTermId, 0, 0);

        final long t2 = t1 + startupCanvassTimeoutMs;
        election.doWork(t2);
        assertThat(election.state(), is(Election.State.NOMINATE));

        final long t3 = t2 + electionStatusIntervalMs * NOMINATION_TIMEOUT_MULTIPLIER;
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.CANDIDATE_BALLOT));

        final long t4 = t3 + 1;
        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(
            leadershipTermId + 1, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[2].id(), false);
        election.doWork(t4);
        assertThat(election.state(), is(Election.State.CANDIDATE_BALLOT));

        final long t5 = t4 + electionTimeoutMs;
        election.doWork(t5);
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onCanvassPosition(leadershipTermId, 0, 0);

        final long t6 = t5 + 1;
        election.doWork(t6);

        final long t7 = t6 + electionTimeoutMs;
        election.doWork(t7);
        assertThat(election.state(), is(Election.State.NOMINATE));

        final long t8 = t7 + electionTimeoutMs;
        election.doWork(t8);
        assertThat(election.state(), is(Election.State.CANDIDATE_BALLOT));

        final long t9 = t8 + 1;
        election.doWork(t9);

        final long candidateTermId = leadershipTermId + 2;
        election.onVote(
            candidateTermId, leadershipTermId + 1, logPosition, candidateMember.id(), clusterMembers[2].id(), true);

        final long t10 = t9 + electionTimeoutMs;
        election.doWork(t10);
        assertThat(election.state(), is(Election.State.LEADER_REPLAY));

        final long t11 = t10 + 1;
        election.doWork(t11);
        election.doWork(t11);
        assertThat(election.state(), is(Election.State.LEADER_READY));
        assertThat(election.leadershipTermId(), is(candidateTermId));
    }

    @Test
    public void shouldTimeoutFollowerBallotWithoutLeaderEmerging()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0L;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        final long candidateTermId = leadershipTermId + 1;
        election.onRequestVote(leadershipTermId, logPosition, candidateTermId, 0);

        final long t2 = t1 + 1;
        election.doWork(t2);
        assertThat(election.state(), is(Election.State.FOLLOWER_BALLOT));

        final long t3 = t2 + electionTimeoutMs;
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.CANVASS));
        assertThat(election.leadershipTermId(), is(leadershipTermId));
    }

    @Test
    public void shouldBecomeFollowerIfEnteringNewElection()
    {
        final long leadershipTermId = 1;
        final long logPosition = 120;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember thisMember = clusterMembers[0];

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.LEADER);
        final Election election = newElection(false, leadershipTermId, logPosition, clusterMembers, thisMember);

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));
        verify(consensusModuleAgent).prepareForElection(logPosition);
        verify(consensusModuleAgent).role(Cluster.Role.FOLLOWER);
    }

    private Election newElection(
        final boolean isStartup,
        final long logLeadershipTermId,
        final long logPosition,
        final ClusterMember[] clusterMembers,
        final ClusterMember thisMember)
    {
        return new Election(
            isStartup,
            logLeadershipTermId,
            logPosition,
            clusterMembers,
            thisMember,
            memberStatusAdapter,
            memberStatusPublisher,
            ctx,
            consensusModuleAgent);
    }

    private Election newElection(
        final long logLeadershipTermId,
        final long logPosition,
        final ClusterMember[] clusterMembers,
        final ClusterMember thisMember)
    {
        return new Election(
            true,
            logLeadershipTermId,
            logPosition,
            clusterMembers,
            thisMember,
            memberStatusAdapter,
            memberStatusPublisher,
            ctx,
            consensusModuleAgent);
    }

    private static ClusterMember[] prepareClusterMembers()
    {
        final ClusterMember[] clusterMembers = ClusterMember.parse(
            "0,clientEndpoint,memberEndpoint,logEndpoint,transferEndpoint,archiveEndpoint|" +
            "1,clientEndpoint,memberEndpoint,logEndpoint,transferEndpoint,archiveEndpoint|" +
            "2,clientEndpoint,memberEndpoint,logEndpoint,transferEndpoint,archiveEndpoint|");

        clusterMembers[0].publication(mock(Publication.class));
        clusterMembers[1].publication(mock(Publication.class));
        clusterMembers[2].publication(mock(Publication.class));

        return clusterMembers;
    }
}