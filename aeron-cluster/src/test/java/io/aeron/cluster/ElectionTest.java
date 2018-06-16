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
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class ElectionTest
{
    public static final long RECORDING_ID = 1L;
    private final Aeron aeron = mock(Aeron.class);
    private final Counter electionStateCounter = mock(Counter.class);
    private final RecordingLog recordingLog = mock(RecordingLog.class);
    private final ClusterMarkFile clusterMarkFile = mock(ClusterMarkFile.class);
    private final MemberStatusAdapter memberStatusAdapter = mock(MemberStatusAdapter.class);
    private final MemberStatusPublisher memberStatusPublisher = mock(MemberStatusPublisher.class);
    private final SequencerAgent sequencerAgent = mock(SequencerAgent.class);

    private final ConsensusModule.Context ctx = new ConsensusModule.Context()
        .epochClock(new CachedEpochClock())
        .aeron(aeron)
        .recordingLog(recordingLog)
        .random(new Random())
        .clusterMarkFile(clusterMarkFile);

    @Before
    public void before()
    {
        when(aeron.addCounter(anyInt(), anyString())).thenReturn(electionStateCounter);
        when(sequencerAgent.logRecordingId()).thenReturn(RECORDING_ID);
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
        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, thisMember, ctx);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 0;
        election.doWork(t1);
        verify(sequencerAgent).becomeLeader();
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

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember, ctx);

        assertThat(election.state(), is(Election.State.INIT));

        final long candidateTermId = leadershipTermId + 1;
        final long t1 = 1;
        clock.update(t1);
        election.doWork(t1);
        verify(sequencerAgent).role(Cluster.Role.CANDIDATE);
        assertThat(election.state(), is(Election.State.CANDIDATE_BALLOT));

        final long t2 = 2;
        clock.update(t2);
        election.doWork(t2);
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

        when(sequencerAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(candidateTermId, candidateMember.id(), clusterMembers[1].id(), true);
        election.onVote(candidateTermId, candidateMember.id(), clusterMembers[2].id(), true);

        final long t3 = 3;
        clock.update(t3);
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.LEADER_TRANSITION));

        final long t4 = 4;
        clock.update(t4);
        election.doWork(t4);
        verify(sequencerAgent).becomeLeader();
        verify(recordingLog).appendTerm(RECORDING_ID, 0L, 0L, t4);

        assertThat(clusterMembers[1].logPosition(), is(NULL_POSITION));
        assertThat(clusterMembers[2].logPosition(), is(NULL_POSITION));
        assertThat(election.state(), is(Election.State.LEADER_READY));
        assertThat(election.leadershipTermId(), is(candidateTermId));

        final long t5 = t1 + TimeUnit.NANOSECONDS.toMillis(ctx.leaderHeartbeatIntervalNs());
        clock.update(t5);
        final int logSessionId = -7;
        election.logSessionId(logSessionId);
        election.doWork(t5);
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

        final long t6 = t5 + 1;
        clock.update(t6);
        election.onAppendedPosition(candidateTermId, 0, clusterMembers[1].id());
        election.onAppendedPosition(candidateTermId, 0, clusterMembers[2].id());
        election.doWork(t6);
        final InOrder inOrder = inOrder(sequencerAgent, electionStateCounter);
        inOrder.verify(sequencerAgent).electionComplete();
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

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember, ctx);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        final long candidateTermId = leadershipTermId + 1;
        final long t2 = 2;
        clock.update(t2);
        election.onRequestVote(leadershipTermId, logPosition, candidateTermId, candidateId);
        verify(memberStatusPublisher).placeVote(
            clusterMembers[candidateId].publication(), candidateTermId, candidateId, followerMember.id(), true);
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.FOLLOWER_BALLOT));

        final int logSessionId = -7;
        election.onNewLeadershipTerm(leadershipTermId, logPosition, candidateTermId, candidateId, logSessionId);
        assertThat(election.state(), is(Election.State.FOLLOWER_TRANSITION));

        when(sequencerAgent.createAndRecordLogSubscriptionAsFollower(anyString(), anyLong()))
            .thenReturn(mock(Subscription.class));
        final long t3 = 3;
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.FOLLOWER_READY));

        when(memberStatusPublisher.appendedPosition(any(), anyLong(), anyLong(), anyInt())).thenReturn(Boolean.TRUE);

        final long t4 = 4;
        election.doWork(t4);
        final InOrder inOrder = inOrder(memberStatusPublisher, sequencerAgent, electionStateCounter);
        inOrder.verify(memberStatusPublisher).appendedPosition(
            clusterMembers[candidateId].publication(), candidateTermId, 0, followerMember.id());
        inOrder.verify(sequencerAgent).electionComplete();
        inOrder.verify(electionStateCounter).close();
    }

    @Test
    public void shouldCanvassMembersForSuccessfulLeadershipBid()
    {
        final long logPosition = 0;
        final long leaderShipTermId = Aeron.NULL_VALUE;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];

        final Election election = newElection(leaderShipTermId, logPosition, clusterMembers, followerMember, ctx);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        final long t2 = t1 + TimeUnit.NANOSECONDS.toMillis(ctx.statusIntervalNs());
        election.doWork(t2);
        verify(memberStatusPublisher).canvassPosition(
            clusterMembers[0].publication(), leaderShipTermId, logPosition, followerMember.id());
        verify(memberStatusPublisher).canvassPosition(
            clusterMembers[2].publication(), leaderShipTermId, logPosition, followerMember.id());
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onCanvassPosition(leaderShipTermId, 0, 0);
        election.onCanvassPosition(leaderShipTermId, 0, 2);

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

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember, ctx);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        final long t2 = t1 + TimeUnit.NANOSECONDS.toMillis(ctx.statusIntervalNs());
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

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember, ctx);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        final long t2 = t1 + TimeUnit.NANOSECONDS.toMillis(ctx.statusIntervalNs());
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

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember, ctx);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onAppendedPosition(leadershipTermId, 0, 0);
        assertThat(election.state(), is(Election.State.CANVASS));

        final long t2 = t1 + 1;
        election.doWork(t2);
        assertThat(election.state(), is(Election.State.CANVASS));

        final long t3 = t2 + TimeUnit.NANOSECONDS.toMillis(ctx.startupStatusTimeoutNs());
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.NOMINATE));
    }

    @Test
    public void shouldTimeoutCandidateBallotWithMajority()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember, ctx);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onCanvassPosition(leadershipTermId, 0, 0);
        election.onCanvassPosition(leadershipTermId, 0, 2);

        final long t2 = t1 + 1;
        election.doWork(t2);
        assertThat(election.state(), is(Election.State.NOMINATE));

        final long t3 = t2 + TimeUnit.NANOSECONDS.toMillis(ctx.statusIntervalNs());
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.CANDIDATE_BALLOT));

        final long t4 = t3 + 1;
        when(sequencerAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(leadershipTermId + 1, candidateMember.id(), clusterMembers[2].id(), true);
        election.doWork(t4);
        assertThat(election.state(), is(Election.State.CANDIDATE_BALLOT));

        final long t5 = t4 + TimeUnit.NANOSECONDS.toMillis(ctx.electionTimeoutNs());
        election.doWork(t5);
        assertThat(election.state(), is(Election.State.LEADER_TRANSITION));
    }

    @Test
    public void shouldElectCandidateWithFullVote()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember, ctx);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onCanvassPosition(leadershipTermId, 0, 0);
        election.onCanvassPosition(leadershipTermId, 0, 2);

        final long t2 = t1 + 1;
        election.doWork(t2);
        assertThat(election.state(), is(Election.State.NOMINATE));

        final long t3 = t2 + TimeUnit.NANOSECONDS.toMillis(ctx.statusIntervalNs());
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.CANDIDATE_BALLOT));

        final long t4 = t3 + 1;
        final long candidateTermId = leadershipTermId + 1;
        when(sequencerAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(candidateTermId, candidateMember.id(), clusterMembers[0].id(), false);
        election.onVote(candidateTermId, candidateMember.id(), clusterMembers[2].id(), true);
        election.doWork(t4);
        assertThat(election.state(), is(Election.State.LEADER_TRANSITION));
    }


    @Test
    public void shouldTimeoutCandidateBallotWithoutMajority()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember, ctx);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onCanvassPosition(leadershipTermId, 0, 0);
        election.onCanvassPosition(leadershipTermId, 0, 2);

        final long t2 = t1 + 1;
        election.doWork(t2);
        assertThat(election.state(), is(Election.State.NOMINATE));

        final long t3 = t2 + TimeUnit.NANOSECONDS.toMillis(ctx.statusIntervalNs());
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.CANDIDATE_BALLOT));

        final long t4 = t3 + TimeUnit.NANOSECONDS.toMillis(ctx.electionTimeoutNs());
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

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember, ctx);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onCanvassPosition(leadershipTermId, 0, 0);

        final long t2 = t1 + ctx.statusIntervalNs();
        election.doWork(t2);
        assertThat(election.state(), is(Election.State.NOMINATE));

        final long t3 = t2 + TimeUnit.NANOSECONDS.toMillis(ctx.statusIntervalNs());
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.CANDIDATE_BALLOT));

        final long t4 = t3 + 1;
        when(sequencerAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(leadershipTermId + 1, candidateMember.id(), clusterMembers[2].id(), false);
        election.doWork(t4);
        assertThat(election.state(), is(Election.State.CANDIDATE_BALLOT));

        final long t5 = t4 + TimeUnit.NANOSECONDS.toMillis(ctx.electionTimeoutNs());
        election.doWork(t5);
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onCanvassPosition(leadershipTermId, 0, 0);

        final long t6 = t5 + 1;
        election.doWork(t6);

        final long t7 = t6 + TimeUnit.NANOSECONDS.toMillis(ctx.electionTimeoutNs());
        election.doWork(t7);
        assertThat(election.state(), is(Election.State.NOMINATE));

        final long t8 = t7 + ctx.statusIntervalNs();
        election.doWork(t8);
        assertThat(election.state(), is(Election.State.CANDIDATE_BALLOT));

        final long t9 = t8 + 1;
        election.doWork(t9);

        final long candidateTermId = leadershipTermId + 2;
        election.onVote(candidateTermId, candidateMember.id(), clusterMembers[2].id(), true);

        final long t10 = t9 + TimeUnit.NANOSECONDS.toMillis(ctx.electionTimeoutNs());
        election.doWork(t10);
        assertThat(election.state(), is(Election.State.LEADER_TRANSITION));

        final long t11 = t10 + 1;
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

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember, ctx);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        final long candidateTermId = leadershipTermId + 1;
        election.onRequestVote(leadershipTermId, logPosition, candidateTermId, 0);

        final long t2 = t1 + 1;
        election.doWork(t2);
        assertThat(election.state(), is(Election.State.FOLLOWER_BALLOT));

        final long t3 = t2 + TimeUnit.NANOSECONDS.toMillis(ctx.electionTimeoutNs());
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.CANVASS));
        assertThat(election.leadershipTermId(), is(leadershipTermId));
    }

    private Election newElection(
        final long logLeadershipTermId,
        final long logPosition,
        final ClusterMember[] clusterMembers,
        final ClusterMember thisMember,
        final ConsensusModule.Context ctx)
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
            null,
            sequencerAgent);
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