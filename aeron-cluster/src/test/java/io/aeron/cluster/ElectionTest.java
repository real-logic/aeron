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
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.RecordingLog;
import org.agrona.concurrent.CachedEpochClock;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.ArrayList;
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
    private final Aeron aeron = mock(Aeron.class);
    private final Counter electionStateCounter = mock(Counter.class);
    private final RecordingLog recordingLog = mock(RecordingLog.class);
    private final MemberStatusAdapter memberStatusAdapter = mock(MemberStatusAdapter.class);
    private final MemberStatusPublisher memberStatusPublisher = mock(MemberStatusPublisher.class);
    private final SequencerAgent sequencerAgent = mock(SequencerAgent.class);

    @Before
    public void before()
    {
        when(aeron.addCounter(anyInt(), anyString())).thenReturn(electionStateCounter);
    }

    @Test
    public void shouldElectSingleNodeClusterLeader()
    {
        final long leadershipTermId = -1;
        final RecordingLog.RecoveryPlan recoveryPlan = recoveryPlan(leadershipTermId);
        final ClusterMember[] clusterMembers = ClusterMember.parse(
            "0,clientEndpoint,memberEndpoint,logEndpoint,archiveEndpoint");

        final ConsensusModule.Context ctx = new ConsensusModule.Context()
            .recordingLog(recordingLog)
            .aeron(aeron);

        final ClusterMember thisMember = clusterMembers[0];
        final Election election = newElection(leadershipTermId, recoveryPlan, clusterMembers, thisMember, ctx);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 0;
        election.doWork(t1);
        verify(recordingLog).appendTerm(0L, 0L, t1, thisMember.id());
        verify(sequencerAgent).becomeLeader(t1);
        assertThat(election.state(), is(Election.State.LEADER_READY));
    }

    @Test
    public void shouldElectAppointedLeader()
    {
        final long leadershipTermId = -1;
        final RecordingLog.RecoveryPlan recoveryPlan = recoveryPlan(leadershipTermId);
        final ClusterMember[] clusterMembers = prepareClusterMembers();

        final ClusterMember candidateMember = clusterMembers[0];
        final ConsensusModule.Context ctx = new ConsensusModule.Context()
            .recordingLog(recordingLog)
            .appointedLeaderId(candidateMember.id())
            .aeron(aeron);

        final Election election = newElection(leadershipTermId, recoveryPlan, clusterMembers, candidateMember, ctx);

        assertThat(election.state(), is(Election.State.INIT));

        final long candidateTermId = leadershipTermId + 1;
        final long t1 = 1;
        election.doWork(t1);
        verify(sequencerAgent).role(Cluster.Role.CANDIDATE);
        verify(recordingLog).appendTerm(0L, 0L, t1, candidateMember.id());
        assertThat(election.state(), is(Election.State.CANDIDATE_BALLOT));
        assertThat(election.leadershipTermId(), is(candidateTermId));

        final long t2 = 2;
        election.doWork(t2);
        verify(memberStatusPublisher).requestVote(
            clusterMembers[1].publication(),
            recoveryPlan.lastTermBaseLogPosition,
            recoveryPlan.lastTermPositionAppended,
            candidateTermId,
            candidateMember.id());
        verify(memberStatusPublisher).requestVote(
            clusterMembers[2].publication(),
            recoveryPlan.lastTermBaseLogPosition,
            recoveryPlan.lastTermPositionAppended,
            candidateTermId,
            candidateMember.id());
        assertThat(election.state(), is(Election.State.CANDIDATE_BALLOT));

        when(sequencerAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(candidateTermId, candidateMember.id(), clusterMembers[1].id(), true);
        election.onVote(candidateTermId, candidateMember.id(), clusterMembers[2].id(), true);

        final long t3 = 3;
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.LEADER_TRANSITION));

        final long t4 = 4;
        election.doWork(t4);
        verify(sequencerAgent).becomeLeader(t4);
        assertThat(clusterMembers[1].termPosition(), is(NULL_POSITION));
        assertThat(clusterMembers[2].termPosition(), is(NULL_POSITION));
        assertThat(election.state(), is(Election.State.LEADER_READY));

        final long t5 = t1 + TimeUnit.NANOSECONDS.toMillis(ctx.leaderHeartbeatIntervalNs());
        final int logSessionId = -7;
        election.logSessionId(logSessionId);
        election.doWork(t5);
        verify(memberStatusPublisher).newLeadershipTerm(
            clusterMembers[1].publication(),
            recoveryPlan.lastTermBaseLogPosition,
            recoveryPlan.lastTermPositionAppended,
            candidateTermId,
            candidateMember.id(),
            logSessionId);
        verify(memberStatusPublisher).newLeadershipTerm(
            clusterMembers[2].publication(),
            recoveryPlan.lastTermBaseLogPosition,
            recoveryPlan.lastTermPositionAppended,
            candidateTermId,
            candidateMember.id(),
            logSessionId);
        assertThat(election.state(), is(Election.State.LEADER_READY));

        final long t6 = t5 + 1;
        election.onAppendedPosition(0, candidateTermId, clusterMembers[1].id());
        election.onAppendedPosition(0, candidateTermId, clusterMembers[2].id());
        election.doWork(t6);
        final InOrder inOrder = inOrder(sequencerAgent, electionStateCounter);
        inOrder.verify(sequencerAgent).electionComplete(Cluster.Role.LEADER);
        inOrder.verify(electionStateCounter).close();
    }

    @Test
    public void shouldVoteForAppointedLeader()
    {
        final long leadershipTermId = -1;
        final int candidateId = 0;
        final RecordingLog.RecoveryPlan recoveryPlan = recoveryPlan(leadershipTermId);
        final ClusterMember[] clusterMembers = prepareClusterMembers();

        final ClusterMember followerMember = clusterMembers[1];
        final CachedEpochClock clock = new CachedEpochClock();
        final ConsensusModule.Context ctx = new ConsensusModule.Context()
            .recordingLog(recordingLog)
            .appointedLeaderId(candidateId)
            .epochClock(clock)
            .aeron(aeron);

        final Election election = newElection(leadershipTermId, recoveryPlan, clusterMembers, followerMember, ctx);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.FOLLOWER_BALLOT));

        final long candidateTermId = leadershipTermId + 1;
        final long t2 = 2;
        clock.update(t2);
        election.onRequestVote(
            candidateTermId, recoveryPlan.lastTermBaseLogPosition, recoveryPlan.lastTermPositionAppended, candidateId);
        verify(recordingLog).appendTerm(candidateTermId, 0, t2, candidateId);
        verify(memberStatusPublisher).placeVote(
            clusterMembers[candidateId].publication(), candidateTermId, candidateId, followerMember.id(), true);
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.FOLLOWER_AWAITING_RESULT));

        final int logSessionId = -7;
        election.onNewLeadershipTerm(
            recoveryPlan.lastTermBaseLogPosition,
            recoveryPlan.lastTermPositionAppended,
            candidateTermId,
            candidateId,
            logSessionId);
        assertThat(election.state(), is(Election.State.FOLLOWER_TRANSITION));

        final long t3 = 3;
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.FOLLOWER_READY));

        when(memberStatusPublisher.appendedPosition(any(), anyLong(), anyLong(), anyInt())).thenReturn(Boolean.TRUE);

        final long t4 = 4;
        election.doWork(t4);
        final InOrder inOrder = inOrder(memberStatusPublisher, sequencerAgent, electionStateCounter);
        inOrder.verify(memberStatusPublisher).appendedPosition(
            clusterMembers[candidateId].publication(), 0, candidateTermId, followerMember.id());
        inOrder.verify(sequencerAgent).electionComplete(Cluster.Role.FOLLOWER);
        inOrder.verify(electionStateCounter).close();
    }

    @Test
    public void shouldCanvassMembersForSuccessfulLeadershipBid()
    {
        final long leaderShipTermId = -1;
        final RecordingLog.RecoveryPlan recoveryPlan = recoveryPlan(leaderShipTermId);
        final ClusterMember[] clusterMembers = prepareClusterMembers();

        final ClusterMember followerMember = clusterMembers[1];
        final CachedEpochClock clock = new CachedEpochClock();
        final ConsensusModule.Context ctx = new ConsensusModule.Context()
            .random(new Random())
            .recordingLog(recordingLog)
            .epochClock(clock)
            .aeron(aeron);

        final Election election = newElection(leaderShipTermId, recoveryPlan, clusterMembers, followerMember, ctx);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        final long t2 = t1 + TimeUnit.NANOSECONDS.toMillis(ctx.statusIntervalNs());
        election.doWork(t2);
        verify(memberStatusPublisher).appendedPosition(
            clusterMembers[0].publication(),
            recoveryPlan.lastTermPositionAppended,
            leaderShipTermId,
            followerMember.id());
        verify(memberStatusPublisher).appendedPosition(
            clusterMembers[2].publication(),
            recoveryPlan.lastTermPositionAppended,
            leaderShipTermId,
            followerMember.id());
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onAppendedPosition(0, leaderShipTermId, 0);
        election.onAppendedPosition(0, leaderShipTermId, 2);
        assertThat(election.state(), is(Election.State.NOMINATE));
    }

    @Test
    public void shouldCanvassMembersForUnSuccessfulLeadershipBid()
    {
        final long leadershipTermId = -1;
        final RecordingLog.RecoveryPlan recoveryPlan = recoveryPlan(leadershipTermId);
        final ClusterMember[] clusterMembers = prepareClusterMembers();

        final ClusterMember followerMember = clusterMembers[0];
        final CachedEpochClock clock = new CachedEpochClock();
        final ConsensusModule.Context ctx = new ConsensusModule.Context()
            .random(new Random())
            .recordingLog(recordingLog)
            .epochClock(clock)
            .aeron(aeron);

        final Election election = newElection(leadershipTermId, recoveryPlan, clusterMembers, followerMember, ctx);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        final long t2 = t1 + TimeUnit.NANOSECONDS.toMillis(ctx.statusIntervalNs());
        election.doWork(t2);
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onAppendedPosition(0, leadershipTermId + 1, 1);
        election.onAppendedPosition(0, leadershipTermId, 2);
        assertThat(election.state(), is(Election.State.FOLLOWER_BALLOT));
    }

    @Test
    public void shouldVoteForCandidateDuringNomination()
    {
        final long leadershipTermId = -1;
        final RecordingLog.RecoveryPlan recoveryPlan = recoveryPlan(leadershipTermId);
        final ClusterMember[] clusterMembers = prepareClusterMembers();

        final ClusterMember followerMember = clusterMembers[1];
        final CachedEpochClock clock = new CachedEpochClock();
        final ConsensusModule.Context ctx = new ConsensusModule.Context()
            .random(new Random())
            .recordingLog(recordingLog)
            .epochClock(clock)
            .aeron(aeron);

        final Election election = newElection(leadershipTermId, recoveryPlan, clusterMembers, followerMember, ctx);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        final long t2 = t1 + TimeUnit.NANOSECONDS.toMillis(ctx.statusIntervalNs());
        election.doWork(t2);
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onAppendedPosition(0, leadershipTermId, 0);
        election.onAppendedPosition(0, leadershipTermId, 2);
        assertThat(election.state(), is(Election.State.NOMINATE));

        final long t3 = t2 + 1;
        final long candidateTermId = leadershipTermId + 1;
        election.onRequestVote(
            candidateTermId, recoveryPlan.lastTermBaseLogPosition, recoveryPlan.lastTermBaseLogPosition, 0);
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.FOLLOWER_AWAITING_RESULT));
    }

    @Test
    public void shouldTimeoutCanvassWithQuorum()
    {
        final long leadershipTermId = -1;
        final RecordingLog.RecoveryPlan recoveryPlan = recoveryPlan(leadershipTermId);
        final ClusterMember[] clusterMembers = prepareClusterMembers();

        final ClusterMember followerMember = clusterMembers[1];
        final CachedEpochClock clock = new CachedEpochClock();
        final ConsensusModule.Context ctx = new ConsensusModule.Context()
            .random(new Random())
            .recordingLog(recordingLog)
            .epochClock(clock)
            .aeron(aeron);

        final Election election = newElection(leadershipTermId, recoveryPlan, clusterMembers, followerMember, ctx);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 1;
        election.doWork(t1);
        assertThat(election.state(), is(Election.State.CANVASS));

        election.onAppendedPosition(0, leadershipTermId, 0);
        assertThat(election.state(), is(Election.State.CANVASS));

        final long t2 = t1 + 1;
        election.doWork(t2);
        assertThat(election.state(), is(Election.State.CANVASS));

        final long t3 = t2 + TimeUnit.NANOSECONDS.toMillis(ctx.startupStatusTimeoutNs());
        election.doWork(t3);
        assertThat(election.state(), is(Election.State.NOMINATE));
    }

    private Election newElection(
        final long leadershipTermId,
        final RecordingLog.RecoveryPlan recoveryPlan,
        final ClusterMember[] clusterMembers,
        final ClusterMember thisMember,
        final ConsensusModule.Context ctx)
    {
        return new Election(
            true,
            leadershipTermId,
            clusterMembers,
            thisMember,
            memberStatusAdapter,
            memberStatusPublisher,
            recoveryPlan,
            null,
            ctx,
            null,
            sequencerAgent);
    }

    private static ClusterMember[] prepareClusterMembers()
    {
        final ClusterMember[] clusterMembers = ClusterMember.parse(
            "0,clientEndpoint,memberEndpoint,logEndpoint,archiveEndpoint|" +
                "1,clientEndpoint,memberEndpoint,logEndpoint,archiveEndpoint|" +
                "2,clientEndpoint,memberEndpoint,logEndpoint,archiveEndpoint|");

        clusterMembers[0].publication(mock(Publication.class));
        clusterMembers[1].publication(mock(Publication.class));
        clusterMembers[2].publication(mock(Publication.class));

        return clusterMembers;
    }

    private static RecordingLog.RecoveryPlan recoveryPlan(final long leadershipTermId)
    {
        final long lastTermBaseLogPosition = 0;
        final long lastTermPositionCommitted = 0;
        final long lastTermPositionAppended = 0;
        final RecordingLog.ReplayStep snapshotStep = null;
        final ArrayList<RecordingLog.ReplayStep> termSteps = new ArrayList<>();

        return new RecordingLog.RecoveryPlan(
            leadershipTermId,
            lastTermBaseLogPosition,
            lastTermPositionCommitted,
            lastTermPositionAppended,
            snapshotStep,
            termSteps);
    }
}