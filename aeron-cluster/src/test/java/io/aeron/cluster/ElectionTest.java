/*
 * Copyright 2014-2021 Real Logic Limited.
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
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.test.cluster.TestClusterClock;
import org.agrona.collections.Int2ObjectHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Random;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class ElectionTest
{
    private static final long RECORDING_ID = 1L;
    private static final int LOG_SESSION_ID = 777;
    private final Aeron aeron = mock(Aeron.class);
    private final Counter electionStateCounter = mock(Counter.class);
    private final Subscription subscription = mock(Subscription.class);
    private final Image logImage = mock(Image.class);
    private final RecordingLog recordingLog = mock(RecordingLog.class);
    private final ClusterMarkFile clusterMarkFile = mock(ClusterMarkFile.class);
    private final ConsensusPublisher consensusPublisher = mock(ConsensusPublisher.class);
    private final ConsensusModuleAgent consensusModuleAgent = mock(ConsensusModuleAgent.class);

    private final ConsensusModule.Context ctx = new ConsensusModule.Context()
        .aeron(aeron)
        .recordingLog(recordingLog)
        .clusterClock(new TestClusterClock(NANOSECONDS))
        .random(new Random())
        .electionStateCounter(electionStateCounter)
        .clusterMarkFile(clusterMarkFile);

    @BeforeEach
    public void before()
    {
        when(aeron.addCounter(anyInt(), anyString())).thenReturn(electionStateCounter);
        when(aeron.addSubscription(anyString(), anyInt())).thenReturn(subscription);
        when(consensusModuleAgent.logRecordingId()).thenReturn(RECORDING_ID);
        when(consensusModuleAgent.addLogPublication()).thenReturn(LOG_SESSION_ID);
        when(clusterMarkFile.candidateTermId()).thenReturn((long)Aeron.NULL_VALUE);
        when(subscription.imageBySessionId(anyInt())).thenReturn(logImage);
    }

    @Test
    public void shouldElectSingleNodeClusterLeader()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = ClusterMember.parse(
            "0,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint");

        final ClusterMember thisMember = clusterMembers[0];
        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, thisMember);

        final long newLeadershipTermId = leadershipTermId + 1;
        when(recordingLog.isUnknown(newLeadershipTermId)).thenReturn(Boolean.TRUE);
        final long t1 = 1;
        election.doWork(t1);
        election.doWork(t1);

        verify(clusterMarkFile).candidateTermId();
        verify(consensusModuleAgent).joinLogAsLeader(eq(newLeadershipTermId), eq(logPosition), anyInt(), eq(true));
        verify(recordingLog).isUnknown(newLeadershipTermId);
        verify(recordingLog).appendTerm(RECORDING_ID, newLeadershipTermId, logPosition, t1);
        verify(electionStateCounter).setOrdered(ElectionState.LEADER_READY.code());
    }

    @Test
    public void shouldElectAppointedLeader()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[0];

        ctx.appointedLeaderId(candidateMember.id());

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember);

        final long candidateTermId = leadershipTermId + 1;
        final long t1 = 1;
        election.doWork(t1);
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, logPosition, 1);
        election.onCanvassPosition(leadershipTermId, logPosition, 2);

        final long t2 = 2;
        election.doWork(t2);
        verify(electionStateCounter).setOrdered(ElectionState.NOMINATE.code());

        final long t3 = t2 + (ctx.electionTimeoutNs() >> 1);
        election.doWork(t3);
        election.doWork(t3);
        verify(consensusPublisher).requestVote(
            clusterMembers[1].publication(),
            leadershipTermId,
            logPosition,
            candidateTermId,
            candidateMember.id());
        verify(consensusPublisher).requestVote(
            clusterMembers[2].publication(),
            leadershipTermId,
            logPosition,
            candidateTermId,
            candidateMember.id());
        verify(electionStateCounter).setOrdered(ElectionState.CANDIDATE_BALLOT.code());
        verify(consensusModuleAgent).role(Cluster.Role.CANDIDATE);

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(
            candidateTermId, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[1].id(), true);
        election.onVote(
            candidateTermId, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[2].id(), true);

        final long t4 = t3 + 1;
        election.doWork(t3);
        verify(electionStateCounter).setOrdered(ElectionState.LEADER_REPLAY.code());

        when(recordingLog.isUnknown(candidateTermId)).thenReturn(Boolean.TRUE);

        final long t5 = t4 + 1;
        election.doWork(t5);
        verify(consensusPublisher).newLeadershipTerm(
            clusterMembers[1].publication(),
            leadershipTermId,
            logPosition,
            candidateTermId,
            logPosition,
            t5,
            candidateMember.id(),
            LOG_SESSION_ID,
            election.isLeaderStartup());
        verify(consensusPublisher).newLeadershipTerm(
            clusterMembers[2].publication(),
            leadershipTermId,
            logPosition,
            candidateTermId,
            logPosition,
            t5,
            candidateMember.id(),
            LOG_SESSION_ID,
            election.isLeaderStartup());

        election.doWork(t5);

        verify(consensusModuleAgent).joinLogAsLeader(eq(candidateTermId), eq(logPosition), anyInt(), eq(true));
        verify(recordingLog).appendTerm(RECORDING_ID, candidateTermId, logPosition, t5);
        verify(electionStateCounter).setOrdered(ElectionState.LEADER_READY.code());

        assertEquals(NULL_POSITION, clusterMembers[1].logPosition());
        assertEquals(NULL_POSITION, clusterMembers[2].logPosition());
        assertEquals(candidateTermId, election.leadershipTermId());

        final long t6 = t5 + 1;
        election.doWork(t6);
        verify(electionStateCounter).setOrdered(ElectionState.LEADER_READY.code());

        when(consensusModuleAgent.electionComplete()).thenReturn(true);

        final long t7 = t6 + 1;
        election.onAppendPosition(candidateTermId, logPosition, clusterMembers[1].id());
        election.onAppendPosition(candidateTermId, logPosition, clusterMembers[2].id());
        election.doWork(t7);
        final InOrder inOrder = inOrder(consensusModuleAgent, electionStateCounter);
        inOrder.verify(consensusModuleAgent).electionComplete();
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CLOSED.code());
    }

    @Test
    public void shouldVoteForAppointedLeader()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final int candidateId = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        final long t1 = 1;
        election.doWork(t1);
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        final long candidateTermId = leadershipTermId + 1;
        final long t2 = 2;
        election.onRequestVote(leadershipTermId, logPosition, candidateTermId, candidateId);
        verify(consensusPublisher).placeVote(
            clusterMembers[candidateId].publication(),
            candidateTermId,
            leadershipTermId,
            logPosition,
            candidateId,
            followerMember.id(),
            true);
        election.doWork(t2);
        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_BALLOT.code());

        final int logSessionId = -7;
        election.onNewLeadershipTerm(
            leadershipTermId, logPosition, candidateTermId, logPosition, t2, candidateId, logSessionId, false);
        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_REPLAY.code());

        final long t3 = 3;
        election.doWork(t3);
        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_LOG_INIT.code());

        final long t4 = 4;
        election.doWork(t4);
        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_LOG_AWAIT.code());

        final long t5 = 5;
        election.doWork(t5);
        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_READY.code());

        when(consensusPublisher.appendPosition(any(), anyLong(), anyLong(), anyInt())).thenReturn(Boolean.TRUE);
        when(consensusModuleAgent.electionComplete()).thenReturn(true);

        final long t6 = 6;
        election.doWork(t6);
        final InOrder inOrder = inOrder(consensusPublisher, consensusModuleAgent, electionStateCounter);
        inOrder.verify(consensusPublisher).appendPosition(
            clusterMembers[candidateId].publication(), candidateTermId, logPosition, followerMember.id());
        inOrder.verify(consensusModuleAgent).electionComplete();
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CLOSED.code());
    }

    @Test
    public void shouldCanvassMembersInSuccessfulLeadershipBid()
    {
        final long logPosition = 0;
        final long leadershipTermId = Aeron.NULL_VALUE;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        final long t1 = 1;
        election.doWork(t1);
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        final long t2 = t1 + ctx.electionStatusIntervalNs();
        election.doWork(t2);
        verify(consensusPublisher).canvassPosition(
            clusterMembers[0].publication(), leadershipTermId, logPosition, followerMember.id());
        verify(consensusPublisher).canvassPosition(
            clusterMembers[2].publication(), leadershipTermId, logPosition, followerMember.id());

        election.onCanvassPosition(leadershipTermId, logPosition, 0);
        election.onCanvassPosition(leadershipTermId, logPosition, 2);

        final long t3 = t2 + 1;
        election.doWork(t3);
        verify(electionStateCounter).setOrdered(ElectionState.NOMINATE.code());
    }

    @Test
    public void shouldVoteForCandidateDuringNomination()
    {
        final long logPosition = 0;
        final long leadershipTermId = Aeron.NULL_VALUE;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        final long t1 = 1;
        election.doWork(t1);
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        final long t2 = t1 + ctx.electionStatusIntervalNs();
        election.doWork(t2);

        election.onCanvassPosition(leadershipTermId, logPosition, 0);
        election.onCanvassPosition(leadershipTermId, logPosition, 2);

        final long t3 = t2 + 1;
        election.doWork(t3);
        verify(electionStateCounter).setOrdered(ElectionState.NOMINATE.code());

        final long t4 = t3 + 1;
        final long candidateTermId = leadershipTermId + 1;
        election.onRequestVote(leadershipTermId, logPosition, candidateTermId, 0);
        election.doWork(t4);
        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_BALLOT.code());
    }

    @Test
    public void shouldTimeoutCanvassWithMajority()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        final long t1 = 1;
        election.doWork(t1);
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onAppendPosition(leadershipTermId, logPosition, 0);

        final long t2 = t1 + 1;
        election.doWork(t2);

        final long t3 = t2 + ctx.startupCanvassTimeoutNs();
        election.doWork(t3);
        verify(electionStateCounter).setOrdered(ElectionState.NOMINATE.code());
    }

    @Test
    public void shouldWinCandidateBallotWithMajority()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[1];

        final Election election = newElection(false, leadershipTermId, logPosition, clusterMembers, candidateMember);

        final long t1 = 1;
        election.doWork(t1);
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, logPosition, 0);
        election.onCanvassPosition(leadershipTermId, logPosition, 2);

        final long t2 = t1 + 1;
        election.doWork(t2);
        verify(electionStateCounter).setOrdered(ElectionState.NOMINATE.code());

        final long t3 = t2 + (ctx.electionTimeoutNs() >> 1);
        election.doWork(t3);
        verify(electionStateCounter).setOrdered(ElectionState.CANDIDATE_BALLOT.code());

        final long t4 = t3 + ctx.electionTimeoutNs();
        final long candidateTermId = leadershipTermId + 1;
        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(
            candidateTermId, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[2].id(), true);
        election.doWork(t4);
        verify(electionStateCounter).setOrdered(ElectionState.LEADER_REPLAY.code());
    }

    @ParameterizedTest
    @CsvSource(value = {"true,true", "true,false", "false,false", "false,true"})
    public void shouldBaseStartupValueOnLeader(final boolean isLeaderStart, final boolean isNodeStart)
    {
        final long leadershipTermId = 0;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];

        final Election election = newElection(
            isNodeStart, leadershipTermId, logPosition, clusterMembers, followerMember);

        final long t1 = 1;
        election.doWork(t1);
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        final long t2 = t1 + 1;
        final int leaderMemberId = clusterMembers[0].id();
        election.onNewLeadershipTerm(
            leadershipTermId, logPosition, leadershipTermId, logPosition, t2, leaderMemberId, 0, isLeaderStart);
        election.doWork(t2);

        final long t3 = t2 + 1;
        election.doWork(t3);

        final long t4 = t3 + 1;
        election.doWork(t4);

        verify(consensusModuleAgent).joinLogAsFollower(logImage, leadershipTermId, isLeaderStart);
    }

    @Test
    public void shouldElectCandidateWithFullVote()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember);

        final long t1 = 1;
        election.doWork(t1);
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, logPosition, 0);
        election.onCanvassPosition(leadershipTermId, logPosition, 2);

        final long t2 = t1 + 1;
        election.doWork(t2);
        verify(electionStateCounter).setOrdered(ElectionState.NOMINATE.code());

        final long t3 = t2 + (ctx.electionTimeoutNs() >> 1);
        election.doWork(t3);
        verify(electionStateCounter).setOrdered(ElectionState.CANDIDATE_BALLOT.code());

        final long t4 = t3 + 1;
        final long candidateTermId = leadershipTermId + 1;
        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(
            candidateTermId, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[0].id(), false);
        election.onVote(
            candidateTermId, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[2].id(), true);
        election.doWork(t4);
        verify(electionStateCounter).setOrdered(ElectionState.LEADER_REPLAY.code());
    }

    @Test
    public void shouldTimeoutCandidateBallotWithoutMajority()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember);

        final long t1 = 1;
        election.doWork(t1);
        final InOrder inOrder = Mockito.inOrder(electionStateCounter);
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, logPosition, 0);
        election.onCanvassPosition(leadershipTermId, logPosition, 2);

        final long t2 = t1 + 1;
        election.doWork(t2);
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.NOMINATE.code());

        final long t3 = t2 + (ctx.electionTimeoutNs() >> 1);
        election.doWork(t3);
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CANDIDATE_BALLOT.code());

        final long t4 = t3 + ctx.electionTimeoutNs();
        election.doWork(t4);
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());
        assertEquals(leadershipTermId, election.leadershipTermId());
    }

    @Test
    public void shouldTimeoutFailedCandidateBallotOnSplitVoteThenSucceedOnRetry()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember);

        final long t1 = 1;
        election.doWork(t1);
        final InOrder inOrder = Mockito.inOrder(electionStateCounter);
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, logPosition, 0);

        final long t2 = t1 + ctx.startupCanvassTimeoutNs();
        election.doWork(t2);
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.NOMINATE.code());

        final long t3 = t2 + (ctx.electionTimeoutNs() >> 1);
        election.doWork(t3);
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CANDIDATE_BALLOT.code());

        final long t4 = t3 + 1;
        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(
            leadershipTermId + 1, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[2].id(), false);
        election.doWork(t4);

        final long t5 = t4 + ctx.electionTimeoutNs();
        election.doWork(t5);
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, logPosition, 0);

        final long t6 = t5 + 1;
        election.doWork(t6);

        final long t7 = t6 + ctx.electionTimeoutNs();
        election.doWork(t7);
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.NOMINATE.code());

        final long t8 = t7 + ctx.electionTimeoutNs();
        election.doWork(t8);
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CANDIDATE_BALLOT.code());

        final long t9 = t8 + 1;
        election.doWork(t9);

        final long candidateTermId = leadershipTermId + 2;
        election.onVote(
            candidateTermId, leadershipTermId + 1, logPosition, candidateMember.id(), clusterMembers[2].id(), true);

        final long t10 = t9 + ctx.electionTimeoutNs();
        election.doWork(t10);
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.LEADER_REPLAY.code());

        final long t11 = t10 + 1;
        election.doWork(t11);
        election.doWork(t11);
        assertEquals(candidateTermId, election.leadershipTermId());
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.LEADER_READY.code());
    }

    @Test
    public void shouldTimeoutFollowerBallotWithoutLeaderEmerging()
    {
        final long leadershipTermId = Aeron.NULL_VALUE;
        final long logPosition = 0L;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        final long t1 = 1;
        election.doWork(t1);
        final InOrder inOrder = Mockito.inOrder(electionStateCounter);
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        final long candidateTermId = leadershipTermId + 1;
        election.onRequestVote(leadershipTermId, logPosition, candidateTermId, 0);

        final long t2 = t1 + 1;
        election.doWork(t2);
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_BALLOT.code());

        final long t3 = t2 + ctx.electionTimeoutNs();
        election.doWork(t3);
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());
        assertEquals(leadershipTermId, election.leadershipTermId());
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
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());
        verify(consensusModuleAgent).prepareForNewLeadership(logPosition);
        verify(consensusModuleAgent).role(Cluster.Role.FOLLOWER);
    }

    private Election newElection(
        final boolean isStartup,
        final long logLeadershipTermId,
        final long logPosition,
        final ClusterMember[] clusterMembers,
        final ClusterMember thisMember)
    {
        final Int2ObjectHashMap<ClusterMember> idToClusterMemberMap = new Int2ObjectHashMap<>();

        ClusterMember.addClusterMemberIds(clusterMembers, idToClusterMemberMap);

        return new Election(
            isStartup,
            logLeadershipTermId,
            logPosition,
            logPosition,
            clusterMembers,
            idToClusterMemberMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);
    }

    private Election newElection(
        final long logLeadershipTermId,
        final long logPosition,
        final ClusterMember[] clusterMembers,
        final ClusterMember thisMember)
    {
        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();

        ClusterMember.addClusterMemberIds(clusterMembers, clusterMemberByIdMap);

        return new Election(
            true,
            logLeadershipTermId,
            logPosition,
            logPosition,
            clusterMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);
    }

    private static ClusterMember[] prepareClusterMembers()
    {
        final ClusterMember[] clusterMembers = ClusterMember.parse(
            "0,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|" +
            "1,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|" +
            "2,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|");

        clusterMembers[0].publication(mock(ExclusivePublication.class));
        clusterMembers[1].publication(mock(ExclusivePublication.class));
        clusterMembers[2].publication(mock(ExclusivePublication.class));

        return clusterMembers;
    }
}
