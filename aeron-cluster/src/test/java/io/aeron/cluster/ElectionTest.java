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
import io.aeron.cluster.service.RecordingLog;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ElectionTest
{
    @Test
    public void shouldElectSingleNodeClusterAsLeader()
    {
        final RecordingLog recordingLog = mock(RecordingLog.class);
        final RecordingLog.RecoveryPlan recoveryPlan = mock(RecordingLog.RecoveryPlan.class);

        final long initialLeaderShipTermId = -1;
        final ClusterMember[] clusterMembers = ClusterMember.parse(
            "0,clientEndpoint,memberEndpoint,logEndpoint,archiveEndpoint|");

        final Aeron aeron = mock(Aeron.class);
        when(aeron.addCounter(anyInt(), anyString())).thenReturn(mock(Counter.class));

        final MemberStatusAdapter memberStatusAdapter = mock(MemberStatusAdapter.class);
        final MemberStatusPublisher memberStatusPublisher = mock(MemberStatusPublisher.class);
        final SequencerAgent sequencerAgent = mock(SequencerAgent.class);
        final ConsensusModule.Context ctx = new ConsensusModule.Context()
            .recordingLog(recordingLog)
            .aeron(aeron);

        final Election election = new Election(
            initialLeaderShipTermId,
            clusterMembers,
            clusterMembers[0],
            memberStatusAdapter,
            memberStatusPublisher,
            recoveryPlan,
            null,
            ctx,
            null,
            sequencerAgent);

        assertThat(election.state(), is(Election.State.INIT));

        final long t1 = 0;
        assertThat(election.doWork(t1), is(1));
        verify(recordingLog).appendTerm(0L, 0L, t1, clusterMembers[0].id());
        assertThat(election.state(), is(Election.State.LEADER_TRANSITION));

        final long t2 = 1;
        assertThat(election.doWork(t2), is(1));
        verify(sequencerAgent).becomeLeader(t2);
        assertThat(election.state(), is(Election.State.LEADER_READY));
    }
}