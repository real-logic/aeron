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

import io.aeron.cluster.service.ClusteredService;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@Ignore
public class MultiNodeTest
{
    private static final String THREE_NODE_MEMBERS =
        "0,localhost:9010,localhost:9020,localhost:9030,localhost:8010|" +
        "1,localhost:9011,localhost:9021,localhost:9031,localhost:8011|" +
        "2,localhost:9012,localhost:9022,localhost:9032,localhost:8012";

    private final MemberStatusListener[] mockMemberStatusListeners = new MemberStatusListener[3];

    @Before
    public void before()
    {
        for (int i = 0; i < mockMemberStatusListeners.length; i++)
        {
            mockMemberStatusListeners[i] = mock(MemberStatusListener.class);
        }
    }

    @Test(timeout = 10_000L)
    public void shouldBecomeLeaderStaticThreeNodeConfigWithElection()
    {
        final ClusteredService mockService = mock(ClusteredService.class);

        final ConsensusModule.Context context = new ConsensusModule.Context()
            .clusterMembers(THREE_NODE_MEMBERS)
            .memberStatusChannel("aeron:udp?endpoint=localhost:9020")
            .appointedLeaderId(0);

        final MemberStatusListener[] printStatusListeners =
            ConsensusModuleHarness.printMemberStatusMixIn(System.out, mockMemberStatusListeners);

        try (ConsensusModuleHarness harness = new ConsensusModuleHarness(
            context, mockService, mockMemberStatusListeners, true, true))
        {
            harness.awaitMemberStatusMessage(1);
            harness.awaitMemberStatusMessage(2);

            verify(mockMemberStatusListeners[1]).onRequestVote(0, 0, 0, 0);
            verify(mockMemberStatusListeners[2]).onRequestVote(0, 0, 0, 0);

            harness.memberStatusPublisher().vote(
                harness.memberStatusPublication(1),
                0,
                0,
                0,
                0,
                1,
                true);

            harness.memberStatusPublisher().vote(
                harness.memberStatusPublication(2),
                0,
                0,
                0,
                0,
                2,
                true);

            harness.awaitMemberStatusMessage(1);
            harness.awaitMemberStatusMessage(2);

            verify(mockMemberStatusListeners[1]).onCommitPosition(eq(0L), eq(0L), eq(0), anyInt());
            verify(mockMemberStatusListeners[2]).onCommitPosition(eq(0L), eq(0L), eq(0), anyInt());

            harness.memberStatusPublisher().appendedPosition(
                harness.memberStatusPublication(1), 0, 0, 1);

            harness.memberStatusPublisher().appendedPosition(
                harness.memberStatusPublication(2), 0, 0, 2);

            harness.awaitServiceOnStart();
        }
    }
}
