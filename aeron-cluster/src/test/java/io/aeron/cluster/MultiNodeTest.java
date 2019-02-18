/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import io.aeron.cluster.service.Cluster;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class MultiNodeTest
{
    @Test(timeout = 10_000L)
    public void shouldElectAppointedLeaderWithThreeNodesWithNoReplayNoSnapshot() throws Exception
    {
        final int appointedLeaderIndex = 1;

        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(appointedLeaderIndex))
        {
            final TestNode leader = cluster.awaitLeader();

            assertThat(leader.index(), is(appointedLeaderIndex));
            assertThat(leader.role(), is(Cluster.Role.LEADER));
            assertThat(cluster.node(0).role(), is(Cluster.Role.FOLLOWER));
            assertThat(cluster.node(2).role(), is(Cluster.Role.FOLLOWER));
        }
    }

    @Test(timeout = 10_000L)
    public void shouldReplayWithAppointedLeaderWithThreeNodesWithNoSnapshot() throws Exception
    {
        final int appointedLeaderIndex = 1;
        final int messageCount = 10;

        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(appointedLeaderIndex))
        {
            TestNode leader = cluster.awaitLeader();

            assertThat(leader.index(), is(appointedLeaderIndex));
            assertThat(leader.role(), is(Cluster.Role.LEADER));

            cluster.connectClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponses(messageCount);
            cluster.awaitMessageCountForService(leader, messageCount);

            cluster.stopAllNodes();
            cluster.restartAllNodes(false);

            leader = cluster.awaitLeader();
            cluster.awaitMessageCountForService(leader, messageCount);
            cluster.awaitMessageCountForService(cluster.node(0), messageCount);
            cluster.awaitMessageCountForService(cluster.node(2), messageCount);
        }
    }

    @Test(timeout = 10_000L)
    public void shouldCatchUpWithAppointedLeaderWithThreeNodesWithNoSnapshot() throws Exception
    {
        final int appointedLeaderIndex = 1;
        final int preCatchupMessageCount = 5;
        final int postCatchupMessageCount = 10;
        final int totalMessageCount = preCatchupMessageCount + postCatchupMessageCount;

        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(appointedLeaderIndex))
        {
            TestNode leader = cluster.awaitLeader();

            assertThat(leader.index(), is(appointedLeaderIndex));
            assertThat(leader.role(), is(Cluster.Role.LEADER));

            cluster.connectClient();
            cluster.sendMessages(preCatchupMessageCount);
            cluster.awaitResponses(preCatchupMessageCount);
            cluster.awaitMessageCountForService(leader, preCatchupMessageCount);

            cluster.stopNode(cluster.node(0));

            cluster.sendMessages(postCatchupMessageCount);
            cluster.awaitResponses(postCatchupMessageCount);

            cluster.stopAllNodes();
            cluster.restartAllNodes(false);

            leader = cluster.awaitLeader();
            cluster.awaitMessageCountForService(leader, totalMessageCount);
            cluster.awaitMessageCountForService(cluster.node(0), totalMessageCount);
            cluster.awaitMessageCountForService(cluster.node(2), totalMessageCount);
        }
    }
}
