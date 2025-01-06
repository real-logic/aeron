/*
 * Copyright 2014-2025 Real Logic Limited.
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

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.Cluster;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class MultiNodeTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(20)
    void shouldElectAppointedLeaderWithThreeNodesWithNoReplayNoSnapshot()
    {
        final int appointedLeaderIndex = 1;

        final TestCluster cluster = aCluster().withStaticNodes(3).withAppointedLeader(appointedLeaderIndex).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        assertEquals(appointedLeaderIndex, leader.index());
        assertEquals(Cluster.Role.LEADER, leader.role());
        assertEquals(Cluster.Role.FOLLOWER, cluster.node(0).role());
        assertEquals(Cluster.Role.FOLLOWER, cluster.node(2).role());
    }

    @Test
    @InterruptAfter(20)
    @SlowTest
    void shouldReplayWithAppointedLeaderWithThreeNodesWithNoSnapshot()
    {
        final int appointedLeaderIndex = 1;

        final TestCluster cluster = aCluster().withStaticNodes(3).withAppointedLeader(appointedLeaderIndex).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        assertEquals(appointedLeaderIndex, leader.index());
        assertEquals(Cluster.Role.LEADER, leader.role());

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendAndAwaitMessages(messageCount);

        cluster.stopAllNodes();
        cluster.restartAllNodes(false);

        cluster.awaitLeader();
        cluster.awaitServicesMessageCount(messageCount);
    }

    @Test
    @InterruptAfter(20)
    @SlowTest
    void shouldCatchUpWithAppointedLeaderWithThreeNodesWithNoSnapshot()
    {
        final int appointedLeaderIndex = 1;

        final TestCluster cluster = aCluster().withStaticNodes(3).withAppointedLeader(appointedLeaderIndex).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        assertEquals(appointedLeaderIndex, leader.index());
        assertEquals(Cluster.Role.LEADER, leader.role());

        final int preCatchupMessageCount = 5;
        final int postCatchupMessageCount = 10;
        final int totalMessageCount = preCatchupMessageCount + postCatchupMessageCount;
        cluster.connectClient();
        cluster.sendAndAwaitMessages(preCatchupMessageCount);

        cluster.stopNode(cluster.node(0));

        cluster.sendAndAwaitMessages(postCatchupMessageCount, totalMessageCount);

        cluster.stopAllNodes();
        cluster.restartAllNodes(false);

        cluster.awaitLeader();
        cluster.awaitServicesMessageCount(totalMessageCount);
    }

    @Test
    @InterruptAfter(20)
    void shouldConnectClientOverIpc()
    {
        final AeronCluster.Context clientCtx = new AeronCluster.Context();

        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int numMessages = 10;
        assertNotNull(cluster.connectIpcClient(clientCtx, leader.mediaDriver().aeronDirectoryName()));
        cluster.sendMessages(numMessages);
        cluster.awaitResponseMessageCount(numMessages);
    }

    @ParameterizedTest
    @ValueSource(strings = { "9020", "0" })
    @InterruptAfter(20)
    void shouldConnectClientUsingResolvedResponsePort(final String responsePort)
    {
        final AeronCluster.Context clientCtx = new AeronCluster.Context()
            .ingressChannel("aeron:udp?term-length=64k")
            .egressChannel("aeron:udp?term-length=64k|endpoint=localhost:" + responsePort);

        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final int numMessages = 10;
        cluster.connectClient(clientCtx);
        cluster.sendMessages(numMessages);
        cluster.awaitResponseMessageCount(numMessages);
    }
}
