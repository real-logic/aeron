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

import io.aeron.cluster.service.Cluster;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SingleNodeTest
{
    @Test
    @Timeout(20)
    public void shouldStartCluster()
    {
        try (TestCluster cluster = TestCluster.startSingleNodeStaticCluster())
        {
            final TestNode leader = cluster.awaitLeader();

            assertEquals(0, leader.index());
            assertEquals(Cluster.Role.LEADER, leader.role());
        }
    }

    @Test
    @Timeout(20)
    public void shouldSendMessagesToCluster()
    {
        try (TestCluster cluster = TestCluster.startSingleNodeStaticCluster())
        {
            final TestNode leader = cluster.awaitLeader();

            assertEquals(0, leader.index());
            assertEquals(Cluster.Role.LEADER, leader.role());

            cluster.connectClient();
            cluster.sendMessages(10);
            cluster.awaitResponseMessageCount(10);
            cluster.awaitServiceMessageCount(leader, 10);
        }
    }

    @Test
    @Timeout(20)
    public void shouldReplayLog()
    {
        try (TestCluster cluster = TestCluster.startSingleNodeStaticCluster())
        {
            final TestNode leader = cluster.awaitLeader();

            final int messageCount = 10;
            cluster.connectClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);
            cluster.awaitServiceMessageCount(leader, messageCount);

            cluster.stopNode(leader);

            cluster.startStaticNode(0, false);
            final TestNode newLeader = cluster.awaitLeader();
            cluster.awaitServiceMessageCount(newLeader, messageCount);
        }
    }
}
