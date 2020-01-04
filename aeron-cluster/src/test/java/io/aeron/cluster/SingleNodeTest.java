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

import io.aeron.cluster.service.Cluster;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SingleNodeTest
{
    @Test(timeout = 10_000L)
    public void shouldStartCluster() throws Exception
    {
        try (TestCluster cluster = TestCluster.startSingleNodeStaticCluster())
        {
            final TestNode leader = cluster.awaitLeader();

            assertEquals(0, leader.index());
            assertEquals(Cluster.Role.LEADER, leader.role());
        }
    }

    @Test(timeout = 10_000L)
    public void shouldSendMessagesToCluster() throws Exception
    {
        try (TestCluster cluster = TestCluster.startSingleNodeStaticCluster())
        {
            final TestNode leader = cluster.awaitLeader();

            assertEquals(0, leader.index());
            assertEquals(Cluster.Role.LEADER, leader.role());

            cluster.connectClient();
            cluster.sendMessages(10);
            cluster.awaitResponses(10);
            cluster.awaitMessageCountForService(leader, 10);
        }
    }

    @Test(timeout = 20_000L)
    public void shouldReplayLog() throws Exception
    {
        try (TestCluster cluster = TestCluster.startSingleNodeStaticCluster())
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.connectClient();
            cluster.sendMessages(10);
            cluster.awaitResponses(10);
            cluster.awaitMessageCountForService(leader, 10);

            cluster.stopNode(leader);

            cluster.startStaticNode(0, false);
            final TestNode newLeader = cluster.awaitLeader();
            cluster.awaitMessageCountForService(newLeader, 10);
        }
    }
}
