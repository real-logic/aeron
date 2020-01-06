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
import org.junit.jupiter.api.Test;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeout;

public class SingleNodeTest
{
    @Test
    public void shouldStartCluster() throws Exception
    {
        assertTimeout(ofSeconds(10), () ->
        {
            try (TestCluster cluster = TestCluster.startSingleNodeStaticCluster())
            {
                final TestNode leader = cluster.awaitLeader();

                assertEquals(0, leader.index());
                assertEquals(Cluster.Role.LEADER, leader.role());
            }
        });
    }

    @Test
    public void shouldSendMessagesToCluster() throws Exception
    {
        assertTimeout(ofSeconds(10), () ->
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
        });
    }

    @Test
    public void shouldReplayLog() throws Exception
    {
        assertTimeout(ofSeconds(20), () ->
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
        });
    }
}
