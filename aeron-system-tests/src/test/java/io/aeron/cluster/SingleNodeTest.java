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
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.aeron.Aeron.Configuration.PRE_TOUCH_MAPPED_MEMORY_PROP_NAME;
import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(InterruptingTestCallback.class)
public class SingleNodeTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(20)
    public void shouldStartCluster()
    {
        final TestCluster cluster = aCluster().withStaticNodes(1).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        assertEquals(0, leader.index());
        assertEquals(Cluster.Role.LEADER, leader.role());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    @InterruptAfter(20)
    public void shouldSendMessagesToCluster(final boolean preTouch)
    {
        System.setProperty(PRE_TOUCH_MAPPED_MEMORY_PROP_NAME, Boolean.toString(preTouch));
        try
        {
            final TestCluster cluster = aCluster().withStaticNodes(1).start();
            systemTestWatcher.cluster(cluster);

            final TestNode leader = cluster.awaitLeader();

            assertEquals(0, leader.index());
            assertEquals(Cluster.Role.LEADER, leader.role());

            cluster.connectClient();
            cluster.sendMessages(10);
            cluster.awaitResponseMessageCount(10);
            cluster.awaitServiceMessageCount(leader, 10);
        }
        finally
        {
            System.clearProperty(PRE_TOUCH_MAPPED_MEMORY_PROP_NAME);
        }
    }

    @Test
    @InterruptAfter(20)
    public void shouldReplayLog()
    {
        final TestCluster cluster = aCluster().withStaticNodes(1).start();
        systemTestWatcher.cluster(cluster);

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
