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
import static org.junit.Assert.assertTrue;

public class AppointedLeaderClusterTest
{
    @Test(timeout = 10_000L)
    public void shouldConnectAndSendKeepAlive() throws Exception
    {
        final int appointedLeaderIndex = 1;

        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(appointedLeaderIndex))
        {
            cluster.awaitLeader();

            cluster.connectClient();
            assertTrue(cluster.client().sendKeepAlive());
        }
    }

    @Test(timeout = 10_000L)
    public void shouldEchoMessagesViaService() throws Exception
    {
        final int appointedLeaderIndex = 1;

        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(appointedLeaderIndex))
        {
            final TestNode leader = cluster.awaitLeader();

            assertThat(leader.index(), is(appointedLeaderIndex));
            assertThat(leader.role(), is(Cluster.Role.LEADER));

            cluster.connectClient();
            final int messageCount = 10;
            cluster.sendMessages(messageCount);
            cluster.awaitResponses(messageCount);
            cluster.awaitMessageCountForService(leader, messageCount);
            cluster.awaitMessageCountForService(cluster.node(0), messageCount);
            cluster.awaitMessageCountForService(cluster.node(2), messageCount);
        }
    }
}
