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

import org.junit.Test;

import static io.aeron.Aeron.NULL_VALUE;

public class ClusterServiceIpcIngressTest
{
    @Test(timeout = 10_000L)
    public void shouldEchoIpcMessages() throws Exception
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            cluster.awaitLeader();
            cluster.connectClient();

            final int messageCount = 10;
            for (int i = 0; i < messageCount; i++)
            {
                cluster.msgBuffer().putStringWithoutLengthAscii(0, TestMessages.ECHO_IPC_INGRESS);
                cluster.sendMessage(TestMessages.ECHO_IPC_INGRESS.length());
            }

            cluster.awaitResponses(messageCount);
            cluster.awaitMessageCountForService(cluster.node(0), messageCount);
            cluster.awaitMessageCountForService(cluster.node(1), messageCount);
            cluster.awaitMessageCountForService(cluster.node(2), messageCount);
        }
    }
}
