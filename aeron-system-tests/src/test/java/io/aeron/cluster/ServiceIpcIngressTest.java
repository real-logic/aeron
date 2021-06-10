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

import io.aeron.test.ClusterTestWatcher;
import io.aeron.test.cluster.ClusterTests;
import io.aeron.test.cluster.TestCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ServiceIpcIngressTest
{
    @RegisterExtension
    final ClusterTestWatcher clusterTestWatcher = new ClusterTestWatcher();

    @AfterEach
    void tearDown()
    {
        assertEquals(
            0, clusterTestWatcher.errorCount(), "Errors observed in cluster test");
    }

    @Test
    @Timeout(20)
    public void shouldEchoIpcMessages()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        clusterTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        cluster.connectClient();

        final int messageCount = 10;
        for (int i = 0; i < messageCount; i++)
        {
            cluster.msgBuffer().putStringWithoutLengthAscii(0, ClusterTests.ECHO_IPC_INGRESS_MSG);
            cluster.pollUntilMessageSent(ClusterTests.ECHO_IPC_INGRESS_MSG.length());
        }

        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);
    }
}
