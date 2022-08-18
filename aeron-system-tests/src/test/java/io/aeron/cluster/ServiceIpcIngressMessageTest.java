/*
 * Copyright 2014-2022 Real Logic Limited.
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
import io.aeron.test.cluster.ClusterTests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static io.aeron.test.cluster.TestCluster.awaitElectionClosed;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(InterruptingTestCallback.class)
class ServiceIpcIngressMessageTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(20)
    void shouldEchoServiceIpcMessages()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        cluster.connectClient();
        final int messageLength = cluster.msgBuffer().putStringWithoutLengthAscii(
            0, ClusterTests.ECHO_SERVICE_IPC_INGRESS_MSG);

        final int messageCount = 10;
        for (int i = 0; i < messageCount; i++)
        {
            cluster.pollUntilMessageSent(messageLength);
        }

        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);
    }

    @Test
    @InterruptAfter(20)
    void shouldProcessServiceMessagesWithoutDuplicates()
    {
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withServiceSupplier((i) -> new TestNode.TestService[]{ new TestNode.MessageTrackingService().index(i) })
            .start();
        systemTestWatcher.cluster(cluster);

        TestNode oldLeader = cluster.awaitLeader();
        cluster.connectClient();
        final ExpandableArrayBuffer msgBuffer = cluster.msgBuffer();

        int messageCount = 0;
        for (int i = 0; i < 10; i++)
        {
            msgBuffer.putInt(0, messageCount++, LITTLE_ENDIAN);
            cluster.pollUntilMessageSent(SIZE_OF_INT);
        }
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount * 4); // 1 client message + 3 service messages

        cluster.stopNode(oldLeader);

        final TestNode newLeader = cluster.awaitLeader();
        final TestNode follower = cluster.node(3 - oldLeader.index() - newLeader.index());
        assertEquals(Cluster.Role.FOLLOWER, follower.role());
        cluster.reconnectClient();
        for (int i = 0; i < 3; i++)
        {
            msgBuffer.putInt(0, messageCount++, LITTLE_ENDIAN);
            cluster.pollUntilMessageSent(SIZE_OF_INT);
        }
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount * 4);

        oldLeader = cluster.startStaticNode(oldLeader.index(), false);
        awaitElectionClosed(oldLeader);
        assertEquals(Cluster.Role.FOLLOWER, oldLeader.role());
        cluster.awaitServiceMessageCount(oldLeader, messageCount * 4);
    }
}
