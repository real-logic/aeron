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
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.ClusterTests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.function.Supplier;

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
    @InterruptAfter(10)
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
    @SlowTest
    @InterruptAfter(30)
    void shouldProcessServiceMessagesWithoutDuplicates()
    {
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withTimerServiceSupplier(new PriorityHeapTimerServiceSupplier())
            .withServiceSupplier((i) -> new TestNode.TestService[]{
                new TestNode.MessageTrackingService(1, i),
                new TestNode.MessageTrackingService(2, i),
                new TestNode.MessageTrackingService(3, i) })
            .start();
        systemTestWatcher.cluster(cluster);
        final int serviceCount = cluster.node(0).services().length;

        TestNode oldLeader = cluster.awaitLeader();
        cluster.connectClient();
        final ExpandableArrayBuffer msgBuffer = cluster.msgBuffer();

        int messageCount = 0;
        for (int i = 0; i < 50; i++)
        {
            msgBuffer.putInt(0, ++messageCount, LITTLE_ENDIAN);
            cluster.pollUntilMessageSent(SIZE_OF_INT);
        }
        cluster.awaitResponseMessageCount(messageCount * serviceCount);
        awaitMessageCounts(cluster, messageCount);

        cluster.stopNode(oldLeader);

        final TestNode newLeader = cluster.awaitLeader(oldLeader.index());
        final TestNode follower = cluster.node(3 - oldLeader.index() - newLeader.index());
        assertEquals(Cluster.Role.FOLLOWER, follower.role());
        cluster.reconnectClient();
        for (int i = 0; i < 30; i++)
        {
            msgBuffer.putInt(0, ++messageCount, LITTLE_ENDIAN);
            cluster.pollUntilMessageSent(SIZE_OF_INT);
        }
        cluster.awaitResponseMessageCount(messageCount * serviceCount);
        awaitMessageCounts(cluster, messageCount);

        oldLeader = cluster.startStaticNode(oldLeader.index(), false);
        awaitElectionClosed(oldLeader);
        assertEquals(Cluster.Role.FOLLOWER, oldLeader.role());
        awaitMessageCounts(cluster, oldLeader, messageCount);

        assertTrackedMessages(cluster, messageCount);
    }

    @Test
    @SlowTest
    @InterruptAfter(30)
    void shouldProcessServiceMessagesAndTimersWithoutDuplicatesWhenLeaderServicesAreStopped()
    {
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withTimerServiceSupplier(new PriorityHeapTimerServiceSupplier())
            .withServiceSupplier((i) -> new TestNode.TestService[]{
                new TestNode.MessageTrackingService(1, i),
                new TestNode.MessageTrackingService(2, i) })
            .start();
        systemTestWatcher.cluster(cluster);
        final int serviceCount = cluster.node(0).services().length;

        TestNode oldLeader = cluster.awaitLeader();
        cluster.connectClient();
        final ExpandableArrayBuffer msgBuffer = cluster.msgBuffer();

        int messageCount = 0;
        for (int i = 0; i < 50; i++)
        {
            msgBuffer.putInt(0, ++messageCount, LITTLE_ENDIAN);
            cluster.pollUntilMessageSent(SIZE_OF_INT);
        }
        cluster.awaitResponseMessageCount(messageCount * serviceCount);
        awaitMessageCounts(cluster, messageCount);

        oldLeader.stopServiceContainers(); // stop services to cause a new election

        final TestNode newLeader = cluster.awaitLeader(oldLeader.index());
        final TestNode follower = cluster.node(3 - oldLeader.index() - newLeader.index());
        assertEquals(Cluster.Role.FOLLOWER, follower.role());
        cluster.awaitNodeState(oldLeader, node -> Cluster.Role.FOLLOWER == node.role());
        cluster.reconnectClient();
        for (int i = 0; i < 30; i++)
        {
            msgBuffer.putInt(0, ++messageCount, LITTLE_ENDIAN);
            cluster.pollUntilMessageSent(SIZE_OF_INT);
        }
        cluster.awaitResponseMessageCount(messageCount * serviceCount);
        awaitMessageCounts(cluster, newLeader, messageCount);
        awaitMessageCounts(cluster, follower, messageCount);
        assertTrackedMessages(newLeader, messageCount);
        assertTrackedMessages(follower, messageCount);

        cluster.stopNode(oldLeader);
        oldLeader = cluster.startStaticNode(oldLeader.index(), false);
        awaitMessageCounts(cluster, oldLeader, messageCount);

        assertTrackedMessages(cluster, messageCount);
    }

    @Test
    @SlowTest
    @InterruptAfter(30)
    void shouldProcessServiceMessagesWithoutDuplicatesAfterAFullClusterRestart()
    {
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withTimerServiceSupplier(new PriorityHeapTimerServiceSupplier())
            .withServiceSupplier((i) -> new TestNode.TestService[]{
                new TestNode.MessageTrackingService(1, i) })
            .start();
        systemTestWatcher.cluster(cluster);
        final int serviceCount = cluster.node(0).services().length;

        cluster.awaitLeader();
        cluster.connectClient();
        final ExpandableArrayBuffer msgBuffer = cluster.msgBuffer();

        int messageCount = 0;
        for (int i = 0; i < 10; i++)
        {
            msgBuffer.putInt(0, ++messageCount, LITTLE_ENDIAN);
            cluster.pollUntilMessageSent(SIZE_OF_INT);
        }
        cluster.awaitResponseMessageCount(messageCount * serviceCount);
        awaitMessageCounts(cluster, messageCount);

        cluster.stopAllNodes();
        cluster.restartAllNodes(false);
        cluster.awaitLeader();

        cluster.reconnectClient();
        for (int i = 0; i < 20; i++)
        {
            msgBuffer.putInt(0, ++messageCount, LITTLE_ENDIAN);
            cluster.pollUntilMessageSent(SIZE_OF_INT);
        }
        cluster.awaitResponseMessageCount(messageCount * serviceCount);
        awaitMessageCounts(cluster, messageCount);

        assertTrackedMessages(cluster, messageCount);
    }

    @Test
    @SlowTest
    @InterruptAfter(30)
    void shouldProcessServiceMessagesWithoutDuplicatesWhenClusterIsRestartedAfterTakingASnapshot()
    {
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withTimerServiceSupplier(new PriorityHeapTimerServiceSupplier())
            .withServiceSupplier((i) -> new TestNode.TestService[]{
                new TestNode.MessageTrackingService(1, i),
                new TestNode.MessageTrackingService(2, i) })
            .start();
        systemTestWatcher.cluster(cluster);
        final int serviceCount = cluster.node(0).services().length;

        final TestNode leader = cluster.awaitLeader();
        cluster.connectClient();
        final ExpandableArrayBuffer msgBuffer = cluster.msgBuffer();

        int messageCount = 0;
        for (int i = 0; i < 10; i++)
        {
            msgBuffer.putInt(0, ++messageCount, LITTLE_ENDIAN);
            cluster.pollUntilMessageSent(SIZE_OF_INT);
        }
        cluster.awaitResponseMessageCount(messageCount * serviceCount);
        awaitMessageCounts(cluster, messageCount);

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);

        for (int i = 0; i < 5; i++)
        {
            msgBuffer.putInt(0, ++messageCount, LITTLE_ENDIAN);
            cluster.pollUntilMessageSent(SIZE_OF_INT);
        }
        cluster.awaitResponseMessageCount(messageCount * serviceCount);
        awaitMessageCounts(cluster, messageCount);

        cluster.stopAllNodes();
        cluster.restartAllNodes(false);
        cluster.awaitLeader();

        cluster.reconnectClient();
        for (int i = 0; i < 20; i++)
        {
            msgBuffer.putInt(0, ++messageCount, LITTLE_ENDIAN);
            cluster.pollUntilMessageSent(SIZE_OF_INT);
        }
        cluster.awaitResponseMessageCount(messageCount * serviceCount);
        awaitMessageCounts(cluster, messageCount);

        assertTrackedMessages(cluster, messageCount);
    }

    private static void awaitMessageCounts(final TestCluster cluster, final int messageCount)
    {
        for (int i = 0; i < 3; i++)
        {
            final TestNode node = cluster.node(i);
            if (null != node && !node.isClosed())
            {
                awaitMessageCounts(cluster, node, messageCount);
            }
        }
    }

    private static void awaitMessageCounts(final TestCluster cluster, final TestNode node, final int messageCount)
    {
        final TestNode.TestService[] services = node.services();
        for (final TestNode.TestService service : services)
        {
            // 1 client message + 3 service messages x number of services
            cluster.awaitServiceMessageCount(node, service, messageCount + (messageCount * 3 * services.length));
            // 2 timers x number of services
            cluster.awaitTimerEventCount(node, service, messageCount * 2 * services.length);
        }
    }

    private static void assertTrackedMessages(final TestCluster cluster, final int messageCount)
    {
        for (int i = 0; i < 3; i++)
        {
            final TestNode node = cluster.node(i);
            assertTrackedMessages(node, messageCount);
        }
    }

    private static void assertTrackedMessages(final TestNode node, final int messageCount)
    {
        final TestNode.TestService[] services = node.services();
        for (final TestNode.TestService service : services)
        {
            final Supplier<String> errorMsg = service::toString;
            final TestNode.MessageTrackingService trackingService = (TestNode.MessageTrackingService)service;
            assertEquals(messageCount, trackingService.clientMessages(), errorMsg);
            assertEquals(messageCount * 3 * services.length, trackingService.serviceMessages(), errorMsg);
            assertEquals(messageCount * 2 * services.length, trackingService.timers(), errorMsg);
        }
    }
}
