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

import io.aeron.cluster.TestNode.TestService;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClientSession;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.EpochClock;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class FollowerRestartTest
{
    @Test(timeout = 30_000)
    public void testRecoveryFromMostRecentSnapshot() throws Exception
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE, MessageCountingService::new))
        {
            final TestNode leader = cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();
            final TestNode followerA = followers.get(0);
            final TestNode followerB = followers.get(1);

            cluster.connectClient();

            cluster.sendMessages(10);
            cluster.awaitResponses(10);
            awaitMessageCountSinceStart(cluster.client(), cluster.node(2), 3);

            leader.terminationExpected(true);
            followerA.terminationExpected(true);
            followerB.terminationExpected(true);

            cluster.awaitNeutralControlToggle(leader);
            cluster.shutdownCluster(leader);
            cluster.awaitNodeTermination(cluster.node(0));
            cluster.awaitNodeTermination(cluster.node(1));
            cluster.awaitNodeTermination(cluster.node(2));

            assertTrue(cluster.node(0).service().wasSnapshotTaken());
            assertTrue(cluster.node(1).service().wasSnapshotTaken());
            assertTrue(cluster.node(2).service().wasSnapshotTaken());

            cluster.stopNode(cluster.node(0));
            cluster.stopNode(cluster.node(1));
            cluster.stopNode(cluster.node(2));
            Thread.sleep(1_000);

            cluster.startStaticNode(0, false, MessageCountingService::new);
            cluster.startStaticNode(1, false, MessageCountingService::new);
            cluster.startStaticNode(2, true, MessageCountingService::new);

            final TestNode newLeader = cluster.awaitLeader();

            assertNotEquals(newLeader.index(), is(2));

            assertTrue(cluster.node(0).service().wasSnapshotLoaded());
            assertTrue(cluster.node(1).service().wasSnapshotLoaded());
            assertFalse(cluster.node(2).service().wasSnapshotLoaded());

            assertEquals(10, cluster.node(0).service().messageCount());
            assertEquals(10, cluster.node(1).service().messageCount());
            assertEquals(10, cluster.node(2).service().messageCount());

            // I'd expect NODE 2 recovers from the most recent snapshot + journal
            // (which should be zero length because of the clean shutdown)
            // but this is not the case: Node 2 processes the complete journal (from the very beginning)
            // all 10 messages.
            assertEquals(0, ((MessageCountingService)cluster.node(0).service()).messageSinceStart());
            assertEquals(0, ((MessageCountingService)cluster.node(1).service()).messageSinceStart());
            assertThat(((MessageCountingService)cluster.node(2).service()).messageSinceStart(), is(0));
        }
    }

    @Test(timeout = 30_000)
    public void testMultipleSnapshotsWithEmptyFollowerLog() throws Exception
    {
        final int memberCount = 3;
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();
            final TestNode followerA = followers.get(0);
            final TestNode followerB = followers.get(1);

            cluster.connectClient();

            cluster.sendMessages(2);
            cluster.awaitResponses(2);
            cluster.awaitMessageCountForService(cluster.node(2), 2);

            cluster.takeSnapshot(leader);
            for (int memberId = 0; memberId < memberCount; memberId++)
            {
                final TestNode node = cluster.node(memberId);
                cluster.awaitSnapshotCounter(node, 1);
                assertTrue(node.service().wasSnapshotTaken());
                node.service().resetSnapshotTaken();
            }

            cluster.sendMessages(1);
            cluster.awaitResponses(3);
            cluster.awaitMessageCountForService(cluster.node(2), 3);

            leader.terminationExpected(true);
            followerA.terminationExpected(true);
            followerB.terminationExpected(true);

            cluster.awaitNeutralControlToggle(leader);
            cluster.shutdownCluster(leader);
            cluster.awaitNodeTermination(cluster.node(0));
            cluster.awaitNodeTermination(cluster.node(1));
            cluster.awaitNodeTermination(cluster.node(2));

            assertTrue(cluster.node(0).service().wasSnapshotTaken());
            assertTrue(cluster.node(1).service().wasSnapshotTaken());
            assertTrue(cluster.node(2).service().wasSnapshotTaken());

            cluster.stopNode(cluster.node(0));
            cluster.stopNode(cluster.node(1));
            cluster.stopNode(cluster.node(2));
            Thread.sleep(1_000);

            cluster.startStaticNode(0, false, MessageCountingService::new);
            cluster.startStaticNode(1, false, MessageCountingService::new);
            cluster.startStaticNode(2, true, MessageCountingService::new);

            // NOTE the EXCEPTION in the logs at this point, but we can't catch it
            // *** Error in node 2 followed by system thread dump ***
            final TestNode newLeader = cluster.awaitLeader();

            assertNotEquals(newLeader.index(), is(2));

            assertTrue(cluster.node(0).service().wasSnapshotLoaded());
            assertTrue(cluster.node(1).service().wasSnapshotLoaded());
            assertFalse(cluster.node(2).service().wasSnapshotLoaded());

            // all three nodes have the correct state
            assertEquals(3, cluster.node(0).service().messageCount());
            assertEquals(3, cluster.node(1).service().messageCount());
            assertEquals(3, cluster.node(2).service().messageCount());

            // now lets see if all nodes work properly
            cluster.reconnectClient();
            final int msgCountAfterStart = 4;
            final int totalMsgCount = 2 + 1 + 4;
            cluster.sendMessages(msgCountAfterStart);
            cluster.awaitResponses(totalMsgCount);
            cluster.awaitMessageCountForService(newLeader, totalMsgCount);
            assertEquals(newLeader.service().messageCount(), totalMsgCount);

            cluster.awaitMessageCountForService(cluster.node(1), totalMsgCount);
            assertEquals((cluster.node(1).service()).messageCount(), totalMsgCount);

            // IMPORTANT: After coming back online and reconstructing state, NODE 2 is not part of the cluster
            cluster.awaitMessageCountForService(cluster.node(2), totalMsgCount);
            assertEquals(cluster.node(2).service().messageCount(), totalMsgCount);
        }
    }

    void awaitMessageCountSinceStart(final AeronCluster client, final TestNode node, final int countSinceStart)
    {
        final EpochClock epochClock = client.context().aeron().context().epochClock();
        long deadlineMs = epochClock.time() + TimeUnit.SECONDS.toMillis(1);
        final MessageCountingService svc = (MessageCountingService)node.service();

        while (svc.messageSinceStart() < countSinceStart)
        {
            TestUtil.checkInterruptedStatus();
            Thread.yield();

            final long nowMs = epochClock.time();
            if (nowMs > deadlineMs)
            {
                client.sendKeepAlive();
                deadlineMs = nowMs + TimeUnit.SECONDS.toMillis(1);
            }
        }
    }

    static class MessageCountingService extends TestService
    {
        volatile int messagesReceivedSinceStart = 0;

        public void onSessionMessage(
            final ClientSession session,
            final long timestampMs,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            super.onSessionMessage(session, timestampMs, buffer, offset, length, header);
            messagesReceivedSinceStart++;
        }

        int messageSinceStart()
        {
            return messagesReceivedSinceStart;
        }
    }
}
