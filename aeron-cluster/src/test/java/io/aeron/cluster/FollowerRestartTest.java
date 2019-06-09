/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.aeron.cluster;

import io.aeron.cluster.TestNode.TestService;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClientSession;
import io.aeron.logbuffer.Header;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.EpochClock;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author ratcashdev
 */
public class FollowerRestartTest
{
    static final String ADD_MSG = "Add message";
    static final String REMOVE_MSG = "Remove message";
    private static final int APPOINTED_LEADER = 0;
    private final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();

    @Test(timeout = 30_000)
    @Ignore
    public void testRecoveryFromMostRecentSnapshot() throws Exception
    {
        final int memberCount = 3;
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(APPOINTED_LEADER,
            MessageCountingService::new))
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

            // all three nodes have a correct state
            assertTrue(((MessageCountingService)cluster.node(0).service()).messageCount() == 10);
            assertTrue(((MessageCountingService)cluster.node(1).service()).messageCount() == 10);
            assertTrue(((MessageCountingService)cluster.node(2).service()).messageCount() == 10);

            // I'd expect NODE 2 recovers from the most recent snapshot + journal
            // (which should be zero length because of the clean shutdown)
            // but this is not the case: Node 2 processes the complete journal (from the very beginning)
            // all 10 messages.
            assertTrue(((MessageCountingService)cluster.node(0).service()).messageSinceStart() == 0);
            assertTrue(((MessageCountingService)cluster.node(1).service()).messageSinceStart() == 0);
            assertThat(((MessageCountingService)cluster.node(2).service()).messageSinceStart(), is(equalTo(0)));
        }
    }

    @Test(timeout = 30_000)
//    @Ignore
    public void testMultipleSnapshotsWithEmptyFollowerLog() throws Exception
    {
        final int memberCount = 3;
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(APPOINTED_LEADER))
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
            // first snapshot is done
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
            assertTrue(((MessageCountingService)cluster.node(0).service()).messageCount() == 3);
            assertTrue(((MessageCountingService)cluster.node(1).service()).messageCount() == 3);
            assertTrue(((MessageCountingService)cluster.node(2).service()).messageCount() == 3);

            // now lets see if all nodes work properly
            cluster.reconnectClient();
            final int msgCountAfterStart = 4;
            final int totalMsgCount = 2 + 1 + 4;
            cluster.sendMessages(msgCountAfterStart);
            cluster.awaitResponses(totalMsgCount);
            cluster.awaitMessageCountForService(newLeader, totalMsgCount);
            assertTrue(((MessageCountingService)newLeader.service()).messageCount() == totalMsgCount);

            cluster.awaitMessageCountForService(cluster.node(1), totalMsgCount);
            assertTrue(((MessageCountingService)cluster.node(1).service()).messageCount() == totalMsgCount);

            // IMPORTANT: After coming back online and reconstructing state, NODE 2 is not part of the cluster
            cluster.awaitMessageCountForService(cluster.node(2), totalMsgCount);
            assertTrue(((MessageCountingService)cluster.node(2).service()).messageCount() == totalMsgCount);
        }
    }

    void awaitMessageCountSinceStart(final AeronCluster client, final TestNode node, final int countSinceStart)
    {
        final EpochClock epochClock = client.context().aeron().context().epochClock();
        long deadlineMs = epochClock.time() + TimeUnit.SECONDS.toMillis(1);
        final MessageCountingService svc = ((MessageCountingService)node.service());
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
        transient int messagesReceivedSinceStart = 0;


        @Override
        public void onSessionMessage(final ClientSession session, final long timestampMs, final DirectBuffer buffer,
            final int offset, final int length, final Header header)
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
