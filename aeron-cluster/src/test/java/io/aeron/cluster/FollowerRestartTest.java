/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.aeron.cluster;

import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.TestNode.TestService;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import static org.agrona.BitUtil.SIZE_OF_INT;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.EpochClock;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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
    public void testSingleSnapshotDuringShutdownWithEmptyFollowerLog() throws Exception
    {
        final int memberCount = 3;
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(APPOINTED_LEADER, MyService::new))
        {
            final TestNode leader = cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();
            final TestNode followerA = followers.get(0);
            final TestNode followerB = followers.get(1);

            cluster.connectClient();

            sendMessageAdd(cluster.client(), 2);
            sendMessageAdd(cluster.client(), 5);
            sendMessageAdd(cluster.client(), 9);
            sendMessageAdd(cluster.client(), 17);
            cluster.awaitResponses(4);
            sendMessageRemove(cluster.client(), 5);
            sendMessageRemove(cluster.client(), 9);
            cluster.awaitResponses(6);
            sendMessageAdd(cluster.client(), 1);
            cluster.awaitResponses(7);

            leader.terminationExpected(true);
            followerA.terminationExpected(true);
            followerB.terminationExpected(true);

            final int expectedSum = 2 + 17 + 1;
            awaitStateForService(cluster.client(), cluster.node(0), 7, expectedSum);
            awaitStateForService(cluster.client(), cluster.node(1), 7, expectedSum);
            awaitStateForService(cluster.client(), cluster.node(2), 7, expectedSum);

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

            cluster.startStaticNode(2, true, MyService::new);
            cluster.startStaticNode(0, false, MyService::new);
            cluster.startStaticNode(1, false, MyService::new);

            final TestNode newLeader = cluster.awaitLeader();

            assertNotEquals(newLeader.index(), is(2));

            assertTrue(cluster.node(0).service().wasSnapshotLoaded());
            assertTrue(cluster.node(1).service().wasSnapshotLoaded());
            assertFalse(cluster.node(2).service().wasSnapshotLoaded());

            awaitStateForService(cluster.client(), cluster.node(0), 7, expectedSum);
            awaitStateForService(cluster.client(), cluster.node(1), 7, expectedSum);
            awaitStateForService(cluster.client(), cluster.node(2), 7, expectedSum);

            // the two nodes with their own state recovered from a snapshot.
            assertTrue(((MyService)cluster.node(0).service()).messageSinceStart() == 0);
            assertTrue(((MyService)cluster.node(1).service()).messageSinceStart() == 0);

            // I'd expect NODE 2 recovers from the most recent snapshot + journal (which should be zero length because
            // of the clean shutdown)
            // but this is not the case: journal since the beginning of times, and processes up to 7 messages.
            assertTrue(((MyService)cluster.node(2).service()).messageSinceStart() == 0);

            // and also creates its own snapshot
            cluster.awaitSnapshotCounter(cluster.node(2), 1);
            assertTrue(cluster.node(2).service().wasSnapshotTaken());

            cluster.reconnectClient();
            sendMessageAdd(cluster.client(), 3);
            cluster.awaitResponses(8);
            final int newExpectedSum = expectedSum + 3;
            awaitStateForService(cluster.client(), cluster.node(0), 8, newExpectedSum);
            assertTrue(((MyService)cluster.node(0).service()).messageSinceStart() == 1);
            awaitStateForService(cluster.client(), cluster.node(1), 8, newExpectedSum);
            awaitStateForService(cluster.client(), cluster.node(2), 8, newExpectedSum);
        }
    }

    @Test(timeout = 30_000)
    @Ignore
    public void testMultipleSnapshotsWithEmptyFollowerLog() throws Exception
    {
        final int memberCount = 3;
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(APPOINTED_LEADER, MyService::new))
        {
            final TestNode leader = cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();
            final TestNode followerA = followers.get(0);
            final TestNode followerB = followers.get(1);

            cluster.connectClient();

            sendMessageAdd(cluster.client(), 2);
            sendMessageAdd(cluster.client(), 5);
            sendMessageAdd(cluster.client(), 9);
            sendMessageAdd(cluster.client(), 17);
            cluster.awaitResponses(4);

            cluster.takeSnapshot(leader);
            for (int memberId = 0; memberId < memberCount; memberId++)
            {
                final TestNode node = cluster.node(memberId);
                cluster.awaitSnapshotCounter(node, 1);
                assertTrue(node.service().wasSnapshotTaken());
                node.service().resetSnapshotTaken();
            }
            // first snapshot is done
            sendMessageRemove(cluster.client(), 5);
            sendMessageRemove(cluster.client(), 9);
            cluster.awaitResponses(6);
            sendMessageAdd(cluster.client(), 1);
            cluster.awaitResponses(7);


            final int expectedSum = 2 + 17 + 1;
            awaitStateForService(cluster.client(), cluster.node(0), 7, expectedSum);
            awaitStateForService(cluster.client(), cluster.node(1), 7, expectedSum);
            awaitStateForService(cluster.client(), cluster.node(2), 7, expectedSum);

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
            // shutdown including the 2nd snapshot is completed
            Thread.sleep(1_000);

            cluster.startStaticNode(2, true, MyService::new);
            cluster.startStaticNode(0, false, MyService::new);
            cluster.startStaticNode(1, false, MyService::new);

            // NOTE the exception in logs at this point
            final TestNode newLeader = cluster.awaitLeader();

            assertNotEquals(newLeader.index(), is(2));

            assertTrue(cluster.node(0).service().wasSnapshotLoaded());
            assertTrue(cluster.node(1).service().wasSnapshotLoaded());
            assertFalse(cluster.node(2).service().wasSnapshotLoaded());

            awaitStateForService(cluster.client(), newLeader, 7, expectedSum);
            awaitStateForService(cluster.client(), cluster.followers().get(0), 7, expectedSum);
            awaitStateForService(cluster.client(), cluster.followers().get(1), 7, expectedSum);

            // the two nodes with their own state should recover from the most recent snapshot.
            assertTrue(((MyService)cluster.node(0).service()).messageSinceStart() == 0);
            assertTrue(((MyService)cluster.node(1).service()).messageSinceStart() == 0);

            // assuming same behavior as in the other test case, NODE 2 receiving all 7 message.
            assertTrue(((MyService)cluster.node(2).service()).messageSinceStart() == 7);

            // logging shows NODE 2 creating 2 snapshots while doing the replay
            // but the counter is not reflecting this, but this is not the point
//            cluster.awaitSnapshotCounter(cluster.node(2), 2);

            // IMPORTANT: NODE 2 is inoperational at this time.
            cluster.reconnectClient();
            sendMessageAdd(cluster.client(), 3);
            cluster.awaitResponses(8);
            final int newExpectedSum = expectedSum + 3;
            awaitStateForService(cluster.client(), newLeader, 8, newExpectedSum);
            assertTrue(((MyService)cluster.node(0).service()).messageSinceStart() == 1);
            awaitStateForService(cluster.client(), cluster.followers().get(0), 8, newExpectedSum);
            awaitStateForService(cluster.client(), cluster.followers().get(1), 8, newExpectedSum);
        }
    }

    /**
     * Sends an ADD-type of message with the desired value
     * @param client the clusterClient
     * @param value value to add
     */
    void sendMessageAdd(final AeronCluster client, final int value)
    {
        final String msg = ADD_MSG + value;
        final int len = msgBuffer.putStringWithoutLengthAscii(0, msg);
        sendMessage(client, len);
    }

    /**
     * Sends a REMOVE-type of message with the desired value
     * @param client the clusterClient
     * @param value value to subtract
     */
    void sendMessageRemove(final AeronCluster client, final int value)
    {
        final String msg = REMOVE_MSG + value;
        final int len = msgBuffer.putStringWithoutLengthAscii(0, msg);
        sendMessage(client, len);
    }

    void sendMessage(final AeronCluster client, final int messageLength)
    {
        while (client.offer(msgBuffer, 0, messageLength) < 0)
        {
            TestUtil.checkInterruptedStatus();
            client.pollEgress();
            Thread.yield();
        }

        client.pollEgress();
    }

    void awaitStateForService(final AeronCluster client, final TestNode node, final int msgCount, final int sum)
    {
        final EpochClock epochClock = client.context().aeron().context().epochClock();
        long deadlineMs = epochClock.time() + TimeUnit.SECONDS.toMillis(1);
        final MyService svc = ((MyService)node.service());
        while (svc.messageCount() < msgCount && svc.sum != sum)
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

    static class MyService extends TestService
    {
        private static final int SNAPSHOT_HEADER_TOTAL_COUNT = 0x01;
        private static final int SNAPSHOT_HEADER_SUM = 0x02;
        private static final int SNAPSHOT_HEADER_MESSAGES = 0x03;
        private static final int SNAPSHOT_HEADER_MESSAGES_LEN = 0x04;

        volatile int totalMsgCount = 0;
        transient int messagesReceivedSinceStart = 0;
        volatile int sum = 0;
        Set<Integer> messages = new HashSet<>();

        private volatile boolean wasSnapshotTaken = false;
        private volatile boolean wasSnapshotLoaded = false;

        MyService(final int index)
        {
            super(index);
        }

        @Override
        boolean wasSnapshotTaken()
        {
            return wasSnapshotTaken;
        }

        @Override
        boolean wasSnapshotLoaded()
        {
            return wasSnapshotLoaded;
        }

        @Override
        void resetSnapshotTaken()
        {
            wasSnapshotTaken = false;
        }

        @Override
        public void onSessionMessage(final ClientSession session, final long timestampMs, final DirectBuffer buffer,
            final int offset, final int length, final Header header)
        {
            final String message = buffer.getStringWithoutLengthAscii(offset, length);

            if (message.startsWith(ADD_MSG))
            {
                final int num = Integer.valueOf(message.substring(ADD_MSG.length()));
                sum += num;
                messages.add(num);
            }
            else if (message.startsWith(REMOVE_MSG))
            {
                final int num = Integer.valueOf(message.substring(REMOVE_MSG.length()));
                sum -= num;
                messages.remove(num);
            }

            if (null != session)
            {
                while (session.offer(buffer, offset, length) < 0)
                {
                    cluster.idle();
                }
            }
            totalMsgCount++;
            messagesReceivedSinceStart++;
//            System.out.println(MessageFormat.format("Node: {0} msg RECEIVED {4}: msgCount={1}, sum={2}, listLen={3}.",
//                    new Object[] {index(), totalMsgCount, sum, messages.size(), message}));
        }

        @Override
        public void onTakeSnapshot(final Publication snapshotPublication)
        {
            final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

            int length = 0;
            buffer.putInt(length, SNAPSHOT_HEADER_TOTAL_COUNT);
            length += SIZE_OF_INT;
            buffer.putInt(length, totalMsgCount);
            length += SIZE_OF_INT;
            long res = snapshotPublication.offer(buffer, 0, length);
            assertTrue(res > 0);
            cluster.idle();
            buffer.setMemory(0, length, (byte)0);

            length = 0;
            buffer.putInt(length, SNAPSHOT_HEADER_SUM);
            length += SIZE_OF_INT;
            buffer.putInt(length, sum);
            length += SIZE_OF_INT;
            res = snapshotPublication.offer(buffer, 0, length);
            assertTrue(res > 0);
            cluster.idle();
            buffer.setMemory(0, length, (byte)0);


            length = 0;
            buffer.putInt(length, SNAPSHOT_HEADER_MESSAGES);
            length += SIZE_OF_INT;
            buffer.putInt(length, SNAPSHOT_HEADER_MESSAGES_LEN);
            length += SIZE_OF_INT;
            buffer.putInt(length, messages.size());
            length += SIZE_OF_INT;
            for (final Integer message : messages)
            {
                buffer.putInt(length, message);
                length += SIZE_OF_INT;
            }
            res = snapshotPublication.offer(buffer, 0, length);
            assertTrue(res > 0);
            cluster.idle();
            System.out.println(MessageFormat.format("Node: {0} Snapshot WRITE: msgCount={1}, sum={2}, listLen={3}.",
                new Object[] {index(), totalMsgCount, sum, messages.size()}));
            wasSnapshotTaken = true;
        }

        @Override
        public void onStart(final Cluster cluster, final Image snapshotImage)
        {
            super.onStart(cluster, null);
            if (snapshotImage != null)
            {
                loadSnapshot(snapshotImage);
            }
        }

        public void loadSnapshot(final Image snapshotImage)
        {
            final FragmentHandler handler = (buffer, offset, length, header) ->
            {
                final int msgTypeId = buffer.getInt(offset);
                switch (msgTypeId)
                {
                    case SNAPSHOT_HEADER_TOTAL_COUNT:
                        totalMsgCount = buffer.getInt(offset + SIZE_OF_INT);
                        break;
                    case SNAPSHOT_HEADER_SUM:
                        sum = buffer.getInt(offset + SIZE_OF_INT);
                        break;
                    case SNAPSHOT_HEADER_MESSAGES:
                        int readOffset = offset + SIZE_OF_INT;
                        final int arrayLen = buffer.getInt(readOffset);
                        for (int i = 0; i < arrayLen; i++)
                        {
                            readOffset += SIZE_OF_INT;
                            messages.add(buffer.getInt(readOffset));
                        }
                        break;
                    default:
                        throw new IllegalStateException("Unknown msgType in snapshot");
                }
            };

            while (true)
            {
                final int fragments = snapshotImage.poll(handler, 1);

                if (snapshotImage.isClosed() || snapshotImage.isEndOfStream())
                {
                    break;
                }

                cluster.idle(fragments);
            }

            System.out.println(MessageFormat.format("Node: {0} Snapshot READ: msgCount={1}, sum={2}, listLen={3}.",
                new Object[] {index(), totalMsgCount, sum, messages.size()}));
            wasSnapshotLoaded = true;
        }

        public int getSum()
        {
            return sum;
        }

        public Set<Integer> getMessages()
        {
            return messages;
        }

        @Override
        int messageCount()
        {
            return totalMsgCount;
        }

        int messageSinceStart()
        {
            return messagesReceivedSinceStart;
        }
    }
}
