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

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.Cluster;
import io.aeron.test.SlowTest;
import io.aeron.test.Tests;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.cluster.TestCluster.awaitElectionClosed;
import static io.aeron.cluster.service.CommitPos.COMMIT_POSITION_TYPE_ID;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.*;

@SlowTest
public class ClusterTest
{
    private static final String MSG = "Hello World!";

    @Test
    public void shouldStopFollowerAndRestartFollower()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                cluster.awaitLeader();

                TestNode follower = cluster.followers().get(0);

                cluster.stopNode(follower);

                Tests.sleep(1_000); // wait until existing replay can be cleaned up by conductor.

                follower = cluster.startStaticNode(follower.index(), false);

                awaitElectionClosed(follower);
                assertEquals(Cluster.Role.FOLLOWER, follower.role());
            }
        });
    }

    @Test
    public void shouldNotifyClientOfNewLeader()
    {
        assertTimeoutPreemptively(ofSeconds(40), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();

                cluster.connectClient();
                cluster.stopNode(leader);
                cluster.awaitLeadershipEvent(1);
            }
        });
    }

    @Test
    public void shouldStopLeaderAndFollowersThenRestartAllWithSnapshot()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();

                cluster.takeSnapshot(leader);
                cluster.awaitSnapshotCount(cluster.node(0), 1);
                cluster.awaitSnapshotCount(cluster.node(1), 1);
                cluster.awaitSnapshotCount(cluster.node(2), 1);

                cluster.stopAllNodes();

                cluster.restartAllNodes(false);

                cluster.awaitLeader();
                assertEquals(2, cluster.followers().size());

                cluster.awaitSnapshotLoadedForService(cluster.node(0));
                cluster.awaitSnapshotLoadedForService(cluster.node(1));
                cluster.awaitSnapshotLoadedForService(cluster.node(2));
            }
        });
    }

    @Test
    public void shouldShutdownClusterAndRestartWithSnapshots()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();

                cluster.node(0).terminationExpected(true);
                cluster.node(1).terminationExpected(true);
                cluster.node(2).terminationExpected(true);

                cluster.shutdownCluster(leader);
                cluster.awaitNodeTermination(cluster.node(0));
                cluster.awaitNodeTermination(cluster.node(1));
                cluster.awaitNodeTermination(cluster.node(2));

                assertTrue(cluster.node(0).service().wasSnapshotTaken());
                assertTrue(cluster.node(1).service().wasSnapshotTaken());
                assertTrue(cluster.node(2).service().wasSnapshotTaken());

                cluster.stopAllNodes();

                cluster.restartAllNodes(false);

                cluster.awaitLeader();
                assertEquals(2, cluster.followers().size());

                cluster.awaitSnapshotLoadedForService(cluster.node(0));
                cluster.awaitSnapshotLoadedForService(cluster.node(1));
                cluster.awaitSnapshotLoadedForService(cluster.node(2));
            }
        });
    }

    @Test
    public void shouldAbortClusterAndRestart()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();

                cluster.node(0).terminationExpected(true);
                cluster.node(1).terminationExpected(true);
                cluster.node(2).terminationExpected(true);

                cluster.abortCluster(leader);
                cluster.awaitNodeTermination(cluster.node(0));
                cluster.awaitNodeTermination(cluster.node(1));
                cluster.awaitNodeTermination(cluster.node(2));

                assertFalse(cluster.node(0).service().wasSnapshotTaken());
                assertFalse(cluster.node(1).service().wasSnapshotTaken());
                assertFalse(cluster.node(2).service().wasSnapshotTaken());

                cluster.stopAllNodes();

                cluster.restartAllNodes(false);

                cluster.awaitLeader();
                assertEquals(2, cluster.followers().size());

                assertFalse(cluster.node(0).service().wasSnapshotLoaded());
                assertFalse(cluster.node(1).service().wasSnapshotLoaded());
                assertFalse(cluster.node(2).service().wasSnapshotLoaded());
            }
        });
    }

    @Test
    public void shouldAbortClusterOnTerminationTimeout()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();
                final List<TestNode> followers = cluster.followers();

                assertEquals(2, followers.size());
                final TestNode followerA = followers.get(0);
                final TestNode followerB = followers.get(1);

                leader.terminationExpected(true);
                followerA.terminationExpected(true);

                cluster.stopNode(followerB);

                cluster.connectClient();

                final int messageCount = 10;
                cluster.sendMessages(messageCount);
                cluster.awaitResponseMessageCount(messageCount);

                cluster.abortCluster(leader);
                cluster.awaitNodeTermination(leader);
                cluster.awaitNodeTermination(followerA);

                cluster.stopNode(leader);
                cluster.stopNode(followerA);
            }
        });
    }

    @Test
    public void shouldEchoMessagesThenContinueOnNewLeader()
    {
        assertTimeoutPreemptively(ofSeconds(40), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode originalLeader = cluster.awaitLeader();
                cluster.connectClient();

                final int preFailureMessageCount = 10;
                final int postFailureMessageCount = 7;

                cluster.sendMessages(preFailureMessageCount);
                cluster.awaitResponseMessageCount(preFailureMessageCount);
                cluster.awaitServiceMessageCount(cluster.node(0), preFailureMessageCount);
                cluster.awaitServiceMessageCount(cluster.node(1), preFailureMessageCount);
                cluster.awaitServiceMessageCount(cluster.node(2), preFailureMessageCount);

                assertEquals(originalLeader.index(), cluster.client().leaderMemberId());

                cluster.stopNode(originalLeader);

                final TestNode newLeader = cluster.awaitLeader(originalLeader.index());

                cluster.sendMessages(postFailureMessageCount);
                cluster.awaitResponseMessageCount(preFailureMessageCount + postFailureMessageCount);
                assertEquals(newLeader.index(), cluster.client().leaderMemberId());

                final TestNode follower = cluster.followers().get(0);

                cluster.awaitServiceMessageCount(newLeader, preFailureMessageCount + postFailureMessageCount);
                cluster.awaitServiceMessageCount(follower, preFailureMessageCount + postFailureMessageCount);
            }
        });
    }

    @Test
    public void shouldStopLeaderAndRestartAsFollower()
    {
        assertTimeoutPreemptively(ofSeconds(40), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode originalLeader = cluster.awaitLeader();

                cluster.stopNode(originalLeader);
                cluster.awaitLeader(originalLeader.index());

                final TestNode follower = cluster.startStaticNode(originalLeader.index(), false);

                awaitElectionClosed(follower);
                assertEquals(Cluster.Role.FOLLOWER, follower.role());
            }
        });
    }

    @Test
    public void shouldStopLeaderAndRestartAsFollowerWithSendingAfter()
    {
        assertTimeoutPreemptively(ofSeconds(40), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode originalLeader = cluster.awaitLeader();

                cluster.stopNode(originalLeader);
                cluster.awaitLeader(originalLeader.index());

                final TestNode follower = cluster.startStaticNode(originalLeader.index(), false);

                awaitElectionClosed(follower);
                assertEquals(Cluster.Role.FOLLOWER, follower.role());

                cluster.connectClient();

                final int messageCount = 10;
                cluster.sendMessages(messageCount);
                cluster.awaitResponseMessageCount(messageCount);
            }
        });
    }

    @Test
    public void shouldStopLeaderAndRestartAsFollowerWithSendingAfterThenStopLeader()
    {
        assertTimeoutPreemptively(ofSeconds(60), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode originalLeader = cluster.awaitLeader();

                cluster.stopNode(originalLeader);
                cluster.awaitLeader(originalLeader.index());

                final TestNode follower = cluster.startStaticNode(originalLeader.index(), false);
                awaitElectionClosed(follower);

                assertEquals(Cluster.Role.FOLLOWER, follower.role());

                cluster.connectClient();

                final int messageCount = 10;
                cluster.sendMessages(messageCount);
                cluster.awaitResponseMessageCount(messageCount);

                final TestNode leader = cluster.awaitLeader();

                cluster.stopNode(leader);

                cluster.awaitLeader(leader.index());
            }
        });
    }

    @Test
    public void shouldAcceptMessagesAfterSingleNodeCleanRestart()
    {
        assertTimeoutPreemptively(ofSeconds(40), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                cluster.awaitLeader();

                TestNode follower = cluster.followers().get(0);

                cluster.stopNode(follower);

                Tests.sleep(10_000);

                follower = cluster.startStaticNode(follower.index(), true);

                awaitElectionClosed(follower);
                assertEquals(Cluster.Role.FOLLOWER, follower.role());

                cluster.connectClient();

                final int messageCount = 10;
                cluster.sendMessages(messageCount);
                cluster.awaitResponseMessageCount(messageCount);
                cluster.awaitServiceMessageCount(follower, messageCount);
            }
        });
    }

    @Test
    public void shouldReplaySnapshotTakenWhileDown()
    {
        assertTimeoutPreemptively(ofSeconds(40), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();
                final TestNode followerA = cluster.followers().get(0);
                TestNode followerB = cluster.followers().get(1);

                cluster.stopNode(followerB);

                Tests.sleep(10_000);

                cluster.takeSnapshot(leader);
                cluster.awaitSnapshotCount(leader, 1);
                cluster.awaitSnapshotCount(followerA, 1);

                cluster.connectClient();

                final int messageCount = 10;
                cluster.sendMessages(messageCount);
                cluster.awaitResponseMessageCount(messageCount);

                followerB = cluster.startStaticNode(followerB.index(), false);

                cluster.awaitSnapshotCount(followerB, 1);
                assertEquals(Cluster.Role.FOLLOWER, followerB.role());

                cluster.awaitServiceMessageCount(followerB, messageCount);
                assertEquals(0L, followerB.errors());
            }
        });
    }

    @Test
    public void shouldTolerateMultipleLeaderFailures()
    {
        assertTimeoutPreemptively(ofSeconds(50), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode firstLeader = cluster.awaitLeader();
                cluster.stopNode(firstLeader);

                final TestNode secondLeader = cluster.awaitLeader();

                final long commitPos = secondLeader.commitPosition();
                final TestNode newFollower = cluster.startStaticNode(firstLeader.index(), false);

                cluster.awaitCommitPosition(newFollower, commitPos);
                cluster.awaitNotInElection(newFollower);

                cluster.stopNode(secondLeader);

                cluster.awaitLeader();

                cluster.connectClient();

                final int messageCount = 10;
                cluster.sendMessages(messageCount);
                cluster.awaitResponseMessageCount(messageCount);
            }
        });
    }

    @Test
    public void shouldAcceptMessagesAfterTwoNodeCleanRestart()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                cluster.awaitLeader();

                final List<TestNode> followers = cluster.followers();
                TestNode followerA = followers.get(0), followerB = followers.get(1);

                cluster.stopNode(followerA);
                cluster.stopNode(followerB);

                Tests.sleep(1_000); // wait until existing replays can be cleaned up by conductor.

                followerA = cluster.startStaticNode(followerA.index(), true);
                followerB = cluster.startStaticNode(followerB.index(), true);

                awaitElectionClosed(followerA);
                awaitElectionClosed(followerB);

                assertEquals(Cluster.Role.FOLLOWER, followerA.role());
                assertEquals(Cluster.Role.FOLLOWER, followerB.role());

                cluster.connectClient();
                final int messageCount = 10;

                cluster.sendMessages(messageCount);
                cluster.awaitResponseMessageCount(messageCount);
                cluster.awaitServiceMessageCount(followerA, messageCount);
                cluster.awaitServiceMessageCount(followerB, messageCount);
            }
        });
    }

    @Test
    public void shouldHaveOnlyOneCommitPositionCounter()
    {
        assertTimeoutPreemptively(ofSeconds(40), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();

                final List<TestNode> followers = cluster.followers();
                final TestNode followerA = followers.get(0), followerB = followers.get(1);

                cluster.stopNode(leader);

                cluster.awaitLeader(leader.index());

                assertEquals(1, countersOfType(followerA.countersReader(), COMMIT_POSITION_TYPE_ID));
                assertEquals(1, countersOfType(followerB.countersReader(), COMMIT_POSITION_TYPE_ID));
            }
        });
    }

    @Test
    public void shouldCallOnRoleChangeOnBecomingLeader()
    {
        assertTimeoutPreemptively(ofSeconds(40), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                TestNode leader = cluster.awaitLeader();

                List<TestNode> followers = cluster.followers();
                final TestNode followerA = followers.get(0);
                final TestNode followerB = followers.get(1);

                assertEquals(Cluster.Role.LEADER, leader.service().roleChangedTo());
                assertNull(followerA.service().roleChangedTo());
                assertNull(followerB.service().roleChangedTo());

                cluster.stopNode(leader);

                leader = cluster.awaitLeader(leader.index());
                followers = cluster.followers();
                final TestNode follower = followers.get(0);

                assertEquals(Cluster.Role.LEADER, leader.service().roleChangedTo());
                assertNull(follower.service().roleChangedTo());
            }
        });
    }

    @Test
    public void shouldLoseLeadershipWhenNoActiveQuorumOfFollowers()
    {
        assertTimeoutPreemptively(ofSeconds(40), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();

                final List<TestNode> followers = cluster.followers();
                final TestNode followerA = followers.get(0);
                final TestNode followerB = followers.get(1);

                assertEquals(Cluster.Role.LEADER, leader.service().roleChangedTo());

                cluster.stopNode(followerA);
                cluster.stopNode(followerB);

                while (leader.service().roleChangedTo() == Cluster.Role.LEADER)
                {
                    Thread.yield();
                    Tests.checkInterruptStatus();
                }

                assertEquals(Cluster.Role.FOLLOWER, leader.service().roleChangedTo());
            }
        });
    }

    @Test
    public void shouldRecoverWhileMessagesContinue()
    {
        assertTimeoutPreemptively(ofSeconds(70), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();

                final List<TestNode> followers = cluster.followers();
                final TestNode followerA = followers.get(0);
                TestNode followerB = followers.get(1);

                cluster.connectClient();
                final Thread messageThread = startMessageThread(cluster, TimeUnit.MICROSECONDS.toNanos(500));
                try
                {
                    cluster.stopNode(followerB);
                    Tests.sleep(10_000);

                    followerB = cluster.startStaticNode(followerB.index(), false);
                    Tests.sleep(30_000);
                }
                finally
                {
                    messageThread.interrupt();
                    messageThread.join();
                }

                awaitElectionClosed(followerB);
                assertEquals(0L, leader.errors());
                assertEquals(0L, followerA.errors());
                assertEquals(0L, followerB.errors());
            }
        });
    }

    @Test
    public void shouldCatchupFromEmptyLog()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                cluster.awaitLeader();

                final List<TestNode> followers = cluster.followers();
                TestNode followerB = followers.get(1);

                cluster.stopNode(followerB);

                cluster.connectClient();
                final int messageCount = 10;
                cluster.sendMessages(messageCount);
                cluster.awaitResponseMessageCount(messageCount);

                Tests.sleep(1_000); // wait until existing replay can be cleaned up by conductor.

                followerB = cluster.startStaticNode(followerB.index(), true);
                cluster.awaitServiceMessageCount(followerB, messageCount);
            }
        });
    }

    @Test
    public void shouldCatchupFromEmptyLogThenSnapshotAfterShutdownAndFollowerCleanStart()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();
                final List<TestNode> followers = cluster.followers();
                final TestNode followerA = followers.get(0);
                final TestNode followerB = followers.get(1);

                cluster.connectClient();
                final int messageCount = 10;
                cluster.sendMessages(messageCount);
                cluster.awaitResponseMessageCount(messageCount);

                leader.terminationExpected(true);
                followerA.terminationExpected(true);
                followerB.terminationExpected(true);

                cluster.shutdownCluster(leader);
                cluster.awaitNodeTermination(cluster.node(0));
                cluster.awaitNodeTermination(cluster.node(1));
                cluster.awaitNodeTermination(cluster.node(2));

                assertTrue(cluster.node(0).service().wasSnapshotTaken());
                assertTrue(cluster.node(1).service().wasSnapshotTaken());
                assertTrue(cluster.node(2).service().wasSnapshotTaken());

                cluster.stopAllNodes();

                cluster.startStaticNode(0, false);
                cluster.startStaticNode(1, false);
                cluster.startStaticNode(2, true);

                final TestNode newLeader = cluster.awaitLeader();

                assertNotEquals(2, newLeader.index());

                assertTrue(cluster.node(0).service().wasSnapshotLoaded());
                assertTrue(cluster.node(1).service().wasSnapshotLoaded());
                assertFalse(cluster.node(2).service().wasSnapshotLoaded());

                cluster.awaitServiceMessageCount(cluster.node(2), messageCount);
                cluster.awaitSnapshotCount(cluster.node(2), 1);
                assertTrue(cluster.node(2).service().wasSnapshotTaken());
            }
        });
    }

    @Test
    public void shouldCatchUpAfterFollowerMissesOneMessage()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
            shouldCatchUpAfterFollowerMissesMessage(TestMessages.NO_OP));
    }

    @Test
    public void shouldCatchUpAfterFollowerMissesTimerRegistration()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
            shouldCatchUpAfterFollowerMissesMessage(TestMessages.REGISTER_TIMER));
    }

    @Test
    public void shouldCatchUpTwoFreshNodesAfterRestart()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();
                final List<TestNode> followers = cluster.followers();

                cluster.connectClient();
                final int messageCount = 50_000;
                for (int i = 0; i < messageCount; i++)
                {
                    cluster.msgBuffer().putStringWithoutLengthAscii(0, TestMessages.NO_OP);
                    cluster.sendMessage(TestMessages.NO_OP.length());
                }
                cluster.awaitResponseMessageCount(messageCount);

                cluster.node(0).terminationExpected(true);
                cluster.node(1).terminationExpected(true);
                cluster.node(2).terminationExpected(true);

                cluster.abortCluster(leader);
                cluster.awaitNodeTermination(cluster.node(0));
                cluster.awaitNodeTermination(cluster.node(1));
                cluster.awaitNodeTermination(cluster.node(2));

                cluster.stopAllNodes();

                final TestNode oldLeader = cluster.startStaticNode(leader.index(), false);
                final TestNode oldFollower1 = cluster.startStaticNode(followers.get(0).index(), true);
                final TestNode oldFollower2 = cluster.startStaticNode(followers.get(1).index(), true);

                cluster.awaitLeader();
                awaitElectionClosed(oldFollower1);
                awaitElectionClosed(oldFollower2);

                assertEquals(0L, oldLeader.errors());
                assertEquals(0L, oldFollower1.errors());
                assertEquals(0L, oldFollower2.errors());
            }
        });
    }

    @Test
    public void shouldReplayMultipleSnapshotsWithEmptyFollowerLog()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();
                final List<TestNode> followers = cluster.followers();
                final TestNode followerA = followers.get(0);
                final TestNode followerB = followers.get(1);

                cluster.connectClient();

                cluster.sendMessages(2);
                cluster.awaitResponseMessageCount(2);
                cluster.awaitServiceMessageCount(cluster.node(2), 2);

                cluster.takeSnapshot(leader);
                final int memberCount = 3;
                for (int memberId = 0; memberId < memberCount; memberId++)
                {
                    final TestNode node = cluster.node(memberId);
                    cluster.awaitSnapshotCount(node, 1);
                    assertTrue(node.service().wasSnapshotTaken());
                    node.service().resetSnapshotTaken();
                }

                cluster.sendMessages(1);
                cluster.awaitResponseMessageCount(3);
                cluster.awaitServiceMessageCount(cluster.node(2), 3);

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

                cluster.stopAllNodes();

                cluster.startStaticNode(0, false);
                cluster.startStaticNode(1, false);
                cluster.startStaticNode(2, true);

                final TestNode newLeader = cluster.awaitLeader();

                assertNotEquals(2, newLeader.index());

                assertTrue(cluster.node(0).service().wasSnapshotLoaded());
                assertTrue(cluster.node(1).service().wasSnapshotLoaded());
                assertFalse(cluster.node(2).service().wasSnapshotLoaded());

                assertEquals(3, cluster.node(0).service().messageCount());
                assertEquals(3, cluster.node(1).service().messageCount());
                assertEquals(3, cluster.node(2).service().messageCount());

                cluster.reconnectClient();

                final int msgCountAfterStart = 4;
                final int totalMsgCount = 2 + 1 + 4;
                cluster.sendMessages(msgCountAfterStart);
                cluster.awaitResponseMessageCount(totalMsgCount);
                cluster.awaitServiceMessageCount(newLeader, totalMsgCount);
                assertEquals(totalMsgCount, newLeader.service().messageCount());

                cluster.awaitServiceMessageCount(cluster.node(1), totalMsgCount);
                assertEquals(totalMsgCount, cluster.node(1).service().messageCount());

                cluster.awaitServiceMessageCount(cluster.node(2), totalMsgCount);
                assertEquals(totalMsgCount, cluster.node(2).service().messageCount());
            }
        });
    }

    @Test
    public void shouldRecoverQuicklyAfterKillingFollowersThenRestartingOne()
    {
        assertTimeoutPreemptively(ofSeconds(40), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                cluster.awaitLeader();

                final TestNode leader = cluster.findLeader();
                final TestNode follower = cluster.followers().get(0);
                final TestNode follower2 = cluster.followers().get(1);

                cluster.connectClient();
                cluster.sendMessages(10);

                cluster.stopNode(follower);
                cluster.stopNode(follower2);

                while (leader.role() != Cluster.Role.FOLLOWER)
                {
                    Tests.sleep(1_000);
                    cluster.sendMessages(1);
                }

                cluster.startStaticNode(follower2.index(), true);
                cluster.awaitLeader();
            }
        });
    }

    @Test
    void shouldRecoverWhenLastSnapshotIsMarkedInvalid()
    {
        assertTimeoutPreemptively(ofSeconds(40), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                // Leadership Term 0
                final TestNode leader0 = cluster.awaitLeader();
                cluster.connectClient();

                final int numMessages = 3;
                cluster.sendMessages(numMessages);
                awaitAllServiceMessageCount(cluster, 3, numMessages);

                // Snapshot
                cluster.takeSnapshot(leader0);
                cluster.awaitSnapshotCount(leader0, 1);

                cluster.sendMessages(numMessages);
                awaitAllServiceMessageCount(cluster, 3, numMessages * 2);

                // Snapshot
                cluster.takeSnapshot(leader0);
                cluster.awaitSnapshotCount(leader0, 2);

                // Leadership Term 1
                cluster.stopNode(leader0);
                cluster.awaitLeader(leader0.index());
                cluster.startStaticNode(leader0.index(), false);

                cluster.sendMessages(numMessages);
                awaitAllServiceMessageCount(cluster, 3, numMessages * 3);

                // Stop without snapshot
                cluster.node(0).terminationExpected(true);
                cluster.node(1).terminationExpected(true);
                cluster.node(2).terminationExpected(true);

                cluster.stopAllNodes();

                // Invalidate snapshot from leadershipTermId = 1
                cluster.invalidateLatestSnapshots();

                // Start, should replay from snapshot in leadershipTerm = 0.
                cluster.restartAllNodes(false);

                cluster.awaitLeader();
            }
        });
    }

    @Test
    void shouldRecoverWhenLastSnapshotIsInvalidAndWasBetweenTwoElections()
    {
        assertTimeoutPreemptively(ofSeconds(50), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                // Leadership Term 0
                final TestNode leader0 = cluster.awaitLeader();
                cluster.connectClient();

                final int numMessages = 3;
                cluster.sendMessages(numMessages);
                awaitAllServiceMessageCount(cluster, 3, numMessages);

                // Leadership Term 1
                cluster.stopNode(leader0);
                final TestNode leader1 = cluster.awaitLeader(leader0.index());
                cluster.startStaticNode(leader0.index(), false);

                cluster.sendMessages(numMessages);
                awaitAllServiceMessageCount(cluster, 3, numMessages * 2);

                // Snapshot
                cluster.takeSnapshot(leader1);
                cluster.awaitSnapshotCount(leader1, 1);

                // Leadership Term 2
                cluster.stopNode(leader1);
                cluster.awaitLeader(leader1.index());
                cluster.startStaticNode(leader1.index(), false);

                cluster.sendMessages(numMessages);
                awaitAllServiceMessageCount(cluster, 3, numMessages * 3);

                // No snapshot for Term 2

                // Stop without snapshot
                cluster.node(0).terminationExpected(true);
                cluster.node(1).terminationExpected(true);
                cluster.node(2).terminationExpected(true);

                cluster.stopAllNodes();

                // Invalidate snapshot from leadershipTermId = 1
                cluster.invalidateLatestSnapshots();

                // Start, should replay from snapshot in leadershipTerm = 0.
                cluster.restartAllNodes(false);

                cluster.awaitLeader();
            }
        });
    }

    private void awaitAllServiceMessageCount(final TestCluster cluster, final int numNodes, final int numMessages)
    {
        for (int i = 0; i < numNodes; i++)
        {
            cluster.awaitServiceMessageCount(cluster.node(i), numMessages);
        }
    }

    private void shouldCatchUpAfterFollowerMissesMessage(final String message) throws InterruptedException
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();
            TestNode follower = cluster.followers().get(0);

            cluster.stopNode(follower);

            awaitElectionClosed(leader);

            cluster.connectClient();
            cluster.msgBuffer().putStringWithoutLengthAscii(0, message);
            cluster.sendMessage(message.length());
            cluster.awaitResponseMessageCount(1);

            Tests.sleep(1_000); // wait until existing replay can be cleaned up by conductor.

            follower = cluster.startStaticNode(follower.index(), false);

            awaitElectionClosed(follower);
            assertEquals(Cluster.Role.FOLLOWER, follower.role());
        }
    }

    private int countersOfType(final CountersReader countersReader, final int typeIdToCount)
    {
        final MutableInteger count = new MutableInteger();

        countersReader.forEach(
            (counterId, typeId, keyBuffer, label) ->
            {
                if (typeId == typeIdToCount)
                {
                    count.value++;
                }
            });

        return count.get();
    }

    private Thread startMessageThread(final TestCluster cluster, final long intervalNs)
    {
        final Thread thread = new Thread(
            () ->
            {
                final IdleStrategy idleStrategy = YieldingIdleStrategy.INSTANCE;
                final AeronCluster client = cluster.client();
                final ExpandableArrayBuffer msgBuffer = cluster.msgBuffer();
                msgBuffer.putStringWithoutLengthAscii(0, MSG);

                while (!Thread.interrupted())
                {
                    if (client.offer(msgBuffer, 0, MSG.length()) < 0)
                    {
                        LockSupport.parkNanos(intervalNs);
                    }

                    idleStrategy.idle(client.pollEgress());
                }
            });

        thread.setDaemon(true);
        thread.setName("message-thread");

        return thread;
    }
}
