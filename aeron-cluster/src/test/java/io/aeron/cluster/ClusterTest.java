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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.cluster.TestCluster.awaitElectionClosed;
import static org.junit.jupiter.api.Assertions.*;

@SlowTest
public class ClusterTest
{
    @Test
    @Timeout(30)
    public void shouldStopFollowerAndRestartFollower()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            cluster.awaitLeader();
            TestNode follower = cluster.followers().get(0);

            awaitElectionClosed(follower);
            cluster.stopNode(follower);

            Tests.sleep(1_000); // wait until existing replay can be cleaned up by conductor.

            follower = cluster.startStaticNode(follower.index(), false);

            awaitElectionClosed(follower);
            assertEquals(Cluster.Role.FOLLOWER, follower.role());
        }
    }

    @Test
    @Timeout(40)
    public void shouldNotifyClientOfNewLeader()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.connectClient();
            cluster.awaitActiveSessionCount(cluster.followers().get(0), 1);

            cluster.stopNode(leader);
            cluster.awaitLeadershipEvent(1);
        }
    }

    @Test
    @Timeout(30)
    public void shouldStopLeaderAndFollowersThenRestartAllWithSnapshot()
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
    }

    @Test
    @Timeout(30)
    public void shouldShutdownClusterAndRestartWithSnapshots()
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
    }

    @Test
    @Timeout(30)
    public void shouldAbortClusterAndRestart()
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
    }

    @Test
    @Timeout(30)
    public void shouldAbortClusterOnTerminationTimeout()
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
    }

    @Test
    @Timeout(40)
    public void shouldEchoMessagesThenContinueOnNewLeader()
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
    }

    @Test
    @Timeout(40)
    public void shouldStopLeaderAndRestartAsFollower()
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
    }

    @Test
    @Timeout(40)
    public void shouldStopLeaderAndRestartAsFollowerWithSendingAfter()
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
    }

    @Test
    @Timeout(60)
    public void shouldStopLeaderAndRestartAsFollowerWithSendingAfterThenStopLeader()
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
    }

    @Test
    @Timeout(40)
    public void shouldAcceptMessagesAfterSingleNodeCleanRestart()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            cluster.awaitLeader();
            TestNode follower = cluster.followers().get(0);

            awaitElectionClosed(follower);
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
    }

    @Test
    @Timeout(40)
    public void shouldReplaySnapshotTakenWhileDown()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();
            final TestNode followerA = cluster.followers().get(0);
            TestNode followerB = cluster.followers().get(1);

            awaitElectionClosed(followerB);
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
    }

    @Test
    @Timeout(50)
    public void shouldTolerateMultipleLeaderFailures()
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
    }

    @Test
    @Timeout(120)
    public void shouldRecoverAfterTwoLeadersNodesFailAndComeBackUpAtSameTime()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode firstLeader = cluster.awaitLeader();

            final int messageCount = 1_000_000; // Add enough messages so replay takes some time
            cluster.connectClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);

            cluster.closeClient();

            cluster.awaitActiveSessionCount(cluster.followers().get(0), 0);
            cluster.awaitActiveSessionCount(cluster.followers().get(0), 0);

            cluster.stopNode(firstLeader);

            final TestNode secondLeader = cluster.awaitLeader();
            cluster.stopNode(secondLeader);

            final TestNode restartedFirstLeader = cluster.startStaticNode(firstLeader.index(), false);
            final TestNode restartedSecondLeader = cluster.startStaticNode(secondLeader.index(), false);

            cluster.awaitNotInElection(restartedFirstLeader);
            cluster.awaitNotInElection(restartedSecondLeader);

            final TestNode leader = cluster.awaitLeader();

            cluster.connectClient();
            cluster.sendMessages(10);
            cluster.awaitResponseMessageCount(messageCount + 10);

            cluster.awaitServiceMessageCount(leader, messageCount + 10);
            cluster.awaitServiceMessageCount(cluster.followers().get(0), messageCount + 10);
            cluster.awaitServiceMessageCount(cluster.followers().get(1), messageCount + 10);
        }
    }

    @Test
    @Timeout(30)
    public void shouldAcceptMessagesAfterTwoNodeCleanRestart()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();
            TestNode followerA = followers.get(0), followerB = followers.get(1);

            awaitElectionClosed(followerA);
            awaitElectionClosed(followerB);

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
    }

    @Test
    @Timeout(30)
    public void shouldRecoverWithUncommittedMessagesAfterRestartWhenNewCommitPosExceedsPreviousAppendedPos()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();
            TestNode followerA = followers.get(0), followerB = followers.get(1);

            cluster.connectClient();

            cluster.stopNode(followerA);
            cluster.stopNode(followerB);

            cluster.sendPoisonMessages(10);

            while (leader.appendPosition() <= leader.commitPosition())
            {
                Thread.yield();
                Tests.checkInterruptStatus();
            }

            final long targetPosition = leader.appendPosition();
            cluster.stopNode(leader);
            cluster.closeClient();

            followerA = cluster.startStaticNode(followerA.index(), false);
            followerB = cluster.startStaticNode(followerB.index(), false);

            cluster.awaitLeader();
            cluster.connectClient();

            final int messageLength = 128;
            int messageCount = 0;
            while (followerA.commitPosition() < targetPosition)
            {
                cluster.sendMessage(messageLength);
                messageCount++;
            }

            cluster.awaitResponseMessageCount(messageCount);
            cluster.awaitServiceMessageCount(followerA, messageCount);
            cluster.awaitServiceMessageCount(followerB, messageCount);

            final TestNode oldLeader = cluster.startStaticNode(leader.index(), false);
            cluster.awaitServiceMessageCount(oldLeader, messageCount);
        }
    }

    @Test
    @Timeout(30)
    public void shouldRecoverWithUncommittedMessagesAfterRestartWhenNewCommitPosIsLessThanPreviousAppendedPos()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();
            TestNode followerA = followers.get(0), followerB = followers.get(1);

            cluster.connectClient();

            cluster.stopNode(followerA);
            cluster.stopNode(followerB);

            cluster.sendPoisonMessages(10);

            while (leader.appendPosition() <= leader.commitPosition())
            {
                Thread.yield();
                Tests.checkInterruptStatus();
            }

            cluster.closeClient();
            cluster.stopNode(leader);

            followerA = cluster.startStaticNode(followerA.index(), false);
            followerB = cluster.startStaticNode(followerB.index(), false);

            cluster.awaitLeader();

            final TestNode oldLeader = cluster.startStaticNode(leader.index(), false);

            Tests.sleep(1000);

            cluster.connectClient();

            final int messageCount = 10;
            cluster.sendMessages(messageCount);

            cluster.awaitResponseMessageCount(messageCount);
            cluster.awaitServiceMessageCount(followerA, messageCount);
            cluster.awaitServiceMessageCount(followerB, messageCount);
            cluster.awaitServiceMessageCount(oldLeader, messageCount);
        }
    }

    @Test
    @Timeout(40)
    public void shouldCallOnRoleChangeOnBecomingLeader()
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
    }

    @Test
    @Timeout(40)
    public void shouldLoseLeadershipWhenNoActiveQuorumOfFollowers()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();
            final TestNode followerA = followers.get(0);
            final TestNode followerB = followers.get(1);

            assertEquals(Cluster.Role.LEADER, leader.service().roleChangedTo());

            awaitElectionClosed(followerA);
            awaitElectionClosed(followerB);

            cluster.stopNode(followerA);
            cluster.stopNode(followerB);

            while (leader.service().roleChangedTo() != Cluster.Role.LEADER)
            {
                Tests.sleep(1);
            }
        }
    }

    @Test
    @Timeout(40)
    public void shouldTerminateLeaderWhenServiceStops()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            leader.terminationExpected(true);
            leader.container().close();

            while (leader.moduleState() != ConsensusModule.State.CLOSED)
            {
                Tests.sleep(1);
            }

            while (!leader.hasMemberTerminated())
            {
                Tests.sleep(1);
            }
        }
    }

    @Test
    @Timeout(20)
    public void shouldCloseClientOnTimeout()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            final AeronCluster client = cluster.connectClient();
            assertEquals(0, leader.consensusModule().context().timedOutClientCounter().get());
            assertFalse(client.isClosed());

            Tests.sleep(TimeUnit.NANOSECONDS.toMillis(leader.consensusModule().context().sessionTimeoutNs()));

            while (!client.isClosed())
            {
                Tests.sleep(1);
                client.pollEgress();
            }

            assertEquals(1, leader.consensusModule().context().timedOutClientCounter().get());
        }
    }

    @Test
    @Timeout(70)
    public void shouldRecoverWhileMessagesContinue() throws InterruptedException
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();
            final TestNode followerA = followers.get(0);
            TestNode followerB = followers.get(1);

            cluster.connectClient();
            final Thread messageThread = ClusterTests.startMessageThread(cluster, TimeUnit.MICROSECONDS.toNanos(500));
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
    }

    @Test
    @Timeout(30)
    public void shouldCatchupFromEmptyLog()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();
            TestNode followerB = followers.get(1);

            awaitElectionClosed(followerB);
            cluster.stopNode(followerB);

            cluster.connectClient();
            final int messageCount = 10;
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);

            Tests.sleep(1_000); // wait until existing replay can be cleaned up by conductor.

            followerB = cluster.startStaticNode(followerB.index(), true);
            cluster.awaitServiceMessageCount(followerB, messageCount);
        }
    }

    @Test
    @Timeout(30)
    public void shouldCatchupFromEmptyLogThenSnapshotAfterShutdownAndFollowerCleanStart()
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
    }

    @Test
    @Timeout(30)
    public void shouldCatchUpTwoFreshNodesAfterRestart()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();

            cluster.connectClient();
            final int messageCount = 50_000;
            for (int i = 0; i < messageCount; i++)
            {
                cluster.msgBuffer().putStringWithoutLengthAscii(0, ClusterTests.NO_OP_MSG);
                cluster.sendMessage(ClusterTests.NO_OP_MSG.length());
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
    }

    @Test
    @Timeout(30)
    public void shouldReplayMultipleSnapshotsWithEmptyFollowerLog()
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
    }

    @Test
    @Timeout(40)
    public void shouldRecoverQuicklyAfterKillingFollowersThenRestartingOne()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();
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
    }

    @Test
    @Timeout(40)
    void shouldRecoverWhenLastSnapshotIsMarkedInvalid()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            // Leadership Term 0
            final TestNode leader0 = cluster.awaitLeader();
            cluster.connectClient();

            final int numMessages = 3;
            cluster.sendMessages(numMessages);
            cluster.awaitServicesMessageCount(numMessages);

            // Snapshot
            cluster.takeSnapshot(leader0);
            cluster.awaitSnapshotCount(leader0, 1);

            cluster.sendMessages(numMessages);
            cluster.awaitServicesMessageCount(numMessages * 2);

            // Snapshot
            cluster.takeSnapshot(leader0);
            cluster.awaitSnapshotCount(leader0, 2);

            // Leadership Term 1
            cluster.stopNode(leader0);
            cluster.awaitLeader(leader0.index());
            cluster.startStaticNode(leader0.index(), false);

            cluster.sendMessages(numMessages);
            cluster.awaitServicesMessageCount(numMessages * 3);

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
    }

    @Test
    @Timeout(60)
    void shouldHandleMultipleElections()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader0 = cluster.awaitLeader();
            cluster.connectClient();

            final int numMessages = 3;
            cluster.sendMessages(numMessages);
            cluster.awaitResponseMessageCount(numMessages);
            cluster.awaitServicesMessageCount(numMessages);

            cluster.stopNode(leader0);
            final TestNode leader1 = cluster.awaitLeader(leader0.index());
            cluster.awaitLeadershipEvent(1);
            cluster.startStaticNode(leader0.index(), false);

            cluster.sendMessages(numMessages);
            cluster.awaitResponseMessageCount(numMessages * 2);
            cluster.awaitServicesMessageCount(numMessages * 2);

            cluster.stopNode(leader1);
            cluster.awaitLeader(leader1.index());
            cluster.awaitLeadershipEvent(2);
            cluster.startStaticNode(leader1.index(), false);

            cluster.sendMessages(numMessages);
            cluster.awaitResponseMessageCount(numMessages * 3);
            cluster.awaitServicesMessageCount(numMessages * 3);
        }
    }

    @Test
    @Timeout(50)
    void shouldRecoverWhenLastSnapshotIsInvalidAndWasBetweenTwoElections()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            // Leadership Term 0
            final TestNode leader0 = cluster.awaitLeader();
            cluster.connectClient();

            final int numMessages = 3;
            cluster.sendMessages(numMessages);
            cluster.awaitServicesMessageCount(numMessages);

            // Leadership Term 1
            cluster.stopNode(leader0);
            final TestNode leader1 = cluster.awaitLeader(leader0.index());
            cluster.startStaticNode(leader0.index(), false);

            cluster.sendMessages(numMessages);
            cluster.awaitServicesMessageCount(numMessages * 2);

            // Snapshot
            cluster.takeSnapshot(leader1);
            cluster.awaitSnapshotCount(leader1, 1);

            // Leadership Term 2
            cluster.stopNode(leader1);
            cluster.awaitLeader(leader1.index());
            cluster.startStaticNode(leader1.index(), false);

            cluster.sendMessages(numMessages);
            cluster.awaitServicesMessageCount(numMessages * 3);

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
    }

    @Test
    @Timeout(30)
    public void shouldCatchUpAfterFollowerMissesOneMessage()
    {
        shouldCatchUpAfterFollowerMissesMessage(ClusterTests.NO_OP_MSG);
    }

    @Test
    @Timeout(30)
    public void shouldCatchUpAfterFollowerMissesTimerRegistration()
    {
        shouldCatchUpAfterFollowerMissesMessage(ClusterTests.REGISTER_TIMER_MSG);
    }

    private void shouldCatchUpAfterFollowerMissesMessage(final String message)
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            cluster.awaitLeader();
            TestNode follower = cluster.followers().get(0);

            cluster.stopNode(follower);

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
}
