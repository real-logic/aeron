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

import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.test.SlowTest;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.junit.jupiter.api.*;

import java.util.List;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.cluster.service.Cluster.Role.FOLLOWER;
import static io.aeron.cluster.service.Cluster.Role.LEADER;
import static io.aeron.test.cluster.ClusterTests.*;
import static io.aeron.test.cluster.TestCluster.awaitElectionClosed;
import static io.aeron.test.cluster.TestCluster.startThreeNodeStaticCluster;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.*;

@SlowTest
public class ClusterTest
{
    private TestCluster cluster = null;

    @AfterEach
    void after()
    {
        CloseHelper.close(cluster);
    }

    @Test
    @Timeout(30)
    public void shouldStopFollowerAndRestartFollower(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            cluster.awaitLeader();
            TestNode follower = cluster.followers().get(0);

            awaitElectionClosed(follower);
            cluster.stopNode(follower);

            follower = cluster.startStaticNode(follower.index(), false);

            awaitElectionClosed(follower);
            assertEquals(FOLLOWER, follower.role());
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(40)
    public void shouldNotifyClientOfNewLeader(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.connectClient();
            cluster.awaitActiveSessionCount(cluster.followers().get(0), 1);

            cluster.stopNode(leader);
            cluster.awaitNewLeadershipEvent(1);
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(30)
    public void shouldStopLeaderAndFollowersThenRestartAllWithSnapshot(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.takeSnapshot(leader);
            cluster.awaitSnapshotCount(1);

            cluster.stopAllNodes();
            cluster.restartAllNodes(false);
            cluster.awaitLeader();
            assertEquals(2, cluster.followers().size());

            cluster.awaitSnapshotsLoaded();
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(30)
    public void shouldShutdownClusterAndRestartWithSnapshots(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.node(0).isTerminationExpected(true);
            cluster.node(1).isTerminationExpected(true);
            cluster.node(2).isTerminationExpected(true);

            cluster.shutdownCluster(leader);
            cluster.awaitNodeTerminations();

            assertTrue(cluster.node(0).service().wasSnapshotTaken());
            assertTrue(cluster.node(1).service().wasSnapshotTaken());
            assertTrue(cluster.node(2).service().wasSnapshotTaken());

            cluster.stopAllNodes();
            cluster.restartAllNodes(false);
            cluster.awaitLeader();
            assertEquals(2, cluster.followers().size());

            cluster.awaitSnapshotsLoaded();
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(30)
    public void shouldAbortClusterAndRestart(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.node(0).isTerminationExpected(true);
            cluster.node(1).isTerminationExpected(true);
            cluster.node(2).isTerminationExpected(true);

            cluster.abortCluster(leader);
            cluster.awaitNodeTerminations();

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
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(30)
    public void shouldAbortClusterOnTerminationTimeout(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader = cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();

            assertEquals(2, followers.size());
            final TestNode followerA = followers.get(0);
            final TestNode followerB = followers.get(1);

            leader.isTerminationExpected(true);
            followerA.isTerminationExpected(true);

            cluster.stopNode(followerB);

            final int messageCount = 10;
            cluster.connectClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);

            cluster.abortCluster(leader);
            cluster.awaitNodeTermination(leader);
            cluster.awaitNodeTermination(followerA);

            cluster.stopNode(leader);
            cluster.stopNode(followerA);
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(40)
    public void shouldEchoMessages(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            cluster.awaitLeader();
            cluster.connectClient();

            final int expectedCount = 10;

            cluster.sendMessages(expectedCount);
            cluster.awaitResponseMessageCount(expectedCount);
            cluster.awaitServicesMessageCount(expectedCount);
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(40)
    public void shouldEchoMessagesThenContinueOnNewLeader(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode originalLeader = cluster.awaitLeader();
            cluster.connectClient();

            final int preFailureMessageCount = 10;
            final int postFailureMessageCount = 7;

            cluster.sendMessages(preFailureMessageCount);
            cluster.awaitResponseMessageCount(preFailureMessageCount);
            cluster.awaitServicesMessageCount(preFailureMessageCount);

            assertEquals(originalLeader.index(), cluster.client().leaderMemberId());

            cluster.stopNode(originalLeader);

            final TestNode newLeader = cluster.awaitLeader(originalLeader.index());
            cluster.awaitNewLeadershipEvent(1);
            assertEquals(newLeader.index(), cluster.client().leaderMemberId());

            cluster.sendMessages(postFailureMessageCount);
            cluster.awaitResponseMessageCount(preFailureMessageCount + postFailureMessageCount);

            final TestNode follower = cluster.followers().get(0);

            cluster.awaitServiceMessageCount(newLeader, preFailureMessageCount + postFailureMessageCount);
            cluster.awaitServiceMessageCount(follower, preFailureMessageCount + postFailureMessageCount);
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(40)
    public void shouldStopLeaderAndRestartAsFollower(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode originalLeader = cluster.awaitLeader();

            cluster.stopNode(originalLeader);
            cluster.awaitLeader(originalLeader.index());

            final TestNode follower = cluster.startStaticNode(originalLeader.index(), false);

            awaitElectionClosed(follower);
            assertEquals(FOLLOWER, follower.role());
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(40)
    public void shouldStopLeaderAndRestartAsFollowerWithSendingAfter(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode originalLeader = cluster.awaitLeader();

            cluster.stopNode(originalLeader);
            cluster.awaitLeader(originalLeader.index());

            final TestNode follower = cluster.startStaticNode(originalLeader.index(), false);

            awaitElectionClosed(follower);
            assertEquals(FOLLOWER, follower.role());

            final int messageCount = 10;
            cluster.connectClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(60)
    public void shouldStopLeaderAndRestartAsFollowerWithSendingAfterThenStopLeader(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode originalLeader = cluster.awaitLeader();

            cluster.stopNode(originalLeader);
            cluster.awaitLeader(originalLeader.index());

            final TestNode follower = cluster.startStaticNode(originalLeader.index(), false);
            awaitElectionClosed(follower);

            assertEquals(FOLLOWER, follower.role());

            final int messageCount = 10;
            cluster.connectClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);

            final TestNode leader = cluster.awaitLeader();
            cluster.stopNode(leader);

            cluster.awaitLeader(leader.index());
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(40)
    public void shouldAcceptMessagesAfterSingleNodeCleanRestart(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            cluster.awaitLeader();
            TestNode follower = cluster.followers().get(0);

            awaitElectionClosed(follower);
            cluster.stopNode(follower);

            follower = cluster.startStaticNode(follower.index(), true);

            awaitElectionClosed(cluster.node(follower.index()));
            assertEquals(FOLLOWER, follower.role());

            final int messageCount = 10;
            cluster.connectClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);
            cluster.awaitServiceMessageCount(follower, messageCount);
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(40)
    public void shouldReplaySnapshotTakenWhileDown(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader = cluster.awaitLeader();
            final TestNode followerA = cluster.followers().get(0);
            TestNode followerB = cluster.followers().get(1);

            awaitElectionClosed(followerB);
            cluster.stopNode(followerB);

            cluster.takeSnapshot(leader);
            cluster.awaitSnapshotCount(leader, 1);
            cluster.awaitSnapshotCount(followerA, 1);

            final int messageCount = 10;
            cluster.connectClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);

            followerB = cluster.startStaticNode(followerB.index(), false);

            cluster.awaitSnapshotCount(followerB, 1);
            assertEquals(FOLLOWER, followerB.role());

            cluster.awaitServiceMessageCount(followerB, messageCount);
            assertEquals(0L, followerB.errors());
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(50)
    public void shouldTolerateMultipleLeaderFailures(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode firstLeader = cluster.awaitLeader();
            cluster.stopNode(firstLeader);

            final TestNode secondLeader = cluster.awaitLeader();

            final long commitPos = secondLeader.commitPosition();
            final TestNode newFollower = cluster.startStaticNode(firstLeader.index(), false);

            cluster.awaitCommitPosition(newFollower, commitPos);
            awaitElectionClosed(newFollower);

            cluster.stopNode(secondLeader);
            cluster.awaitLeader();

            final int messageCount = 10;
            cluster.connectClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(90)
    public void shouldRecoverAfterTwoLeadersNodesFailAndComeBackUpAtSameTime(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode firstLeader = cluster.awaitLeader();

            final int messageCount = 1_000_000; // Add enough messages so replay takes some time
            cluster.connectClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);
            cluster.closeClient();

            cluster.awaitActiveSessionCount(firstLeader, 0);
            cluster.awaitActiveSessionCount(cluster.followers().get(0), 0);
            cluster.awaitActiveSessionCount(cluster.followers().get(1), 0);

            cluster.stopNode(firstLeader);

            final TestNode secondLeader = cluster.awaitLeader();
            cluster.stopNode(secondLeader);

            cluster.startStaticNode(firstLeader.index(), false);
            cluster.startStaticNode(secondLeader.index(), false);
            cluster.awaitLeader();

            cluster.connectClient();
            cluster.sendMessages(10);
            cluster.awaitResponseMessageCount(messageCount + 10);
            cluster.awaitServicesMessageCount(messageCount + 10);
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(30)
    public void shouldAcceptMessagesAfterTwoNodeCleanRestart(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();
            TestNode followerA = followers.get(0), followerB = followers.get(1);

            awaitElectionClosed(followerA);
            awaitElectionClosed(followerB);

            cluster.stopNode(followerA);
            cluster.stopNode(followerB);

            followerA = cluster.startStaticNode(followerA.index(), true);
            followerB = cluster.startStaticNode(followerB.index(), true);

            awaitElectionClosed(followerA);
            awaitElectionClosed(followerB);

            assertEquals(FOLLOWER, followerA.role());
            assertEquals(FOLLOWER, followerB.role());

            final int messageCount = 10;
            cluster.connectClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);
            cluster.awaitServiceMessageCount(followerA, messageCount);
            cluster.awaitServiceMessageCount(followerB, messageCount);
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(60)
    public void shouldRecoverWithUncommittedMessagesAfterRestartWhenNewCommitPosExceedsPreviousAppendedPos(
        final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader = cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();
            TestNode followerA = followers.get(0), followerB = followers.get(1);

            cluster.connectClient();

            cluster.stopNode(followerA);
            cluster.stopNode(followerB);

            cluster.sendUnexpectedMessages(10);

            final long commitPosition = leader.commitPosition();
            while (leader.appendPosition() <= commitPosition)
            {
                Tests.yield();
            }

            final long targetPosition = leader.appendPosition();
            cluster.stopNode(leader);
            cluster.closeClient();

            followerA = cluster.startStaticNode(followerA.index(), false);
            followerB = cluster.startStaticNode(followerB.index(), false);

            cluster.awaitLeader();

            awaitElectionClosed(followerA);
            awaitElectionClosed(followerB);

            cluster.connectClient();

            final int messageLength = 128;
            int messageCount = 0;
            while (followerA.commitPosition() < targetPosition)
            {
                cluster.pollUntilMessageSent(messageLength);
                messageCount++;
            }

            cluster.awaitResponseMessageCount(messageCount);
            cluster.awaitServiceMessageCount(followerA, messageCount);
            cluster.awaitServiceMessageCount(followerB, messageCount);

            final TestNode oldLeader = cluster.startStaticNode(leader.index(), false);
            cluster.awaitServiceMessageCount(oldLeader, messageCount);
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(30)
    public void shouldRecoverWithUncommittedMessagesAfterRestartWhenNewCommitPosIsLessThanPreviousAppendedPos(
        final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader = cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();
            final TestNode followerA = followers.get(0);
            final TestNode followerB = followers.get(1);

            cluster.connectClient();

            cluster.stopNode(followerA);
            cluster.stopNode(followerB);

            final int messageCount = 10;
            cluster.sendUnexpectedMessages(messageCount);

            final long commitPosition = leader.commitPosition();
            while (leader.appendPosition() <= commitPosition)
            {
                Tests.yield();
            }

            cluster.stopNode(leader);
            cluster.closeClient();

            cluster.startStaticNode(followerA.index(), false);
            cluster.startStaticNode(followerB.index(), false);
            cluster.awaitLeader();

            final TestNode oldLeader = cluster.startStaticNode(leader.index(), false);
            awaitElectionClosed(oldLeader);

            cluster.connectClient();
            cluster.sendMessages(messageCount);

            cluster.awaitResponseMessageCount(messageCount);
            cluster.awaitServicesMessageCount(messageCount);
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(40)
    public void shouldCallOnRoleChangeOnBecomingLeader(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leaderOne = cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();
            final TestNode followerA = followers.get(0);
            final TestNode followerB = followers.get(1);

            awaitElectionClosed(followerA);
            awaitElectionClosed(followerB);

            assertEquals(LEADER, leaderOne.service().roleChangedTo());
            assertNull(followerA.service().roleChangedTo());
            assertNull(followerB.service().roleChangedTo());

            cluster.stopNode(leaderOne);

            final TestNode leaderTwo = cluster.awaitLeader(leaderOne.index());
            final TestNode follower = cluster.followers().get(0);

            assertEquals(LEADER, leaderTwo.service().roleChangedTo());
            assertNull(follower.service().roleChangedTo());
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(40)
    public void shouldLoseLeadershipWhenNoActiveQuorumOfFollowers(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader = cluster.awaitLeader();
            final TestNode.TestService service = leader.service();
            final List<TestNode> followers = cluster.followers();
            final TestNode followerA = followers.get(0);
            final TestNode followerB = followers.get(1);

            assertEquals(LEADER, leader.role());
            assertEquals(LEADER, service.roleChangedTo());

            awaitElectionClosed(followerA);
            awaitElectionClosed(followerB);

            cluster.stopNode(followerA);
            cluster.stopNode(followerB);

            while (service.roleChangedTo() != FOLLOWER)
            {
                Tests.sleep(100);
            }
            assertEquals(FOLLOWER, leader.role());
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(30)
    public void shouldTerminateLeaderWhenServiceStops(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader = cluster.awaitLeader();
            cluster.connectClient();

            leader.isTerminationExpected(true);
            leader.container().close();

            while (!leader.hasMemberTerminated())
            {
                Tests.sleep(1);
            }

            cluster.awaitNewLeadershipEvent(1);
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(30)
    public void shouldEnterElectionWhenRecordingStopsOnLeader(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader = cluster.awaitLeader();
            cluster.connectClient();

            final AeronArchive.Context archiveCtx = new AeronArchive.Context()
                .controlRequestChannel(leader.archive().context().localControlChannel())
                .controlResponseChannel(leader.archive().context().localControlChannel())
                .controlRequestStreamId(leader.archive().context().localControlStreamId())
                .aeronDirectoryName(leader.mediaDriver().aeronDirectoryName());

            try (AeronArchive archive = AeronArchive.connect(archiveCtx))
            {
                final int firstRecordingIdIsTheClusterLog = 0;
                assertTrue(archive.tryStopRecordingByIdentity(firstRecordingIdIsTheClusterLog));
            }

            cluster.awaitNewLeadershipEvent(1);
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(20)
    public void shouldCloseClientOnTimeout(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader = cluster.awaitLeader();

            final AeronCluster client = cluster.connectClient();
            final ConsensusModule.Context context = leader.consensusModule().context();
            assertEquals(0, context.timedOutClientCounter().get());
            assertFalse(client.isClosed());

            Tests.sleep(NANOSECONDS.toMillis(context.sessionTimeoutNs()));

            cluster.shouldErrorOnClientClose(false);
            while (!client.isClosed())
            {
                Tests.sleep(1);
                client.pollEgress();
            }

            assertEquals(1, context.timedOutClientCounter().get());
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(40)
    public void shouldRecoverWhileMessagesContinue(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final MutableInteger messageCounter = new MutableInteger();
            final TestNode leader = cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();
            final TestNode followerA = followers.get(0);
            TestNode followerB = followers.get(1);

            cluster.connectClient();
            final long backoffIntervalNs = MICROSECONDS.toNanos(500);
            final Thread messageThread = startPublisherThread(cluster, messageCounter, backoffIntervalNs);

            try
            {
                cluster.stopNode(followerB);
                Tests.sleep(2_000); // keep ingress going so the cluster advances.

                followerB = cluster.startStaticNode(followerB.index(), false);
                Tests.sleep(2_000); // keep ingress going a while after catchup.
                awaitElectionClosed(followerB);
            }
            finally
            {
                messageThread.interrupt();
                messageThread.join();
            }

            cluster.awaitResponseMessageCount(messageCounter.get());
            cluster.client().close();
            cluster.awaitActiveSessionCount(0);

            assertEquals(0L, leader.errors());
            assertEquals(0L, followerA.errors());
            assertEquals(0L, followerB.errors());
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(30)
    public void shouldCatchupFromEmptyLog(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();
            TestNode follower = followers.get(1);

            awaitElectionClosed(follower);
            cluster.stopNode(follower);

            final int messageCount = 10;
            cluster.connectClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);

            follower = cluster.startStaticNode(follower.index(), true);
            cluster.awaitServiceMessageCount(follower, messageCount);
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(30)
    public void shouldCatchupFromEmptyLogThenSnapshotAfterShutdownAndFollowerCleanStart(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader = cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();
            final TestNode followerA = followers.get(0);
            final TestNode followerB = followers.get(1);

            final int messageCount = 10;
            cluster.connectClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);

            leader.isTerminationExpected(true);
            followerA.isTerminationExpected(true);
            followerB.isTerminationExpected(true);

            cluster.shutdownCluster(leader);
            cluster.awaitNodeTerminations();

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
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(30)
    public void shouldCatchUpTwoFreshNodesAfterRestart(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader = cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();

            final int messageCount = 50_000;
            cluster.connectClient();
            cluster.msgBuffer().putStringWithoutLengthAscii(0, NO_OP_MSG);
            for (int i = 0; i < messageCount; i++)
            {
                cluster.pollUntilMessageSent(NO_OP_MSG.length());
            }
            cluster.awaitResponseMessageCount(messageCount);

            cluster.terminationsExpected(true);
            cluster.abortCluster(leader);
            cluster.awaitNodeTerminations();

            cluster.stopAllNodes();

            final TestNode oldLeader = cluster.startStaticNode(leader.index(), false);
            final TestNode oldFollower1 = cluster.startStaticNode(followers.get(0).index(), true);
            final TestNode oldFollower2 = cluster.startStaticNode(followers.get(1).index(), true);

            cluster.awaitLeader();
            cluster.awaitServiceMessageCount(oldFollower1, messageCount);
            cluster.awaitServiceMessageCount(oldFollower2, messageCount);

            assertEquals(0L, oldLeader.errors());
            assertEquals(0L, oldFollower1.errors());
            assertEquals(0L, oldFollower2.errors());
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(30)
    public void shouldReplayMultipleSnapshotsWithEmptyFollowerLog(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader = cluster.awaitLeader();

            int messageCount = 2;
            cluster.connectClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);
            cluster.awaitServicesMessageCount(messageCount);

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
            messageCount++;
            cluster.awaitResponseMessageCount(messageCount);
            cluster.awaitServicesMessageCount(messageCount);

            cluster.terminationsExpected(true);

            cluster.awaitNeutralControlToggle(leader);
            cluster.shutdownCluster(leader);
            cluster.awaitNodeTerminations();

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

            assertEquals(messageCount, cluster.node(0).service().messageCount());
            assertEquals(messageCount, cluster.node(1).service().messageCount());
            assertEquals(messageCount, cluster.node(2).service().messageCount());

            final int messageCountAfterStart = 4;
            cluster.reconnectClient();
            cluster.sendMessages(messageCountAfterStart);
            messageCount += messageCountAfterStart;
            cluster.awaitResponseMessageCount(messageCount);
            cluster.awaitServicesMessageCount(messageCount);
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(40)
    public void shouldRecoverQuicklyAfterKillingFollowersThenRestartingOne(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader = cluster.awaitLeader();
            final TestNode followerOne = cluster.followers().get(0);
            final TestNode followerTwo = cluster.followers().get(1);

            final int messageCount = 10;
            cluster.connectClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);

            cluster.stopNode(followerOne);
            cluster.stopNode(followerTwo);

            while (leader.role() == LEADER)
            {
                cluster.sendMessages(1);
                Tests.sleep(500);
            }

            cluster.startStaticNode(followerTwo.index(), true);
            cluster.awaitLeader();
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(40)
    public void shouldRecoverWhenLeaderHasAppendedMoreThanFollower(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader = cluster.awaitLeader();
            final TestNode followerOne = cluster.followers().get(0);
            final TestNode followerTwo = cluster.followers().get(1);

            final int messageCount = 10;
            cluster.connectClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);

            cluster.stopNode(followerOne);

            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount * 2);

            cluster.stopNode(followerTwo);
            cluster.stopNode(leader);

            cluster.startStaticNode(leader.index(), false);
            cluster.startStaticNode(followerOne.index(), false);
            cluster.awaitLeader();
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(40)
    void shouldRecoverWhenLastSnapshotIsMarkedInvalid(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader0 = cluster.awaitLeader();

            final int numMessages = 3;
            cluster.connectClient();
            cluster.sendMessages(numMessages);
            cluster.awaitServicesMessageCount(numMessages);

            cluster.takeSnapshot(leader0);
            cluster.awaitSnapshotCount(1);

            cluster.sendMessages(numMessages);
            cluster.awaitServicesMessageCount(numMessages * 2);

            cluster.takeSnapshot(leader0);
            cluster.awaitSnapshotCount(2);

            cluster.stopNode(leader0);
            cluster.awaitLeader(leader0.index());
            cluster.awaitNewLeadershipEvent(1);
            assertTrue(cluster.client().sendKeepAlive());
            cluster.startStaticNode(leader0.index(), false);

            cluster.sendMessages(numMessages);
            cluster.awaitResponseMessageCount(numMessages * 3);
            cluster.awaitServicesMessageCount(numMessages * 3);

            cluster.terminationsExpected(true);
            cluster.stopAllNodes();

            cluster.invalidateLatestSnapshots();

            cluster.restartAllNodes(false);
            cluster.awaitLeader();
            cluster.awaitServicesMessageCount(numMessages * 3);
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(60)
    void shouldHandleMultipleElections(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader0 = cluster.awaitLeader();

            final int numMessages = 3;
            cluster.connectClient();
            cluster.sendMessages(numMessages);
            cluster.awaitResponseMessageCount(numMessages);
            cluster.awaitServicesMessageCount(numMessages);

            cluster.stopNode(leader0);
            final TestNode leader1 = cluster.awaitLeader(leader0.index());
            cluster.awaitNewLeadershipEvent(1);
            assertTrue(cluster.client().sendKeepAlive());
            cluster.startStaticNode(leader0.index(), false);
            awaitElectionClosed(cluster.node(leader0.index()));

            cluster.sendMessages(numMessages);
            cluster.awaitResponseMessageCount(numMessages * 2);
            cluster.awaitServicesMessageCount(numMessages * 2);

            cluster.stopNode(leader1);
            cluster.awaitLeader(leader1.index());
            cluster.awaitNewLeadershipEvent(2);
            assertTrue(cluster.client().sendKeepAlive());
            cluster.startStaticNode(leader1.index(), false);
            awaitElectionClosed(cluster.node(leader1.index()));

            cluster.sendMessages(numMessages);
            cluster.awaitResponseMessageCount(numMessages * 3);
            cluster.awaitServicesMessageCount(numMessages * 3);
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(50)
    void shouldRecoverWhenLastSnapshotIsInvalidBetweenTwoElections(final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            final TestNode leader0 = cluster.awaitLeader();

            final int numMessages = 3;
            cluster.connectClient();
            cluster.sendMessages(numMessages);
            cluster.awaitResponseMessageCount(numMessages);
            cluster.awaitServicesMessageCount(numMessages);

            cluster.stopNode(leader0);
            final TestNode leader1 = cluster.awaitLeader(leader0.index());
            cluster.awaitNewLeadershipEvent(1);
            assertTrue(cluster.client().sendKeepAlive());
            cluster.startStaticNode(leader0.index(), false);

            cluster.sendMessages(numMessages);
            cluster.awaitResponseMessageCount(numMessages * 2);
            cluster.awaitServicesMessageCount(numMessages * 2);

            cluster.takeSnapshot(leader1);
            cluster.awaitSnapshotCount(1);

            cluster.stopNode(leader1);
            cluster.awaitLeader(leader1.index());
            cluster.awaitNewLeadershipEvent(2);
            assertTrue(cluster.client().sendKeepAlive());
            cluster.startStaticNode(leader1.index(), false);

            cluster.sendMessages(numMessages);
            cluster.awaitResponseMessageCount(numMessages * 3);
            cluster.awaitServicesMessageCount(numMessages * 3);

            // No snapshot for Term 2

            cluster.terminationsExpected(true);
            cluster.stopAllNodes();

            cluster.invalidateLatestSnapshots();

            cluster.restartAllNodes(false);
            cluster.awaitLeader();
            cluster.awaitServicesMessageCount(numMessages * 3);
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }

    @Test
    @Timeout(30)
    public void shouldCatchUpAfterFollowerMissesOneMessage(final TestInfo testInfo)
    {
        shouldCatchUpAfterFollowerMissesMessage(NO_OP_MSG, testInfo);
    }

    @Test
    @Timeout(30)
    public void shouldCatchUpAfterFollowerMissesTimerRegistration(final TestInfo testInfo)
    {
        shouldCatchUpAfterFollowerMissesMessage(REGISTER_TIMER_MSG, testInfo);
    }

    private void shouldCatchUpAfterFollowerMissesMessage(final String message, final TestInfo testInfo)
    {
        cluster = startThreeNodeStaticCluster(NULL_VALUE);
        try
        {
            cluster.awaitLeader();
            TestNode follower = cluster.followers().get(0);

            cluster.stopNode(follower);

            cluster.connectClient();
            cluster.msgBuffer().putStringWithoutLengthAscii(0, message);
            cluster.pollUntilMessageSent(message.length());
            cluster.awaitResponseMessageCount(1);

            follower = cluster.startStaticNode(follower.index(), false);

            awaitElectionClosed(follower);
            assertEquals(FOLLOWER, follower.role());
        }
        catch (final Throwable ex)
        {
            cluster.dumpData(testInfo, ex);
        }
    }
}
