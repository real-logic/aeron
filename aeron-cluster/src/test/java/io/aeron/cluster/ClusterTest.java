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

import io.aeron.cluster.service.Cluster;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.status.CountersReader;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.cluster.service.CommitPos.COMMIT_POSITION_TYPE_ID;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

@Ignore
public class ClusterTest
{
    private static final String MSG = "Hello World!";

    private final ExecutorService executor = Executors.newFixedThreadPool(1);

    @After
    public void after() throws InterruptedException
    {
        executor.shutdownNow();

        if (!executor.awaitTermination(5, TimeUnit.SECONDS))
        {
            System.out.println("Warning: not all tasks completed promptly");
        }
    }

    @Test(timeout = 30_000)
    public void shouldStopFollowerAndRestartFollower() throws Exception
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            cluster.awaitLeader();

            TestNode follower = cluster.followers().get(0);

            cluster.stopNode(follower);
            Thread.sleep(1_000);
            follower = cluster.startStaticNode(follower.index(), false);
            Thread.sleep(1_000);

            assertThat(follower.role(), is(Cluster.Role.FOLLOWER));
        }
    }

    @Test(timeout = 30_000)
    public void shouldStopLeaderAndFollowersAndRestartAllWithSnapshot() throws Exception
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.takeSnapshot(leader);
            cluster.awaitSnapshotCounter(cluster.node(0), 1);
            cluster.awaitSnapshotCounter(cluster.node(1), 1);
            cluster.awaitSnapshotCounter(cluster.node(2), 1);

            cluster.stopNode(cluster.node(0));
            cluster.stopNode(cluster.node(1));
            cluster.stopNode(cluster.node(2));

            Thread.sleep(1_000);

            cluster.startStaticNode(0, false);
            cluster.startStaticNode(1, false);
            cluster.startStaticNode(2, false);

            cluster.awaitLeader();
            assertThat(cluster.followers().size(), is(2));

            cluster.awaitSnapshotLoadedForService(cluster.node(0));
            cluster.awaitSnapshotLoadedForService(cluster.node(1));
            cluster.awaitSnapshotLoadedForService(cluster.node(2));
        }
    }

    @Test(timeout = 30_000)
    public void shouldShutdownClusterAndRestartWithSnapshots() throws Exception
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

            cluster.stopNode(cluster.node(0));
            cluster.stopNode(cluster.node(1));
            cluster.stopNode(cluster.node(2));

            Thread.sleep(1_000);

            cluster.startStaticNode(0, false);
            cluster.startStaticNode(1, false);
            cluster.startStaticNode(2, false);

            cluster.awaitLeader();
            assertThat(cluster.followers().size(), is(2));

            cluster.awaitSnapshotLoadedForService(cluster.node(0));
            cluster.awaitSnapshotLoadedForService(cluster.node(1));
            cluster.awaitSnapshotLoadedForService(cluster.node(2));
        }
    }

    @Test(timeout = 30_000)
    public void shouldAbortClusterAndRestart() throws Exception
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

            cluster.stopNode(cluster.node(0));
            cluster.stopNode(cluster.node(1));
            cluster.stopNode(cluster.node(2));

            Thread.sleep(1_000);

            cluster.startStaticNode(0, false);
            cluster.startStaticNode(1, false);
            cluster.startStaticNode(2, false);

            cluster.awaitLeader();
            assertThat(cluster.followers().size(), is(2));

            assertFalse(cluster.node(0).service().wasSnapshotLoaded());
            assertFalse(cluster.node(1).service().wasSnapshotLoaded());
            assertFalse(cluster.node(2).service().wasSnapshotLoaded());
        }
    }

    @Test(timeout = 30_000)
    public void shouldAbortClusterOnTerminationTimeout() throws Exception
    {
        final int messageCount = 10;

        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            final List<TestNode> followers = cluster.followers();

            assertThat(followers.size(), is(2));
            final TestNode followerA = followers.get(0), followerB = followers.get(1);

            leader.terminationExpected(true);
            followerA.terminationExpected(true);

            cluster.stopNode(followerB);

            cluster.startClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponses(messageCount);

            cluster.abortCluster(leader);
            cluster.awaitNodeTermination(leader);
            cluster.awaitNodeTermination(followerA);

            cluster.stopNode(leader);
            cluster.stopNode(followerA);
        }
    }

    @Test(timeout = 30_000)
    public void shouldEchoMessagesThenContinueOnNewLeader() throws Exception
    {
        final int preFailureMessageCount = 10;
        final int postFailureMessageCount = 7;

        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode originalLeader = cluster.awaitLeader();

            cluster.startClient();
            cluster.sendMessages(preFailureMessageCount);
            cluster.awaitResponses(preFailureMessageCount);
            cluster.awaitMessageCountForService(cluster.node(0), preFailureMessageCount);
            cluster.awaitMessageCountForService(cluster.node(1), preFailureMessageCount);
            cluster.awaitMessageCountForService(cluster.node(2), preFailureMessageCount);

            assertThat(cluster.client().leaderMemberId(), is(originalLeader.index()));

            cluster.stopNode(originalLeader);

            final TestNode newLeader = cluster.awaitLeader(originalLeader.index());

            cluster.sendMessages(postFailureMessageCount);
            cluster.awaitResponses(preFailureMessageCount + postFailureMessageCount);
            assertThat(cluster.client().leaderMemberId(), is(newLeader.index()));

            final TestNode follower = cluster.followers().get(0);

            cluster.awaitMessageCountForService(newLeader, preFailureMessageCount + postFailureMessageCount);
            cluster.awaitMessageCountForService(follower, preFailureMessageCount + postFailureMessageCount);
        }
    }

    @Test(timeout = 30_000)
    public void shouldStopLeaderAndRestartAfterElectionAsFollower() throws Exception
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode originalLeader = cluster.awaitLeader();

            cluster.stopNode(originalLeader);

            cluster.awaitLeader(originalLeader.index());

            final TestNode follower = cluster.startStaticNode(originalLeader.index(), false);

            Thread.sleep(5_000);

            assertThat(follower.role(), is(Cluster.Role.FOLLOWER));
            assertThat(follower.electionState(), is((Election.State)null));
        }
    }

    @Test(timeout = 30_000)
    public void shouldStopLeaderAndRestartAfterElectionAsFollowerWithSendingAfter() throws Exception
    {
        final int messageCount = 10;

        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode originalLeader = cluster.awaitLeader();

            cluster.stopNode(originalLeader);

            cluster.awaitLeader(originalLeader.index());

            final TestNode follower = cluster.startStaticNode(originalLeader.index(), false);

            Thread.sleep(5_000);

            assertThat(follower.role(), is(Cluster.Role.FOLLOWER));
            assertThat(follower.electionState(), is((Election.State)null));

            cluster.startClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponses(messageCount);
        }
    }

    @Test(timeout = 60_000)
    public void shouldStopLeaderAndRestartAfterElectionAsFollowerWithSendingAfterThenStopLeader() throws Exception
    {
        final int messageCount = 10;

        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode originalLeader = cluster.awaitLeader();

            cluster.stopNode(originalLeader);

            cluster.awaitLeader(originalLeader.index());

            final TestNode follower = cluster.startStaticNode(originalLeader.index(), false);

            Thread.sleep(5_000);

            assertThat(follower.role(), is(Cluster.Role.FOLLOWER));
            assertThat(follower.electionState(), is((Election.State)null));

            cluster.startClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponses(messageCount);

            final TestNode leader = cluster.awaitLeader();

            cluster.stopNode(leader);

            cluster.awaitLeader(leader.index());
        }
    }

    @Test(timeout = 30_000)
    public void shouldAcceptMessagesAfterSingleNodeGoDownAndComeBackUpClean() throws Exception
    {
        final int messageCount = 10;

        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            cluster.awaitLeader();

            TestNode follower = cluster.followers().get(0);

            cluster.stopNode(follower);

            Thread.sleep(10_000);

            follower = cluster.startStaticNode(follower.index(), true);

            Thread.sleep(1_000);

            assertThat(follower.role(), is(Cluster.Role.FOLLOWER));

            cluster.startClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponses(messageCount);
            cluster.awaitMessageCountForService(follower, messageCount);
        }
    }

    @Test(timeout = 30_000)
    public void followerShouldRecoverWhenSnapshotTakenWhileDown() throws Exception
    {
        final int messageCount = 10;

        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();
            TestNode follower = cluster.followers().get(0);

            cluster.stopNode(follower);

            Thread.sleep(10_000);

            cluster.takeSnapshot(leader);
            cluster.awaitSnapshotCounter(leader, 1);

            cluster.startClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponses(messageCount);

            follower = cluster.startStaticNode(follower.index(), false);

            Thread.sleep(1_000);

            assertThat(follower.role(), is(Cluster.Role.FOLLOWER));

            cluster.awaitMessageCountForService(follower, messageCount);

            assertThat(follower.errors(), is(0L));
        }
    }

    @Test(timeout = 45_000)
    public void shouldTolerateMultipleLeaderFailures() throws Exception
    {
        final int messageCount = 10;

        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode firstLeader = cluster.awaitLeader();
            cluster.stopNode(firstLeader);

            final TestNode secondLeader = cluster.awaitLeader();

            cluster.startStaticNode(firstLeader.index(), false);

            cluster.stopNode(secondLeader);

            cluster.awaitLeader();

            cluster.startClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponses(messageCount);
        }
    }

    @Test(timeout = 30_000)
    public void shouldAcceptMessagesAfterTwoNodesGoDownAndComeBackUpClean() throws Exception
    {
        final int messageCount = 10;

        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            cluster.awaitLeader();

            final List<TestNode> followers = cluster.followers();
            TestNode followerA = followers.get(0), followerB = followers.get(1);

            cluster.stopNode(followerA);
            cluster.stopNode(followerB);

            Thread.sleep(5_000);

            followerA = cluster.startStaticNode(followerA.index(), true);
            followerB = cluster.startStaticNode(followerB.index(), true);

            Thread.sleep(1_000);

            assertThat(followerA.role(), is(Cluster.Role.FOLLOWER));
            assertThat(followerB.role(), is(Cluster.Role.FOLLOWER));

            cluster.startClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponses(messageCount);
            cluster.awaitMessageCountForService(followerA, messageCount);
            cluster.awaitMessageCountForService(followerB, messageCount);
        }
    }

    @Test(timeout = 30_000)
    public void membersShouldHaveOneCommitPositionCounter() throws Exception
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            final List<TestNode> followers = cluster.followers();
            final TestNode followerA = followers.get(0), followerB = followers.get(1);

            cluster.stopNode(leader);

            cluster.awaitLeader(leader.index());

            assertThat(countersOfType(followerA.countersReader(), COMMIT_POSITION_TYPE_ID), is(1));
            assertThat(countersOfType(followerB.countersReader(), COMMIT_POSITION_TYPE_ID), is(1));
        }
    }

    @Test(timeout = 30_000)
    public void shouldCallOnROleChangeOnBecomingLeader() throws Exception
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            TestNode leader = cluster.awaitLeader();

            List<TestNode> followers = cluster.followers();
            final TestNode followerA = followers.get(0), followerB = followers.get(1);

            assertThat(leader.service().roleChangedTo(), is(Cluster.Role.LEADER));
            assertThat(followerA.service().roleChangedTo(), is((Cluster.Role)null));
            assertThat(followerB.service().roleChangedTo(), is((Cluster.Role)null));

            cluster.stopNode(leader);

            leader = cluster.awaitLeader(leader.index());
            followers = cluster.followers();
            final TestNode follower = followers.get(0);

            assertThat(leader.service().roleChangedTo(), is(Cluster.Role.LEADER));
            assertThat(follower.service().roleChangedTo(), is((Cluster.Role)null));
        }
    }

    @Test(timeout = 30_000)
    public void shouldLooseLeadershipWhenNoActiveQuorumOfFollowers() throws Exception
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            final List<TestNode> followers = cluster.followers();
            final TestNode followerA = followers.get(0), followerB = followers.get(1);

            assertThat(leader.service().roleChangedTo(), is(Cluster.Role.LEADER));

            cluster.stopNode(followerA);
            cluster.stopNode(followerB);

            while (leader.service().roleChangedTo() == Cluster.Role.LEADER)
            {
                TestUtil.checkInterruptedStatus();
                Thread.yield();
            }

            assertThat(leader.service().roleChangedTo(), is(Cluster.Role.FOLLOWER));
        }
    }

    @Test(timeout = 60_000)
    public void followerShouldRecoverWhileMessagesContinue() throws Exception
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            final List<TestNode> followers = cluster.followers();
            final TestNode followerA = followers.get(0);
            TestNode followerB = followers.get(1);

            cluster.startClient();
            startMessageThread(cluster, TimeUnit.MICROSECONDS.toNanos(500));

            cluster.stopNode(followerB);

            Thread.sleep(10_000);

            followerB = cluster.startStaticNode(followerB.index(), false);

            Thread.sleep(30_000);

            assertThat(leader.errors(), is(0L));
            assertThat(followerA.errors(), is(0L));
            assertThat(followerB.errors(), is(0L));
            assertThat(followerB.electionState(), is((Election.State)null));
        }
    }

    @Test(timeout = 30_000)
    public void shouldCatchupFromEmptyLog() throws Exception
    {
        final int messageCount = 10;

        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            cluster.awaitLeader();

            final List<TestNode> followers = cluster.followers();
            TestNode followerB = followers.get(1);

            cluster.stopNode(followerB);

            cluster.startClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponses(messageCount);

            followerB = cluster.startStaticNode(followerB.index(), true);

            cluster.awaitMessageCountForService(followerB, messageCount);
        }
    }

    @Test(timeout = 30_000)
    public void shouldCatchupFromEmptyLogThenSnapshotAfterShutdownAndFollowerCleanStart() throws Exception
    {
        final int messageCount = 10;

        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();
            final List<TestNode> followers = cluster.followers();
            final TestNode followerA = followers.get(0), followerB = followers.get(1);

            cluster.startClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponses(messageCount);

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

            cluster.stopNode(cluster.node(0));
            cluster.stopNode(cluster.node(1));
            cluster.stopNode(cluster.node(2));

            Thread.sleep(1_000);

            cluster.startStaticNode(0, false);
            cluster.startStaticNode(1, false);
            cluster.startStaticNode(2, true);

            final TestNode newLeader = cluster.awaitLeader();

            assertNotEquals(newLeader.index(), is(2));

            assertTrue(cluster.node(0).service().wasSnapshotLoaded());
            assertTrue(cluster.node(1).service().wasSnapshotLoaded());
            assertFalse(cluster.node(2).service().wasSnapshotLoaded());

            cluster.awaitMessageCountForService(cluster.node(2), messageCount);
            cluster.awaitSnapshotCounter(cluster.node(2), 1);
            assertTrue(cluster.node(2).service().wasSnapshotTaken());
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

    private void startMessageThread(final TestCluster cluster, final long intervalNs)
    {
        executor.submit(() ->
        {
            //final IdleStrategy idleStrategy = new YieldingIdleStrategy();
            cluster.msgBuffer().putStringWithoutLengthAscii(0, MSG);

            while (true)
            {
                while (cluster.client().offer(cluster.msgBuffer(), 0, MSG.length()) < 0)
                {
                    if (Thread.interrupted())
                    {
                        return;
                    }

                    cluster.client().pollEgress();
                    LockSupport.parkNanos(intervalNs);
                }

                cluster.client().pollEgress();
                //idleStrategy.idle();
            }
        });
    }
}
