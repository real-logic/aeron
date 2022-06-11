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

import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.client.ControlledEgressListener;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.*;
import io.aeron.log.EventLogExtension;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.security.AuthorisationService;
import io.aeron.test.*;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.DirectBuffer;
import org.agrona.collections.Hashing;
import org.agrona.collections.MutableBoolean;
import org.agrona.collections.MutableInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.zip.CRC32;

import static io.aeron.cluster.service.Cluster.Role.FOLLOWER;
import static io.aeron.cluster.service.Cluster.Role.LEADER;
import static io.aeron.logbuffer.FrameDescriptor.computeMaxMessageLength;
import static io.aeron.test.SystemTestWatcher.UNKNOWN_HOST_FILTER;
import static io.aeron.test.Tests.awaitAvailableWindow;
import static io.aeron.test.cluster.ClusterTests.*;
import static io.aeron.test.cluster.TestCluster.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.jupiter.api.Assertions.*;

@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
public class ClusterTest
{
    @RegisterExtension
    public final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private TestCluster cluster = null;

    @BeforeEach
    void setUp()
    {
        systemTestWatcher.ignoreErrorsMatching(
            (s) -> s.contains("ats_gcm_decrypt final_ex: error:00000000:lib(0):func(0):reason(0)"));
    }

    @Test
    @InterruptAfter(30)
    public void shouldStopFollowerAndRestartFollower()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        TestNode follower = cluster.followers().get(0);

        awaitElectionClosed(follower);
        cluster.stopNode(follower);

        follower = cluster.startStaticNode(follower.index(), false);

        awaitElectionClosed(follower);
        assertEquals(FOLLOWER, follower.role());
    }

    @Test
    @InterruptAfter(40)
    public void shouldNotifyClientOfNewLeader()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        cluster.connectClient();
        cluster.awaitActiveSessionCount(cluster.followers().get(0), 1);

        cluster.stopNode(leader);
        cluster.awaitNewLeadershipEvent(1);
    }

    @Test
    @InterruptAfter(30)
    public void shouldStopLeaderAndFollowersThenRestartAllWithSnapshot()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);

        cluster.stopAllNodes();
        cluster.restartAllNodes(false);
        cluster.awaitLeader();
        assertEquals(2, cluster.followers().size());

        cluster.awaitSnapshotsLoaded();
    }

    @Test
    @InterruptAfter(30)
    public void shouldShutdownClusterAndRestartWithSnapshots()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

    @Test
    @InterruptAfter(30)
    public void shouldAbortClusterAndRestart()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

    @Test
    @InterruptAfter(30)
    public void shouldAbortClusterOnTerminationTimeout()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

    @Test
    @InterruptAfter(40)
    public void shouldEchoMessages()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        cluster.connectClient();

        final int expectedCount = 10;

        cluster.sendMessages(expectedCount);
        cluster.awaitResponseMessageCount(expectedCount);
        cluster.awaitServicesMessageCount(expectedCount);
    }

    @Test
    @InterruptAfter(40)
    public void shouldHandleLeaderFailOverWhenNameIsNotResolvable()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster).ignoreErrorsMatching(UNKNOWN_HOST_FILTER);

        final TestNode originalLeader = cluster.awaitLeader();
        cluster.connectClient();

        final int expectedCount = 10;

        cluster.sendMessages(expectedCount);
        cluster.awaitResponseMessageCount(expectedCount);
        cluster.awaitServicesMessageCount(expectedCount);

        cluster.disableNameResolution(originalLeader.hostname());
        originalLeader.close();

        cluster.awaitLeader();

        cluster.sendMessages(expectedCount);
        cluster.awaitResponseMessageCount(2 * expectedCount);
        cluster.awaitServicesMessageCount(2 * expectedCount);
    }

    @Test
    @InterruptAfter(20)
    public void shouldHandleClusterStartWhenANameIsNotResolvable()
    {
        final int initiallyUnresolvableNodeId = 1;

        cluster = aCluster().withStaticNodes(3).withInvalidNameResolution(initiallyUnresolvableNodeId).start();
        systemTestWatcher.cluster(cluster).ignoreErrorsMatching(UNKNOWN_HOST_FILTER);

        cluster.awaitLeader();
        cluster.connectClient();

        final int expectedCount = 10;

        cluster.sendMessages(expectedCount);
        cluster.awaitResponseMessageCount(expectedCount);
        cluster.awaitServicesMessageCount(expectedCount);

        cluster.restoreNameResolution(initiallyUnresolvableNodeId);
        assertNotNull(cluster.startStaticNode(initiallyUnresolvableNodeId, true));

        cluster.awaitServiceMessageCount(cluster.node(initiallyUnresolvableNodeId), expectedCount);
    }

    @Test
    @InterruptAfter(10)
    public void shouldHandleClusterStartWhereMostNamesBecomeResolvableDuringElection()
    {
        cluster = aCluster().withStaticNodes(3).withInvalidNameResolution(0).withInvalidNameResolution(2).start();
        systemTestWatcher.cluster(cluster).ignoreErrorsMatching(UNKNOWN_HOST_FILTER);

        awaitElectionState(cluster.node(1), ElectionState.CANVASS);

        cluster.restoreNameResolution(0);
        cluster.restoreNameResolution(2);
        assertNotNull(cluster.startStaticNode(0, true));
        assertNotNull(cluster.startStaticNode(2, true));

        cluster.awaitLeader();
        cluster.connectClient();

        final int expectedCount = 10;
        cluster.sendMessages(expectedCount);
        cluster.awaitResponseMessageCount(expectedCount);
        cluster.awaitServicesMessageCount(expectedCount);
    }

    @Test
    @InterruptAfter(40)
    public void shouldEchoMessagesThenContinueOnNewLeader()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

    @Test
    @InterruptAfter(40)
    public void shouldStopLeaderAndRestartAsFollower()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode originalLeader = cluster.awaitLeader();

        cluster.stopNode(originalLeader);
        cluster.awaitLeader(originalLeader.index());

        final TestNode follower = cluster.startStaticNode(originalLeader.index(), false);

        awaitElectionClosed(follower);
        assertEquals(FOLLOWER, follower.role());
    }

    @Test
    @InterruptAfter(40)
    public void shouldStopLeaderAndRestartAsFollowerWithSendingAfter()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

    @Test
    @InterruptAfter(60)
    public void shouldStopLeaderAndRestartAsFollowerWithSendingAfterThenStopLeader()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

    @Test
    @InterruptAfter(40)
    public void shouldAcceptMessagesAfterSingleNodeCleanRestart()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

    @Test
    @InterruptAfter(40)
    public void shouldReplaySnapshotTakenWhileDown()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        final TestNode followerA = followers.get(0);
        TestNode followerB = followers.get(1);

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
    }

    @Test
    @InterruptAfter(50)
    public void shouldTolerateMultipleLeaderFailures()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

    @Test
    @InterruptAfter(90)
    public void shouldRecoverAfterTwoLeadersNodesFailAndComeBackUpAtSameTime()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

    @Test
    @InterruptAfter(30)
    public void shouldAcceptMessagesAfterTwoNodeCleanRestart()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

    @Test
    @InterruptAfter(60)
    public void shouldRecoverWithUncommittedMessagesAfterRestartWhenNewCommitPosExceedsPreviousAppendedPos()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

    @Test
    @InterruptAfter(30)
    public void shouldRecoverWithUncommittedMessagesAfterRestartWhenNewCommitPosIsLessThanPreviousAppendedPos()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

    @Test
    @InterruptAfter(40)
    public void shouldCallOnRoleChangeOnBecomingLeader()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

    @Test
    @InterruptAfter(40)
    public void shouldLoseLeadershipWhenNoActiveQuorumOfFollowers()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

    @Test
    @InterruptAfter(30)
    public void shouldTerminateLeaderWhenServiceStops()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

    @Test
    @InterruptAfter(30)
    public void shouldEnterElectionWhenRecordingStopsOnLeader()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        cluster.connectClient();
        cluster.sendMessages(1);
        cluster.awaitResponseMessageCount(1);
        cluster.awaitServicesMessageCount(1);

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
        cluster.followers(2);
    }

    @Test
    @InterruptAfter(30)
    public void shouldRecoverFollowerWhenRecordingStops()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();

        final TestNode follower = cluster.followers().get(0);
        final AeronArchive.Context archiveCtx = new AeronArchive.Context()
            .controlRequestChannel(follower.archive().context().localControlChannel())
            .controlResponseChannel(follower.archive().context().localControlChannel())
            .controlRequestStreamId(follower.archive().context().localControlStreamId())
            .aeronDirectoryName(follower.mediaDriver().aeronDirectoryName());

        try (AeronArchive archive = AeronArchive.connect(archiveCtx))
        {
            final int firstRecordingIdIsTheClusterLog = 0;
            assertTrue(archive.tryStopRecordingByIdentity(firstRecordingIdIsTheClusterLog));
        }

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        cluster.awaitServiceMessageCount(follower, messageCount);
    }

    @Test
    @InterruptAfter(30)
    public void shouldCloseClientOnTimeout()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final AeronCluster client = cluster.connectClient();
        final ConsensusModule.Context context = leader.consensusModule().context();
        final Counter timedOutClientCounter = context.timedOutClientCounter();

        assertEquals(0, timedOutClientCounter.get());
        assertFalse(client.isClosed());

        Tests.sleep(NANOSECONDS.toMillis(context.sessionTimeoutNs()));

        cluster.shouldErrorOnClientClose(false);
        while (!client.isClosed())
        {
            Tests.sleep(1);
            client.pollEgress();
        }

        assertEquals(1, timedOutClientCounter.get());
    }

    @Test
    @InterruptAfter(40)
    public void shouldRecoverWhileMessagesContinue() throws InterruptedException
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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
        cluster.awaitServiceMessageCount(followerB, messageCounter.get());

        cluster.client().close();
        cluster.awaitActiveSessionCount(0);

        assertEquals(0L, leader.errors());

        assertEquals(0L, followerA.errors());

        assertEquals(0L, followerB.errors());
    }

    @Test
    @InterruptAfter(30)
    public void shouldCatchupFromEmptyLog()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

    @Test
    @InterruptAfter(30)
    public void shouldCatchupFromEmptyLogThenSnapshotAfterShutdownAndFollowerCleanStart()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers(2);
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

    @Test
    @InterruptAfter(30)
    public void shouldCatchUpTwoFreshNodesAfterRestart()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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
        cluster.awaitServicesMessageCount(messageCount);

        cluster.terminationsExpected(true);
        cluster.abortCluster(leader);
        cluster.awaitNodeTerminations();
        cluster.stopAllNodes();

        final TestNode oldLeader = cluster.startStaticNode(leader.index(), false);
        final TestNode oldFollower1 = cluster.startStaticNode(followers.get(0).index(), true);
        final TestNode oldFollower2 = cluster.startStaticNode(followers.get(1).index(), true);

        cluster.awaitLeader();
        cluster.awaitServicesMessageCount(messageCount);

        assertEquals(0L, oldLeader.errors());
        assertEquals(0L, oldFollower1.errors());
        assertEquals(0L, oldFollower2.errors());
    }

    @Test
    @InterruptAfter(30)
    public void shouldReplayMultipleSnapshotsWithEmptyFollowerLog()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

        // Needs a little time to replay the transactions...
        Tests.await(() -> cluster.node(2).service().messageCount() >= 3);
        assertEquals(messageCount, cluster.node(2).service().messageCount());

        final int messageCountAfterStart = 4;
        cluster.reconnectClient();
        cluster.sendMessages(messageCountAfterStart);
        messageCount += messageCountAfterStart;
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);
    }

    @Test
    @InterruptAfter(40)
    public void shouldRecoverQuicklyAfterKillingFollowersThenRestartingOne()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        final TestNode followerOne = followers.get(0);
        final TestNode followerTwo = followers.get(1);

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

    @Test
    @InterruptAfter(40)
    public void shouldRecoverWhenLeaderHasAppendedMoreThanFollower()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        final TestNode followerOne = followers.get(0);
        final TestNode followerTwo = followers.get(1);

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

    @Test
    @InterruptAfter(40)
    public void shouldRecoverWhenFollowerIsMultipleTermsBehind()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode originalLeader = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        cluster.stopNode(originalLeader);
        final TestNode newLeader = cluster.awaitLeader();
        cluster.reconnectClient();

        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount * 2);

        cluster.stopNode(newLeader);
        cluster.startStaticNode(newLeader.index(), false);
        cluster.awaitLeader();
        cluster.reconnectClient();

        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount * 3);

        cluster.startStaticNode(originalLeader.index(), false);
        final TestNode lateJoiningNode = cluster.node(originalLeader.index());

        while (lateJoiningNode.service().messageCount() < messageCount * 3)
        {
            Tests.yieldingIdle("Waiting for late joining follower to catch up");
        }
    }

    @Test
    @InterruptAfter(40)
    public void shouldRecoverWhenFollowerIsMultipleTermsBehindFromEmptyLog()
    {
        cluster = aCluster().withStaticNodes(3).start();

        systemTestWatcher.cluster(cluster);

        final TestNode originalLeader = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        cluster.stopNode(originalLeader);
        final TestNode newLeader = cluster.awaitLeader();
        cluster.reconnectClient();

        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount * 2);

        cluster.stopNode(newLeader);
        cluster.startStaticNode(newLeader.index(), false);
        cluster.awaitLeader();
        cluster.reconnectClient();

        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount * 3);

        cluster.startStaticNode(originalLeader.index(), true);
        final TestNode lateJoiningNode = cluster.node(originalLeader.index());

        cluster.awaitServiceMessageCount(lateJoiningNode, messageCount * 3);
    }

    @Test
    @InterruptAfter(40)
    @Disabled
    public void shouldHandleManyLargeMessages()
    {
        cluster = aCluster().withStaticNodes(3).start();

        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        awaitElectionState(cluster.node(0), ElectionState.CLOSED);
        awaitElectionState(cluster.node(1), ElectionState.CLOSED);
        awaitElectionState(cluster.node(2), ElectionState.CLOSED);

        final int largeMessageCount = 256_000;

        cluster.connectClient();
        cluster.sendLargeMessages(largeMessageCount);
        cluster.awaitResponseMessageCount(largeMessageCount);
        cluster.awaitServicesMessageCount(largeMessageCount);
    }

    @Test
    @InterruptAfter(40)
    @Disabled
    public void shouldRecoverWhenFollowerWithInitialSnapshotAndArchivePurgeThenIsMultipleTermsBehind()
    {
        cluster = aCluster().withStaticNodes(3).start();

        systemTestWatcher.cluster(cluster);

        final TestNode originalLeader = cluster.awaitLeader();

        final int largeMessageCount = 128_000;
        final int messageCount = 10;

        cluster.connectClient();
        cluster.sendLargeMessages(largeMessageCount);
        cluster.awaitResponseMessageCount(largeMessageCount);
        cluster.awaitServicesMessageCount(largeMessageCount);

        cluster.takeSnapshot(originalLeader);
        cluster.awaitSnapshotCount(1);
        cluster.purgeLogToLastSnapshot();

        cluster.stopNode(originalLeader);
        final TestNode newLeader = cluster.awaitLeader();

        cluster.reconnectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(largeMessageCount + messageCount);

        cluster.stopNode(newLeader);
        cluster.startStaticNode(newLeader.index(), false);
        cluster.awaitLeader();

        cluster.reconnectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(largeMessageCount + (messageCount * 2));

        cluster.startStaticNode(originalLeader.index(), false);
        final TestNode lateJoiningNode = cluster.node(originalLeader.index());

        cluster.awaitServiceMessageCount(lateJoiningNode, largeMessageCount + (messageCount * 2));
    }

    @Test
    @InterruptAfter(40)
    public void shouldRecoverWhenFollowerArrivesPartWayThroughTerm()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        final TestNode followerOne = cluster.followers().get(0);

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        cluster.stopNode(followerOne);

        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount * 2);

        cluster.startStaticNode(followerOne.index(), false);

        // Needs a little time to replay the transactions...
        Tests.await(() -> cluster.node(followerOne.index()).service().messageCount() >= messageCount * 2);
        assertEquals(messageCount * 2, cluster.node(followerOne.index()).service().messageCount());
    }

    @Test
    @InterruptAfter(40)
    public void shouldRecoverWhenFollowerArrivePartWayThroughTermAfterMissingElection()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        final TestNode followerOne = followers.get(0);
        final TestNode followerTwo = followers.get(1);

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
        cluster.startStaticNode(followerTwo.index(), false);
        cluster.awaitLeader();
        cluster.reconnectClient();

        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount * 3);

        cluster.startStaticNode(followerOne.index(), false);

        // Needs a little time to replay the transactions...
        Tests.await(() -> cluster.node(followerOne.index()).service().messageCount() >= messageCount * 3);
        assertEquals(messageCount * 3, cluster.node(followerOne.index()).service().messageCount());
    }

    @Test
    @InterruptAfter(40)
    void shouldRecoverWhenLastSnapshotIsMarkedInvalid()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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
        awaitAvailableWindow(cluster.client().ingressPublication());
        assertTrue(cluster.client().sendKeepAlive());
        cluster.startStaticNode(leader0.index(), false);

        cluster.sendMessages(numMessages);
        cluster.awaitResponseMessageCount(numMessages * 3);
        cluster.awaitServicesMessageCount(numMessages * 3);

        cluster.terminationsExpected(true);
        cluster.stopAllNodes();

        cluster.invalidateLatestSnapshot();

        cluster.restartAllNodes(false);
        cluster.awaitLeader();
        cluster.awaitServicesMessageCount(numMessages * 3);
    }

    @Test
    @InterruptAfter(30)
    void shouldRecoverWhenLastSnapshotForShutdownIsMarkedInvalid()
    {
        cluster = aCluster().withStaticNodes(1).start();
        systemTestWatcher.cluster(cluster);

        TestNode leader = cluster.awaitLeader();

        final int numMessages = 3;
        cluster.connectClient();
        cluster.sendMessages(numMessages);
        cluster.awaitServicesMessageCount(numMessages);

        cluster.stopNode(leader);
        cluster.startStaticNode(leader.index(), false);
        leader = cluster.awaitLeader();

        cluster.terminationsExpected(true);
        cluster.shutdownCluster(leader);
        cluster.awaitNodeTerminations();
        assertTrue(leader.service().wasSnapshotTaken());
        cluster.stopNode(leader);

        cluster.invalidateLatestSnapshot();

        cluster.restartAllNodes(false);
        leader = cluster.awaitLeader();
        cluster.awaitServicesMessageCount(numMessages);
        assertTrue(leader.service().wasSnapshotTaken());
    }

    @Test
    @InterruptAfter(60)
    void shouldHandleMultipleElections()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader0 = cluster.awaitLeader();

        final int numMessages = 3;
        cluster.connectClient();
        cluster.sendMessages(numMessages);
        cluster.awaitResponseMessageCount(numMessages);
        cluster.awaitServicesMessageCount(numMessages);

        cluster.stopNode(leader0);
        final TestNode leader1 = cluster.awaitLeader(leader0.index());
        cluster.awaitNewLeadershipEvent(1);
        awaitAvailableWindow(cluster.client().ingressPublication());
        assertTrue(cluster.client().sendKeepAlive());
        cluster.startStaticNode(leader0.index(), false);
        awaitElectionClosed(cluster.node(leader0.index()));

        cluster.connectClient();
        cluster.sendMessages(numMessages);
        cluster.awaitResponseMessageCount(numMessages * 2);
        cluster.awaitServicesMessageCount(numMessages * 2);

        cluster.stopNode(leader1);
        cluster.awaitLeader(leader1.index());
        cluster.awaitNewLeadershipEvent(2);
        awaitAvailableWindow(cluster.client().ingressPublication());
        assertTrue(cluster.client().sendKeepAlive());
        cluster.startStaticNode(leader1.index(), false);
        awaitElectionClosed(cluster.node(leader1.index()));

        cluster.connectClient();
        cluster.sendMessages(numMessages);
        cluster.awaitResponseMessageCount(numMessages * 3);
        cluster.awaitServicesMessageCount(numMessages * 3);
    }

    @Test
    @InterruptAfter(50)
    void shouldRecoverWhenLastSnapshotIsInvalidBetweenTwoElections()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader0 = cluster.awaitLeader();

        final int numMessages = 3;
        cluster.connectClient();
        cluster.sendMessages(numMessages);
        cluster.awaitResponseMessageCount(numMessages);
        cluster.awaitServicesMessageCount(numMessages);

        cluster.stopNode(leader0);
        final TestNode leader1 = cluster.awaitLeader(leader0.index());
        cluster.awaitNewLeadershipEvent(1);
        awaitAvailableWindow(cluster.client().ingressPublication());
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
        awaitAvailableWindow(cluster.client().ingressPublication());
        assertTrue(cluster.client().sendKeepAlive());
        cluster.startStaticNode(leader1.index(), false);

        cluster.sendMessages(numMessages);
        cluster.awaitResponseMessageCount(numMessages * 3);
        cluster.awaitServicesMessageCount(numMessages * 3);

        // No snapshot for Term 2

        cluster.terminationsExpected(true);
        cluster.stopAllNodes();

        cluster.invalidateLatestSnapshot();

        cluster.restartAllNodes(false);
        cluster.awaitLeader();
        cluster.awaitServicesMessageCount(numMessages * 3);
    }

    @Test
    @InterruptAfter(50)
    void shouldRecoverWhenLastTwosSnapshotsAreInvalidAfterElection()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader0 = cluster.awaitLeader();

        final int numMessages = 3;
        cluster.connectClient();
        cluster.sendMessages(numMessages);
        cluster.awaitResponseMessageCount(numMessages);
        cluster.awaitServicesMessageCount(numMessages);

        cluster.takeSnapshot(leader0);
        cluster.awaitSnapshotCount(1);

        cluster.stopNode(leader0);
        final TestNode leader1 = cluster.awaitLeader(leader0.index());
        cluster.awaitNewLeadershipEvent(1);
        awaitAvailableWindow(cluster.client().ingressPublication());
        assertTrue(cluster.client().sendKeepAlive());
        cluster.startStaticNode(leader0.index(), false);

        cluster.sendMessages(numMessages);
        cluster.awaitResponseMessageCount(numMessages * 2);
        cluster.awaitServicesMessageCount(numMessages * 2);

        cluster.takeSnapshot(leader1);
        for (int i = 0; i < 3; i++)
        {
            cluster.awaitSnapshotCount(cluster.node(i), leader0.index() == i ? 1 : 2);
        }

        cluster.sendMessages(numMessages);
        cluster.awaitResponseMessageCount(numMessages * 3);
        cluster.awaitServicesMessageCount(numMessages * 3);

        cluster.takeSnapshot(leader1);
        for (int i = 0; i < 3; i++)
        {
            cluster.awaitSnapshotCount(cluster.node(i), leader0.index() == i ? 2 : 3);
        }

        cluster.sendMessages(numMessages);
        cluster.awaitResponseMessageCount(numMessages * 4);
        cluster.awaitServicesMessageCount(numMessages * 4);

        cluster.terminationsExpected(true);
        cluster.stopAllNodes();

        cluster.invalidateLatestSnapshot();
        cluster.invalidateLatestSnapshot();

        cluster.restartAllNodes(false);
        cluster.awaitLeader();

        cluster.awaitSnapshotCount(2);

        cluster.awaitServicesMessageCount(numMessages * 4);
    }

    @Test
    @InterruptAfter(30)
    public void shouldCatchUpAfterFollowerMissesOneMessage()
    {
        shouldCatchUpAfterFollowerMissesMessage(NO_OP_MSG);
    }

    @Test
    @InterruptAfter(30)
    public void shouldCatchUpAfterFollowerMissesTimerRegistration()
    {
        shouldCatchUpAfterFollowerMissesMessage(REGISTER_TIMER_MSG);
    }

    @SuppressWarnings("MethodLength")
    @Test
    @InterruptAfter(30)
    public void shouldAllowChangingTermBufferLengthAndMtuAfterRecordingLogIsTruncatedToTheLatestSnapshot()
    {
        final int originalTermLength = 256 * 1024;
        final int originalMtu = 1408;
        final int newTermLength = 2 * 1024 * 1024;
        final int newMtu = 8992;

        final CRC32 crc32 = new CRC32();
        cluster = aCluster().withStaticNodes(3)
            .withLogChannel("aeron:udp?term-length=" + originalTermLength + "|mtu=" + originalMtu)
            .withIngressChannel("aeron:udp?term-length=" + originalTermLength + "|mtu=" + originalMtu)
            .withEgressChannel(
                "aeron:udp?endpoint=localhost:0|term-length=" + originalTermLength + "|mtu=" + originalMtu)
            .withServiceSupplier(
                (i) -> new TestNode.TestService[]{ new TestNode.TestService(), new TestNode.ChecksumService() })
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        for (int i = 0; i < 3; i++)
        {
            assertEquals(2, cluster.node(i).services().length);
        }

        cluster.connectClient();
        final int firstBatch = 9;
        int messageLength = computeMaxMessageLength(originalTermLength) - AeronCluster.SESSION_HEADER_LENGTH;
        int payloadLength = messageLength - SIZE_OF_INT;
        cluster.msgBuffer().setMemory(0, payloadLength, (byte)'x');
        crc32.reset();
        crc32.update(cluster.msgBuffer().byteArray(), 0, payloadLength);
        int msgChecksum = (int)crc32.getValue();
        cluster.msgBuffer().putInt(payloadLength, msgChecksum, LITTLE_ENDIAN);
        long checksum = 0;
        for (int i = 0; i < firstBatch; i++)
        {
            cluster.pollUntilMessageSent(messageLength);
            checksum = Hashing.hash(checksum ^ msgChecksum);
        }
        cluster.awaitResponseMessageCount(firstBatch);

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);

        cluster.msgBuffer().setMemory(0, payloadLength, (byte)'y');
        crc32.reset();
        crc32.update(cluster.msgBuffer().byteArray(), 0, payloadLength);
        msgChecksum = (int)crc32.getValue();
        cluster.msgBuffer().putInt(payloadLength, msgChecksum, LITTLE_ENDIAN);
        final int secondBatch = 11;
        cluster.reconnectClient();
        for (int i = 0; i < secondBatch; i++)
        {
            try
            {
                cluster.pollUntilMessageSent(messageLength);
            }
            catch (final ClusterException ex)
            {
                throw new RuntimeException("i=" + i, ex);
            }
        }
        cluster.awaitResponseMessageCount(firstBatch + secondBatch);

        cluster.stopAllNodes();

        // seed all recording logs from the latest snapshot
        for (int i = 0; i < 3; i++)
        {
            ClusterTool.seedRecordingLogFromSnapshot(cluster.node(i).consensusModule().context().clusterDir());
        }

        cluster.logChannel("aeron:udp?term-length=" + newTermLength + "|mtu=" + newMtu);
        cluster.ingressChannel("aeron:udp?term-length=" + newTermLength + "|mtu=" + newMtu);
        cluster.egressChannel("aeron:udp?endpoint=localhost:0|term-length=" + newTermLength + "|mtu=" + newMtu);
        cluster.restartAllNodes(false);
        cluster.awaitLeader();
        assertEquals(2, cluster.followers().size());
        for (int i = 0; i < 3; i++)
        {
            assertEquals(2, cluster.node(i).services().length);
        }

        cluster.awaitSnapshotsLoaded();

        cluster.reconnectClient();
        messageLength = computeMaxMessageLength(newTermLength) - AeronCluster.SESSION_HEADER_LENGTH;
        payloadLength = messageLength - SIZE_OF_INT;
        cluster.msgBuffer().setMemory(0, payloadLength, (byte)'z');
        crc32.reset();
        crc32.update(cluster.msgBuffer().byteArray(), 0, payloadLength);
        msgChecksum = (int)crc32.getValue();
        cluster.msgBuffer().putInt(payloadLength, msgChecksum, LITTLE_ENDIAN);
        final int thirdBatch = 5;
        for (int i = 0; i < thirdBatch; i++)
        {
            cluster.pollUntilMessageSent(messageLength);
            checksum = Hashing.hash(checksum ^ msgChecksum);
        }
        cluster.awaitResponseMessageCount(firstBatch + secondBatch + thirdBatch);

        final int finalMessageCount = firstBatch + thirdBatch;
        final long finalChecksum = checksum;
        final Predicate<TestNode> finalServiceState =
            (node) ->
            {
                final TestNode.TestService[] services = node.services();
                return finalMessageCount == services[0].messageCount() &&
                    finalChecksum == ((TestNode.ChecksumService)services[1]).checksum();
            };

        for (int i = 0; i < 3; i++)
        {
            final TestNode node = cluster.node(i);
            cluster.awaitServiceState(node, finalServiceState);
        }
    }

    @Test
    @InterruptAfter(60)
    public void shouldRecoverWhenFollowersIsMultipleTermsBehindFromEmptyLogAndPartialLogWithoutCommittedLogEntry()
    {
        cluster = aCluster().withStaticNodes(5).start(4);

        systemTestWatcher.cluster(cluster);

        final int messageCount = 10;
        final int numTerms = 3;
        int totalMessages = 0;

        int partialNode = Aeron.NULL_VALUE;

        for (int i = 0; i < numTerms; i++)
        {
            final TestNode oldLeader = cluster.awaitLeader();

            cluster.connectClient();
            cluster.sendMessages(messageCount);
            totalMessages += messageCount;
            cluster.awaitResponseMessageCount(totalMessages);

            if (Aeron.NULL_VALUE == partialNode)
            {
                partialNode = (oldLeader.index() + 1) % 4;
                cluster.stopNode(cluster.node(partialNode));
            }
            cluster.stopNode(oldLeader);
            cluster.startStaticNode(oldLeader.index(), false);
            cluster.awaitLeader();
        }

        final TestNode lateJoiningNode = cluster.startStaticNode(4, true);

        cluster.awaitServiceMessageCount(lateJoiningNode, totalMessages);

        final TestNode node = cluster.startStaticNode(partialNode, false);

        cluster.awaitServiceMessageCount(node, totalMessages);

        cluster.awaitLeader();

        cluster.connectClient();
        cluster.sendMessages(messageCount);
        totalMessages += messageCount;
        cluster.awaitResponseMessageCount(totalMessages);
        cluster.awaitServiceMessageCount(node, totalMessages);

        cluster.assertRecordingLogsEqual();
    }

    @Test
    @InterruptAfter(10)
    void shouldRejectTakeSnapshotRequestWithAnAuthorisationError()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();

        final long requestCorrelationId = System.nanoTime();
        final MutableBoolean hasResponse = injectAdminResponseEgressListener(
            requestCorrelationId,
            AdminRequestType.SNAPSHOT,
            AdminResponseCode.UNAUTHORISED_ACCESS,
            "Execution of the " + AdminRequestType.SNAPSHOT + " request was not authorised");

        final AeronCluster client = cluster.connectClient();
        while (!client.sendAdminRequestToTakeASnapshot(requestCorrelationId))
        {
            Tests.yield();
        }

        while (!hasResponse.get())
        {
            client.pollEgress();
            Tests.yield();
        }

        long time = System.nanoTime();
        final long deadline = time + TimeUnit.SECONDS.toNanos(2);
        do
        {
            assertEquals(0, cluster.getSnapshotCount(leader));
            for (final TestNode follower : followers)
            {
                assertEquals(0, cluster.getSnapshotCount(follower));
            }
            Tests.sleep(10);
            time = System.nanoTime();
        }
        while (time < deadline);
    }

    @Test
    @InterruptAfter(10)
    void shouldRejectAnInvalidAdminRequest()
    {
        final AdminRequestType invalidRequestType = AdminRequestType.NULL_VAL;
        final AtomicBoolean isAuthorisedInvoked = new AtomicBoolean();
        cluster = aCluster()
            .withStaticNodes(3)
            .withAuthorisationServiceSupplier(() ->
                (protocolId, actionId, type, encodedPrincipal) ->
                {
                    isAuthorisedInvoked.set(true);
                    assertEquals(MessageHeaderDecoder.SCHEMA_ID, protocolId);
                    assertEquals(AdminRequestEncoder.TEMPLATE_ID, actionId);
                    assertEquals(invalidRequestType, type);
                    return true;
                })
            .start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();

        final long requestCorrelationId = System.nanoTime();
        final MutableBoolean hasResponse = injectAdminResponseEgressListener(
            requestCorrelationId,
            invalidRequestType,
            AdminResponseCode.ERROR,
            "Unknown request type: " + invalidRequestType);

        final AeronCluster client = cluster.connectClient();
        final AdminRequestEncoder adminRequestEncoder = new AdminRequestEncoder()
            .wrapAndApplyHeader(cluster.msgBuffer(), 0, new MessageHeaderEncoder())
            .leadershipTermId(client.leadershipTermId())
            .clusterSessionId(client.clusterSessionId())
            .correlationId(requestCorrelationId)
            .requestType(invalidRequestType);

        final Publication ingressPublication = client.ingressPublication();
        while (ingressPublication.offer(
            adminRequestEncoder.buffer(),
            0,
            MessageHeaderEncoder.ENCODED_LENGTH + adminRequestEncoder.encodedLength()) < 0)
        {
            Tests.yield();
        }

        Tests.await(isAuthorisedInvoked::get);

        while (!hasResponse.get())
        {
            client.pollEgress();
            Tests.yield();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldRejectAnAdminRequestIfLeadershipTermIsInvalid()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();

        AeronCluster client = cluster.connectClient();
        final long requestCorrelationId = System.nanoTime();
        final long expectedLeadershipTermId = client.leadershipTermId();
        final long invalidLeadershipTermId = expectedLeadershipTermId - 1000;
        final AdminRequestType requestType = AdminRequestType.NULL_VAL;
        final MutableBoolean hasResponse = injectAdminResponseEgressListener(
            requestCorrelationId,
            requestType,
            AdminResponseCode.ERROR,
            "Invalid leadership term: expected " + expectedLeadershipTermId + ", got " + invalidLeadershipTermId);
        client = cluster.connectClient();

        final AdminRequestEncoder adminRequestEncoder = new AdminRequestEncoder()
            .wrapAndApplyHeader(cluster.msgBuffer(), 0, new MessageHeaderEncoder())
            .leadershipTermId(invalidLeadershipTermId)
            .clusterSessionId(client.clusterSessionId())
            .correlationId(requestCorrelationId)
            .requestType(requestType);

        final Publication ingressPublication = client.ingressPublication();
        while (ingressPublication.offer(
            adminRequestEncoder.buffer(),
            0,
            MessageHeaderEncoder.ENCODED_LENGTH + adminRequestEncoder.encodedLength()) < 0)
        {
            Tests.yield();
        }

        while (!hasResponse.get())
        {
            client.pollEgress();
            Tests.yield();
        }
    }

    @Test
    @InterruptAfter(20)
    void shouldTakeASnapshotAfterReceivingAdminRequestOfTypeSnapshot()
    {
        cluster = aCluster()
            .withStaticNodes(3)
            .withAuthorisationServiceSupplier(() -> AuthorisationService.ALLOW_ALL)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final long requestCorrelationId = System.nanoTime();
        final MutableBoolean hasResponse = injectAdminResponseEgressListener(
            requestCorrelationId, AdminRequestType.SNAPSHOT, AdminResponseCode.OK, "");

        final AeronCluster client = cluster.connectClient();
        while (!client.sendAdminRequestToTakeASnapshot(requestCorrelationId))
        {
            Tests.yield();
        }

        while (!hasResponse.get())
        {
            client.pollEgress();
            Tests.yield();
        }

        cluster.awaitSnapshotCount(1);
        cluster.awaitNeutralControlToggle(leader);
    }

    @Test
    @InterruptAfter(20)
    void shouldTakeASnapshotAfterReceivingAdminRequestOfTypeSnapshotAndNotifyViaControlledPoll()
    {
        cluster = aCluster()
            .withStaticNodes(3)
            .withAuthorisationServiceSupplier(() ->
                (protocolId, actionId, type, encodedPrincipal) ->
                {
                    assertEquals(MessageHeaderDecoder.SCHEMA_ID, protocolId);
                    assertEquals(AdminRequestEncoder.TEMPLATE_ID, actionId);
                    assertEquals(AdminRequestType.SNAPSHOT, type);
                    return true;
                })
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final long requestCorrelationId = System.nanoTime();
        final MutableBoolean hasResponse = injectAdminRequestControlledEgressListener(
            requestCorrelationId, AdminRequestType.SNAPSHOT, AdminResponseCode.OK, "");

        final AeronCluster client = cluster.connectClient();
        while (!client.sendAdminRequestToTakeASnapshot(requestCorrelationId))
        {
            Tests.yield();
        }

        while (!hasResponse.get())
        {
            client.controlledPollEgress();
            Tests.yield();
        }

        cluster.awaitSnapshotCount(1);
        cluster.awaitNeutralControlToggle(leader);
    }

    private void shouldCatchUpAfterFollowerMissesMessage(final String message)
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

    private MutableBoolean injectAdminResponseEgressListener(
        final long expectedCorrelationId,
        final AdminRequestType expectedRequestType,
        final AdminResponseCode expectedResponseCode,
        final String expectedMessage)
    {
        final MutableBoolean hasResponse = new MutableBoolean();

        cluster.egressListener(
            new EgressListener()
            {
                public void onMessage(
                    final long clusterSessionId,
                    final long timestamp,
                    final DirectBuffer buffer,
                    final int offset,
                    final int length,
                    final Header header)
                {
                }

                public void onAdminResponse(
                    final long clusterSessionId,
                    final long correlationId,
                    final AdminRequestType requestType,
                    final AdminResponseCode responseCode,
                    final String message,
                    final DirectBuffer payload,
                    final int payloadOffset,
                    final int payloadLength)
                {
                    hasResponse.set(true);
                    assertEquals(expectedCorrelationId, correlationId);
                    assertEquals(expectedRequestType, requestType);
                    assertEquals(expectedResponseCode, responseCode);
                    assertEquals(expectedMessage, message);
                    assertNotNull(payload);
                    final int minPayloadOffset =
                        MessageHeaderEncoder.ENCODED_LENGTH +
                        AdminResponseEncoder.BLOCK_LENGTH +
                        AdminResponseEncoder.messageHeaderLength() +
                        message.length() +
                        AdminResponseEncoder.payloadHeaderLength();
                    assertTrue(payloadOffset > minPayloadOffset);
                    assertEquals(0, payloadLength);
                }
            });

        return hasResponse;
    }

    private MutableBoolean injectAdminRequestControlledEgressListener(
        final long expectedCorrelationId,
        final AdminRequestType expectedRequestType,
        final AdminResponseCode expectedResponseCode,
        final String expectedMessage)
    {
        final MutableBoolean hasResponse = new MutableBoolean();

        cluster.controlledEgressListener(
            new ControlledEgressListener()
            {
                public ControlledFragmentHandler.Action onMessage(
                    final long clusterSessionId,
                    final long timestamp,
                    final DirectBuffer buffer,
                    final int offset,
                    final int length,
                    final Header header)
                {
                    return ControlledFragmentHandler.Action.ABORT;
                }

                public void onAdminResponse(
                    final long clusterSessionId,
                    final long correlationId,
                    final AdminRequestType requestType,
                    final AdminResponseCode responseCode,
                    final String message,
                    final DirectBuffer payload,
                    final int payloadOffset,
                    final int payloadLength)
                {
                    hasResponse.set(true);
                    assertEquals(expectedCorrelationId, correlationId);
                    assertEquals(expectedRequestType, requestType);
                    assertEquals(expectedResponseCode, responseCode);
                    assertEquals(expectedMessage, message);
                    assertNotNull(payload);
                    final int minPayloadOffset =
                        MessageHeaderEncoder.ENCODED_LENGTH +
                        AdminResponseEncoder.BLOCK_LENGTH +
                        AdminResponseEncoder.messageHeaderLength() +
                        message.length() +
                        AdminResponseEncoder.payloadHeaderLength();
                    assertTrue(payloadOffset > minPayloadOffset);
                    assertEquals(0, payloadLength);
                }
            });

        return hasResponse;
    }
}
