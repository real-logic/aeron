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

import io.aeron.log.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import static io.aeron.cluster.service.Cluster.Role.FOLLOWER;
import static io.aeron.test.cluster.TestCluster.*;
import static org.junit.jupiter.api.Assertions.*;

@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
public class DynamicMembershipTest
{
    private TestCluster cluster = null;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @BeforeEach
    void setUp()
    {
        systemTestWatcher.ignoreErrorsMatching(
            (s) -> s.contains("ats_gcm_decrypt final_ex: error:00000000:lib(0):func(0):reason(0)"));
    }

    @Test
    @InterruptAfter(30)
    public void shouldQueryClusterMembers(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final ClusterMembership clusterMembership = leader.clusterMembership();

        assertEquals(leader.index(), clusterMembership.leaderMemberId);
        assertEquals("", clusterMembership.passiveMembersStr);
        assertEquals(cluster.staticClusterMembers(), clusterMembership.activeMembersStr);
    }

    @Test
    @InterruptAfter(30)
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshots(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final TestNode dynamicMember = cluster.startDynamicNode(3, true);

        awaitElectionClosed(dynamicMember);
        assertEquals(FOLLOWER, dynamicMember.role());

        final ClusterMembership clusterMembership = awaitMembershipSize(leader, 4);

        assertEquals(leader.index(), clusterMembership.leaderMemberId);
        assertEquals("", clusterMembership.passiveMembersStr);
    }

    @Test
    @InterruptAfter(30)
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshotsThenSend(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final TestNode dynamicMember = cluster.startDynamicNode(3, true);

        awaitElectionClosed(dynamicMember);
        assertEquals(FOLLOWER, dynamicMember.role());

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServiceMessageCount(leader, messageCount);
        cluster.awaitServiceMessageCount(dynamicMember, messageCount);
    }

    @Test
    @InterruptAfter(30)
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshotsWithCatchup(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServiceMessageCount(leader, messageCount);

        final TestNode dynamicMember = cluster.startDynamicNode(3, true);

        cluster.awaitServiceMessageCount(dynamicMember, messageCount);
    }

    @Test
    @InterruptAfter(30)
    public void shouldDynamicallyJoinClusterOfThreeWithEmptySnapshot(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);

        final TestNode dynamicMember = cluster.startDynamicNode(3, true);

        awaitElectionClosed(dynamicMember);
        assertEquals(FOLLOWER, dynamicMember.role());

        cluster.awaitSnapshotLoadedForService(dynamicMember);
    }

    @Test
    @InterruptAfter(30)
    public void shouldDynamicallyJoinClusterOfThreeWithSnapshot(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);

        final TestNode dynamicMember = cluster.startDynamicNode(3, true);

        awaitElectionClosed(dynamicMember);
        assertEquals(FOLLOWER, dynamicMember.role());

        cluster.awaitSnapshotLoadedForService(dynamicMember);
        assertEquals(messageCount, dynamicMember.service().messageCount());
    }

    @Test
    @InterruptAfter(30)
    public void shouldDynamicallyJoinClusterOfThreeWithSnapshotThenSend(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int preSnapshotMessageCount = 10;
        final int postSnapshotMessageCount = 7;
        final int totalMessageCount = preSnapshotMessageCount + postSnapshotMessageCount;
        cluster.connectClient();
        cluster.sendMessages(preSnapshotMessageCount);
        cluster.awaitResponseMessageCount(preSnapshotMessageCount);

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);
        assertTrue(cluster.client().sendKeepAlive());

        final TestNode dynamicMember = cluster.startDynamicNode(3, true);

        assertTrue(cluster.client().sendKeepAlive());
        awaitElectionClosed(dynamicMember);
        assertEquals(FOLLOWER, dynamicMember.role());

        cluster.awaitSnapshotLoadedForService(dynamicMember);
        assertEquals(preSnapshotMessageCount, dynamicMember.service().messageCount());

        cluster.sendMessages(postSnapshotMessageCount);
        cluster.awaitResponseMessageCount(totalMessageCount);
        cluster.awaitServiceMessageCount(dynamicMember, totalMessageCount);
    }

    @Test
    @InterruptAfter(15)
    public void shouldRemoveFollower(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        // Ensure all members are connected to the log.
        cluster.connectClient();
        cluster.sendMessages(1);
        cluster.awaitResponseMessageCount(1);
        cluster.awaitServicesMessageCount(1);

        final TestNode follower = cluster.followers().get(0);

        follower.isTerminationExpected(true);
        leader.removeMember(follower.index(), false);

        cluster.awaitNodeTermination(follower);
        cluster.stopNode(follower);

        final ClusterMembership clusterMembership = awaitMembershipSize(leader, 2);
        assertEquals(leader.index(), clusterMembership.leaderMemberId);
    }

    @Test
    @InterruptAfter(30)
    public void shouldRemoveLeader(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode initialLeader = cluster.awaitLeader();

        initialLeader.isTerminationExpected(true);
        initialLeader.removeMember(initialLeader.index(), false);

        cluster.awaitNodeTermination(initialLeader);
        cluster.stopNode(initialLeader);

        final TestNode newLeader = cluster.awaitLeader(initialLeader.index());
        final ClusterMembership clusterMembership = awaitMembershipSize(newLeader, 2);

        assertEquals(newLeader.index(), clusterMembership.leaderMemberId);
        assertNotEquals(initialLeader.index(), clusterMembership.leaderMemberId);
    }

    @Test
    @InterruptAfter(30)
    public void shouldRemoveLeaderAfterDynamicNodeJoined(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        systemTestWatcher.cluster(cluster);

        final TestNode initialLeader = cluster.awaitLeader();
        final TestNode dynamicMember = cluster.startDynamicNode(3, true);

        awaitElectionClosed(dynamicMember);
        awaitMembershipSize(initialLeader, 4);

        initialLeader.isTerminationExpected(true);
        initialLeader.removeMember(initialLeader.index(), false);

        cluster.awaitNodeTermination(initialLeader);
        cluster.stopNode(initialLeader);

        final TestNode newLeader = cluster.awaitLeader(initialLeader.index());
        final ClusterMembership clusterMembership = awaitMembershipSize(newLeader, 3);

        assertEquals(newLeader.index(), clusterMembership.leaderMemberId);
        assertNotEquals(initialLeader.index(), clusterMembership.leaderMemberId);
    }

    @Test
    @InterruptAfter(30)
    public void shouldRemoveLeaderAfterDynamicNodeJoinedThenRestartCluster(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        systemTestWatcher.cluster(cluster);

        final TestNode initialLeader = cluster.awaitLeader();
        final TestNode dynamicMember = cluster.startDynamicNode(3, true);

        awaitElectionClosed(dynamicMember);
        awaitMembershipSize(initialLeader, 4);

        final int initialLeaderIndex = initialLeader.index();
        initialLeader.isTerminationExpected(true);
        initialLeader.removeMember(initialLeaderIndex, false);

        cluster.awaitNodeTermination(initialLeader);
        cluster.stopNode(initialLeader);

        final TestNode newLeader = cluster.awaitLeader(initialLeaderIndex);
        final ClusterMembership clusterMembership = awaitMembershipSize(newLeader, 3);

        assertEquals(newLeader.index(), clusterMembership.leaderMemberId);
        assertNotEquals(initialLeaderIndex, clusterMembership.leaderMemberId);

        cluster.stopAllNodes();

        for (int i = 0; i < 3; i++)
        {
            if (initialLeaderIndex != i)
            {
                cluster.startStaticNode(i, false);
            }
        }

        cluster.awaitLeader();
        assertEquals(1, cluster.followers().size());
        awaitElectionClosed(cluster.followers().get(0));
    }

    @Test
    @InterruptAfter(30)
    public void shouldJoinDynamicNodeToSingleStaticLeader(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(1).withDynamicNodes(1).start();
        systemTestWatcher.cluster(cluster);

        final TestNode initialLeader = cluster.awaitLeader();
        final TestNode dynamicMember = cluster.startDynamicNode(1, true);

        awaitElectionClosed(dynamicMember);
        awaitMembershipSize(initialLeader, 2);
    }

    @Test
    @InterruptAfter(30)
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshotsAndRestartDynamicNode(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final TestNode dynamicMember = cluster.startDynamicNode(3, true);

        awaitElectionClosed(dynamicMember);
        assertEquals(FOLLOWER, dynamicMember.role());

        final ClusterMembership clusterMembership = awaitMembershipSize(leader, 4);

        assertEquals(leader.index(), clusterMembership.leaderMemberId);
        assertEquals("", clusterMembership.passiveMembersStr);

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);

        cluster.stopNode(dynamicMember);
        final TestNode staticMember = cluster.startStaticNodeFromDynamicNode(3);

        awaitElectionClosed(staticMember);
        cluster.awaitServiceMessageCount(cluster.node(3), messageCount);
    }

    @Test
    @InterruptAfter(60)
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshotsWithLogReplicationAndCatchup()
    {
        final int messageCount = 10;
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader0 = cluster.awaitLeader();

        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);
        cluster.takeSnapshot(leader0);
        cluster.awaitSnapshotCount(1);

        cluster.stopNode(leader0);
        final TestNode leader1 = cluster.awaitLeader();
        cluster.startStaticNode(leader0.index(), false);
        TestCluster.awaitElectionClosed(cluster.node(leader0.index()));

        cluster.reconnectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(2 * messageCount);
        cluster.awaitServicesMessageCount(2 * messageCount);

        cluster.stopNode(leader1);
        final TestNode leader2 = cluster.awaitLeader();
        cluster.startStaticNode(leader1.index(), false);
        TestCluster.awaitElectionClosed(cluster.node(leader1.index()));

        cluster.reconnectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(3 * messageCount);
        cluster.awaitServicesMessageCount(3 * messageCount);

        final TestNode dynamicMember = cluster.startDynamicNode(3, true);
        cluster.awaitServiceMessageCount(dynamicMember, 3 * messageCount);
    }

    @Test
    @InterruptAfter(60)
    public void shouldDynamicallyJoinClusterOfThreeWithSnapshotWithDynamicLeader(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(3).start();
        systemTestWatcher.cluster(cluster);
        systemTestWatcher.showAllErrors();
        systemTestWatcher.ignoreErrorsMatching((s) -> s.contains("expected termination"));

        final TestNode staticLeader = cluster.awaitLeader();
        final List<TestNode> staticFollowers = cluster.followers();
        final TestNode staticFollowerA = staticFollowers.get(0);
        final TestNode staticFollowerB = staticFollowers.get(1);

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        cluster.takeSnapshot(staticLeader);
        cluster.awaitSnapshotCount(1);

        staticFollowerA.isTerminationExpected(true);
        staticLeader.removeMember(staticFollowerA.index(), false);

        cluster.awaitNodeTermination(staticFollowerA);
        cluster.stopNode(staticFollowerA);

        awaitMembershipSize(staticLeader, 2);

        final TestNode dynamicMemberA = cluster.startDynamicNode(3, true);

        awaitElectionClosed(dynamicMemberA);
        assertEquals(FOLLOWER, dynamicMemberA.role());

        cluster.awaitSnapshotLoadedForService(dynamicMemberA);
        assertEquals(messageCount, dynamicMemberA.service().messageCount());

        awaitMembershipSize(staticLeader, 3);

        staticFollowerB.isTerminationExpected(true);
        staticLeader.removeMember(staticFollowerB.index(), false);

        cluster.awaitNodeTermination(staticFollowerB);
        cluster.stopNode(staticFollowerB);

        awaitMembershipSize(staticLeader, 2);

        final TestNode dynamicMemberB = cluster.startDynamicNode(4, true);

        awaitElectionClosed(dynamicMemberB);
        assertEquals(FOLLOWER, dynamicMemberB.role());

        cluster.awaitSnapshotLoadedForService(dynamicMemberB);
        assertEquals(messageCount, dynamicMemberB.service().messageCount());

        awaitMembershipSize(staticLeader, 3);

        final int initialLeaderIndex = staticLeader.index();
        staticLeader.isTerminationExpected(true);
        staticLeader.removeMember(initialLeaderIndex, false);

        cluster.awaitNodeTermination(staticLeader);
        cluster.stopNode(staticLeader);

        final TestNode newLeader = cluster.awaitLeader(initialLeaderIndex);

        awaitMembershipSize(newLeader, 2);

        final TestNode dynamicMemberC = cluster.startDynamicNodeConsensusEndpoints(5, true);

        awaitElectionClosed(dynamicMemberC);
        assertEquals(FOLLOWER, dynamicMemberC.role());

        cluster.awaitSnapshotLoadedForService(dynamicMemberC);
        assertEquals(messageCount, dynamicMemberC.service().messageCount());

        awaitMembershipSize(newLeader, 3);

        awaitElectionClosed(dynamicMemberC);
        assertEquals(FOLLOWER, dynamicMemberC.role());
    }

    @Test
    @InterruptAfter(10)
    public void shouldDynamicallyJoinMemberAfterRecyclingAllStaticNodes(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(4).start();
        systemTestWatcher.cluster(cluster);
        systemTestWatcher.showAllErrors();
        systemTestWatcher.ignoreErrorsMatching((s) -> s.contains("expected termination"));

        final TestNode staticLeader = cluster.awaitLeader();
        final List<TestNode> staticFollowers = cluster.followers();
        final TestNode staticFollowerA = staticFollowers.get(0);
        final TestNode staticFollowerB = staticFollowers.get(1);

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        cluster.takeSnapshot(staticLeader);
        cluster.awaitSnapshotCount(1);

        staticFollowerA.isTerminationExpected(true);
        staticLeader.removeMember(staticFollowerA.index(), false);

        cluster.awaitNodeTermination(staticFollowerA);
        cluster.stopNode(staticFollowerA);

        awaitMembershipSize(staticLeader, 2);

        final TestNode dynamicMemberA = cluster.startDynamicNode(3, true);

        awaitElectionClosed(dynamicMemberA);
        assertEquals(FOLLOWER, dynamicMemberA.role());

        cluster.awaitSnapshotLoadedForService(dynamicMemberA);
        assertEquals(messageCount, dynamicMemberA.service().messageCount());

        awaitMembershipSize(staticLeader, 3);

        staticFollowerB.isTerminationExpected(true);
        staticLeader.removeMember(staticFollowerB.index(), false);

        cluster.awaitNodeTermination(staticFollowerB);
        cluster.stopNode(staticFollowerB);

        awaitMembershipSize(staticLeader, 2);

        final TestNode dynamicMemberB = cluster.startDynamicNode(4, true);

        awaitElectionClosed(dynamicMemberB);
        assertEquals(FOLLOWER, dynamicMemberB.role());

        cluster.awaitSnapshotLoadedForService(dynamicMemberB);
        assertEquals(messageCount, dynamicMemberB.service().messageCount());

        awaitMembershipSize(staticLeader, 3);

        final int initialLeaderIndex = staticLeader.index();
        staticLeader.isTerminationExpected(true);
        staticLeader.removeMember(initialLeaderIndex, false);

        cluster.awaitNodeTermination(staticLeader);
        cluster.stopNode(staticLeader);

        final TestNode newLeader = cluster.awaitLeader(initialLeaderIndex);

        awaitMembershipSize(newLeader, 2);

        final TestNode dynamicMemberC = cluster.startDynamicNodeConsensusEndpoints(5, true);

        awaitElectionClosed(dynamicMemberC);
        assertEquals(FOLLOWER, dynamicMemberC.role());

        cluster.awaitSnapshotLoadedForService(dynamicMemberC);
        assertEquals(messageCount, dynamicMemberC.service().messageCount());

        awaitMembershipSize(newLeader, 3);

        final long snapshotCount = cluster.getSnapshotCount(newLeader);
        cluster.takeSnapshot(newLeader);
        cluster.awaitSnapshotCount(snapshotCount + 1);

        final List<TestNode> dynamicFollowers = cluster.followers();
        final TestNode dynamicFollowerA = dynamicFollowers.get(0);

        dynamicFollowerA.isTerminationExpected(true);
        newLeader.removeMember(dynamicFollowerA.index(), false);
        cluster.awaitNodeTermination(dynamicFollowerA);
        cluster.stopNode(dynamicFollowerA);

        awaitMembershipSize(newLeader, 2);

        final TestNode dynamicMemberD = cluster.startDynamicNodeConsensusEndpoints(6, true);

        awaitElectionClosed(dynamicMemberD);
        assertEquals(FOLLOWER, dynamicMemberD.role());
        cluster.awaitSnapshotLoadedForService(dynamicMemberD);
        assertEquals(messageCount, dynamicMemberD.service().messageCount());
        awaitMembershipSize(newLeader, 3);
    }

    @Test
    @InterruptAfter(30)
    public void shouldDynamicallyJoinMemberAfterSnapshotOnNonZeroTerm(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader0 = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);

        cluster.stopNode(leader0);
        final TestNode leader1 = cluster.awaitLeader();
        leader1.removeMember(leader0.index(), false);

        awaitMembershipSize(leader1, 2);

        cluster.takeSnapshot(leader1);
        cluster.awaitSnapshotCount(1);

        final TestNode dynamicMember0 = cluster.startDynamicNode(3, true);
        awaitElectionClosed(dynamicMember0);
        assertEquals(FOLLOWER, dynamicMember0.role());

        cluster.awaitSnapshotLoadedForService(dynamicMember0);
        assertEquals(messageCount, dynamicMember0.service().messageCount());

        awaitMembershipSize(leader1, 3);
    }

    @Test
    @InterruptAfter(60)
    public void shouldDynamicallyJoinMemberAfterSnapshotOnNonZeroTermAndSubsequentLeadershipTerms()
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader0 = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);

        cluster.stopNode(leader0);
        final TestNode leader1 = cluster.awaitLeader();
        cluster.startStaticNode(leader0.index(), false);
        TestCluster.awaitElectionClosed(leader0);

        cluster.takeSnapshot(leader1);
        cluster.awaitSnapshotCount(1);

        cluster.reconnectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(2 * messageCount);
        cluster.awaitServicesMessageCount(2 * messageCount);

        cluster.stopNode(leader1);
        final TestNode leader2 = cluster.awaitLeader();
        cluster.startStaticNode(leader1.index(), false);
        TestCluster.awaitElectionClosed(leader1);

        cluster.reconnectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(3 * messageCount);
        cluster.awaitServicesMessageCount(3 * messageCount);

        final TestNode follower = cluster.followers().get(0);
        follower.isTerminationExpected(true);
        leader2.removeMember(follower.index(), false);
        cluster.awaitNodeTermination(follower);
        cluster.stopNode(follower);

        awaitMembershipSize(leader2, 2);

        final TestNode dynamicMember0 = cluster.startDynamicNode(3, true);
        awaitElectionClosed(dynamicMember0);
        assertEquals(FOLLOWER, dynamicMember0.role());

        cluster.awaitSnapshotLoadedForService(dynamicMember0);
        cluster.awaitServiceMessageCount(dynamicMember0, 3 * messageCount);
        awaitMembershipSize(leader2, 3);
    }
}
