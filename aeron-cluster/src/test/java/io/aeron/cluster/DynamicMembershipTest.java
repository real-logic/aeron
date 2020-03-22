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

import io.aeron.cluster.service.Cluster;
import io.aeron.test.SlowTest;
import io.aeron.test.Tests;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.cluster.TestCluster.awaitElectionClosed;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@SlowTest
public class DynamicMembershipTest
{
    @Test
    @Timeout(30)
    public void shouldQueryClusterMembers()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();
            final ClusterTool.ClusterMembership clusterMembership = leader.clusterMembership();

            assertEquals(leader.index(), clusterMembership.leaderMemberId);
            assertEquals("", clusterMembership.passiveMembersStr);
            assertEquals(cluster.staticClusterMembers(), clusterMembership.activeMembersStr);
        }
    }

    @Test
    @Timeout(30)
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshots()
    {
        try (TestCluster cluster = TestCluster.startCluster(3, 1))
        {
            final TestNode leader = cluster.awaitLeader();
            final TestNode dynamicMember = cluster.startDynamicNode(3, true);

            awaitElectionClosed(dynamicMember);
            assertEquals(Cluster.Role.FOLLOWER, dynamicMember.role());

            final ClusterTool.ClusterMembership clusterMembership = awaitMembershipSize(leader, 4);

            assertEquals(leader.index(), clusterMembership.leaderMemberId);
            assertEquals("", clusterMembership.passiveMembersStr);
        }
    }

    @Test
    @Timeout(30)
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshotsThenSend()
    {
        try (TestCluster cluster = TestCluster.startCluster(3, 1))
        {
            final TestNode leader = cluster.awaitLeader();
            final TestNode dynamicMember = cluster.startDynamicNode(3, true);

            awaitElectionClosed(dynamicMember);
            assertEquals(Cluster.Role.FOLLOWER, dynamicMember.role());

            cluster.connectClient();
            final int messageCount = 10;
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);
            cluster.awaitServiceMessageCount(leader, messageCount);
            cluster.awaitServiceMessageCount(dynamicMember, messageCount);
        }
    }

    @Test
    @Timeout(30)
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshotsWithCatchup()
    {
        try (TestCluster cluster = TestCluster.startCluster(3, 1))
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.connectClient();
            final int messageCount = 10;
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);
            cluster.awaitServiceMessageCount(leader, messageCount);

            final TestNode dynamicMember = cluster.startDynamicNode(3, true);

            cluster.awaitServiceMessageCount(dynamicMember, messageCount);
        }
    }

    @Test
    @Timeout(30)
    public void shouldDynamicallyJoinClusterOfThreeWithEmptySnapshot()
    {
        try (TestCluster cluster = TestCluster.startCluster(3, 1))
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.takeSnapshot(leader);
            cluster.awaitSnapshotCount(cluster.node(0), 1);
            cluster.awaitSnapshotCount(cluster.node(1), 1);
            cluster.awaitSnapshotCount(cluster.node(2), 1);

            final TestNode dynamicMember = cluster.startDynamicNode(3, true);

            awaitElectionClosed(dynamicMember);
            assertEquals(Cluster.Role.FOLLOWER, dynamicMember.role());

            cluster.awaitSnapshotLoadedForService(dynamicMember);
        }
    }

    @Test
    @Timeout(30)
    public void shouldDynamicallyJoinClusterOfThreeWithSnapshot()
    {
        try (TestCluster cluster = TestCluster.startCluster(3, 1))
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.connectClient();
            final int messageCount = 10;
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);

            cluster.takeSnapshot(leader);
            cluster.awaitSnapshotCount(cluster.node(0), 1);
            cluster.awaitSnapshotCount(cluster.node(1), 1);
            cluster.awaitSnapshotCount(cluster.node(2), 1);

            final TestNode dynamicMember = cluster.startDynamicNode(3, true);

            awaitElectionClosed(dynamicMember);
            assertEquals(Cluster.Role.FOLLOWER, dynamicMember.role());

            cluster.awaitSnapshotLoadedForService(dynamicMember);
            assertEquals(messageCount, dynamicMember.service().messageCount());
        }
    }

    @Test
    @Timeout(30)
    public void shouldDynamicallyJoinClusterOfThreeWithSnapshotThenSend()
    {
        try (TestCluster cluster = TestCluster.startCluster(3, 1))
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.connectClient();
            final int preSnapshotMessageCount = 10;
            final int postSnapshotMessageCount = 7;
            final int totalMessageCount = preSnapshotMessageCount + postSnapshotMessageCount;
            cluster.sendMessages(preSnapshotMessageCount);
            cluster.awaitResponseMessageCount(preSnapshotMessageCount);

            cluster.takeSnapshot(leader);
            cluster.awaitSnapshotCount(cluster.node(0), 1);
            cluster.awaitSnapshotCount(cluster.node(1), 1);
            cluster.awaitSnapshotCount(cluster.node(2), 1);

            final TestNode dynamicMember = cluster.startDynamicNode(3, true);

            awaitElectionClosed(dynamicMember);
            assertEquals(Cluster.Role.FOLLOWER, dynamicMember.role());

            cluster.awaitSnapshotLoadedForService(dynamicMember);
            assertEquals(preSnapshotMessageCount, dynamicMember.service().messageCount());

            cluster.sendMessages(postSnapshotMessageCount);
            cluster.awaitResponseMessageCount(totalMessageCount);
            cluster.awaitServiceMessageCount(dynamicMember, totalMessageCount);
        }
    }

    @Test
    @Timeout(30)
    public void shouldRemoveFollower()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();
            final TestNode follower = cluster.followers().get(0);

            follower.terminationExpected(true);
            leader.removeMember(follower.index(), false);

            cluster.awaitNodeTermination(follower);
            cluster.stopNode(follower);

            final ClusterTool.ClusterMembership clusterMembership = awaitMembershipSize(leader, 2);
            assertEquals(leader.index(), clusterMembership.leaderMemberId);
        }
    }

    @Test
    @Timeout(30)
    public void shouldRemoveLeader()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode initialLeader = cluster.awaitLeader();

            initialLeader.terminationExpected(true);
            initialLeader.removeMember(initialLeader.index(), false);

            cluster.awaitNodeTermination(initialLeader);
            cluster.stopNode(initialLeader);

            final TestNode newLeader = cluster.awaitLeader(initialLeader.index());
            final ClusterTool.ClusterMembership clusterMembership = awaitMembershipSize(newLeader, 2);

            assertEquals(newLeader.index(), clusterMembership.leaderMemberId);
            assertNotEquals(initialLeader.index(), clusterMembership.leaderMemberId);
        }
    }

    @Test
    @Timeout(30)
    public void shouldRemoveLeaderAfterDynamicNodeJoined()
    {
        try (TestCluster cluster = TestCluster.startCluster(3, 1))
        {
            final TestNode initialLeader = cluster.awaitLeader();
            final TestNode dynamicMember = cluster.startDynamicNode(3, true);

            awaitElectionClosed(dynamicMember);
            awaitMembershipSize(initialLeader, 4);

            initialLeader.terminationExpected(true);
            initialLeader.removeMember(initialLeader.index(), false);

            cluster.awaitNodeTermination(initialLeader);
            cluster.stopNode(initialLeader);

            final TestNode newLeader = cluster.awaitLeader(initialLeader.index());
            final ClusterTool.ClusterMembership clusterMembership = awaitMembershipSize(newLeader, 3);

            assertEquals(newLeader.index(), clusterMembership.leaderMemberId);
            assertNotEquals(initialLeader.index(), clusterMembership.leaderMemberId);
        }
    }

    @Test
    @Timeout(30)
    public void shouldJoinDynamicNodeToSingleStaticLeader()
    {
        try (TestCluster cluster = TestCluster.startCluster(1, 1))
        {
            final TestNode initialLeader = cluster.awaitLeader();
            final TestNode dynamicMember = cluster.startDynamicNode(1, true);

            awaitElectionClosed(dynamicMember);
            awaitMembershipSize(initialLeader, 2);
        }
    }

    static ClusterTool.ClusterMembership awaitMembershipSize(final TestNode leader, final int size)
    {
        while (true)
        {
            Tests.sleep(100);

            final ClusterTool.ClusterMembership clusterMembership = leader.clusterMembership();
            if (clusterMembership.activeMembers.size() == size)
            {
                return clusterMembership;
            }
        }
    }
}
