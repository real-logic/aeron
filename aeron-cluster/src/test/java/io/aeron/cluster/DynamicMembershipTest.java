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

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.cluster.TestCluster.awaitElectionClosed;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.*;

@SlowTest
public class DynamicMembershipTest
{
    @Test
    public void shouldQueryClusterMembers()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();
                final ClusterTool.ClusterMembership clusterMembership = leader.clusterMembership();

                assertEquals(leader.index(), clusterMembership.leaderMemberId);
                assertEquals("", clusterMembership.passiveMembersStr);
                assertEquals(cluster.staticClusterMembers(), clusterMembership.activeMembersStr);
            }
        });
    }

    @Test
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshots()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
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
        });
    }

    @Test
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshotsThenSend()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
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
                cluster.awaitResponses(messageCount);
                cluster.awaitMessageCountForService(leader, messageCount);
                cluster.awaitMessageCountForService(dynamicMember, messageCount);
            }
        });
    }

    @Test
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshotsWithCatchup()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startCluster(3, 1))
            {
                final TestNode leader = cluster.awaitLeader();

                cluster.connectClient();
                final int messageCount = 10;
                cluster.sendMessages(messageCount);
                cluster.awaitResponses(messageCount);
                cluster.awaitMessageCountForService(leader, messageCount);

                final TestNode dynamicMember = cluster.startDynamicNode(3, true);

                cluster.awaitMessageCountForService(dynamicMember, messageCount);
            }
        });
    }

    @Test
    public void shouldDynamicallyJoinClusterOfThreeWithEmptySnapshot()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startCluster(3, 1))
            {
                final TestNode leader = cluster.awaitLeader();

                cluster.takeSnapshot(leader);
                cluster.awaitSnapshotCounter(cluster.node(0), 1);
                cluster.awaitSnapshotCounter(cluster.node(1), 1);
                cluster.awaitSnapshotCounter(cluster.node(2), 1);

                final TestNode dynamicMember = cluster.startDynamicNode(3, true);

                awaitElectionClosed(dynamicMember);
                assertEquals(Cluster.Role.FOLLOWER, dynamicMember.role());

                cluster.awaitSnapshotLoadedForService(dynamicMember);
            }
        });
    }

    @Test
    public void shouldDynamicallyJoinClusterOfThreeWithSnapshot()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startCluster(3, 1))
            {
                final TestNode leader = cluster.awaitLeader();

                cluster.connectClient();
                final int messageCount = 10;
                cluster.sendMessages(messageCount);
                cluster.awaitResponses(messageCount);

                cluster.takeSnapshot(leader);
                cluster.awaitSnapshotCounter(cluster.node(0), 1);
                cluster.awaitSnapshotCounter(cluster.node(1), 1);
                cluster.awaitSnapshotCounter(cluster.node(2), 1);

                final TestNode dynamicMember = cluster.startDynamicNode(3, true);

                awaitElectionClosed(dynamicMember);
                assertEquals(Cluster.Role.FOLLOWER, dynamicMember.role());

                cluster.awaitSnapshotLoadedForService(dynamicMember);
                assertEquals(messageCount, dynamicMember.service().messageCount());
            }
        });
    }

    @Test
    public void shouldDynamicallyJoinClusterOfThreeWithSnapshotThenSend()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startCluster(3, 1))
            {
                final TestNode leader = cluster.awaitLeader();

                cluster.connectClient();
                final int preSnapshotMessageCount = 10;
                final int postSnapshotMessageCount = 7;
                final int totalMessageCount = preSnapshotMessageCount + postSnapshotMessageCount;
                cluster.sendMessages(preSnapshotMessageCount);
                cluster.awaitResponses(preSnapshotMessageCount);

                cluster.takeSnapshot(leader);
                cluster.awaitSnapshotCounter(cluster.node(0), 1);
                cluster.awaitSnapshotCounter(cluster.node(1), 1);
                cluster.awaitSnapshotCounter(cluster.node(2), 1);

                final TestNode dynamicMember = cluster.startDynamicNode(3, true);

                awaitElectionClosed(dynamicMember);
                assertEquals(Cluster.Role.FOLLOWER, dynamicMember.role());

                cluster.awaitSnapshotLoadedForService(dynamicMember);
                assertEquals(preSnapshotMessageCount, dynamicMember.service().messageCount());

                cluster.sendMessages(postSnapshotMessageCount);
                cluster.awaitResponses(totalMessageCount);
                cluster.awaitMessageCountForService(dynamicMember, totalMessageCount);
            }
        });
    }

    @Test
    public void shouldRemoveFollower()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
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
        });
    }

    @Test
    public void shouldRemoveLeader()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
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
        });
    }

    @Test
    public void shouldRemoveLeaderAfterDynamicNodeJoined()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startCluster(3, 1))
            {
                final TestNode initialLeader = cluster.awaitLeader();
                final TestNode dynamicMember = cluster.startDynamicNode(3, true);

                awaitElectionClosed(dynamicMember);

                initialLeader.terminationExpected(true);
                initialLeader.removeMember(initialLeader.index(), false);

                cluster.awaitNodeTermination(initialLeader);
                cluster.stopNode(initialLeader);

                final TestNode newLeader = cluster.awaitLeader(initialLeader.index());
                final ClusterTool.ClusterMembership clusterMembership = awaitMembershipSize(newLeader, 3);

                assertEquals(newLeader.index(), clusterMembership.leaderMemberId);
                assertNotEquals(initialLeader.index(), clusterMembership.leaderMemberId);
            }
        });
    }

    @Test
    public void shouldJoinDynamicNodeToSingleStaticLeader()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startCluster(1, 1))
            {
                final TestNode initialLeader = cluster.awaitLeader();
                final TestNode dynamicMember = cluster.startDynamicNode(1, true);

                awaitElectionClosed(dynamicMember);
                awaitMembershipSize(initialLeader, 2);
            }
        });
    }

    static ClusterTool.ClusterMembership awaitMembershipSize(final TestNode leader, final int size)
        throws InterruptedException
    {
        while (true)
        {
            Thread.sleep(100);
            Tests.checkInterruptedStatus();

            final ClusterTool.ClusterMembership clusterMembership = leader.clusterMembership();
            if (clusterMembership.activeMembers.size() == size)
            {
                return clusterMembership;
            }
        }
    }
}
