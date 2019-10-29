/*
 * Copyright 2014-2019 Real Logic Ltd.
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
import org.junit.Ignore;
import org.junit.Test;

import static io.aeron.Aeron.NULL_VALUE;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;

@Ignore
public class DynamicMembershipTest
{
    @Test(timeout = 10_000)
    public void shouldQueryClusterMembers() throws Exception
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            final ClusterTool.ClusterMembership clusterMembership = leader.clusterMembership();

            assertThat(clusterMembership.leaderMemberId, is(leader.index()));
            assertThat(clusterMembership.passiveMembersStr, is(""));
            assertThat(clusterMembership.activeMembersStr, is(cluster.staticClusterMembers()));
        }
    }

    @Test(timeout = 10_000)
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshots() throws Exception
    {
        try (TestCluster cluster = TestCluster.startCluster(3, 1))
        {
            final TestNode leader = cluster.awaitLeader();
            final TestNode dynamicMember = cluster.startDynamicNode(3, true);

            Thread.sleep(1000);

            assertThat(dynamicMember.role(), is(Cluster.Role.FOLLOWER));

            final ClusterTool.ClusterMembership clusterMembership = leader.clusterMembership();

            assertThat(clusterMembership.leaderMemberId, is(leader.index()));
            assertThat(clusterMembership.passiveMembersStr, is(""));
            assertThat(numberOfMembers(clusterMembership), is(4));
        }
    }

    @Test(timeout = 10_000)
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshotsThenSend() throws Exception
    {
        try (TestCluster cluster = TestCluster.startCluster(3, 1))
        {
            final TestNode leader = cluster.awaitLeader();
            final TestNode dynamicMember = cluster.startDynamicNode(3, true);

            Thread.sleep(1000);

            assertThat(dynamicMember.role(), is(Cluster.Role.FOLLOWER));

            cluster.connectClient();
            final int messageCount = 10;
            cluster.sendMessages(messageCount);
            cluster.awaitResponses(messageCount);
            cluster.awaitMessageCountForService(leader, messageCount);
            cluster.awaitMessageCountForService(dynamicMember, messageCount);
        }
    }

    @Test(timeout = 10_000)
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshotsWithCatchup() throws Exception
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
    }

    @Test(timeout = 10_000)
    public void shouldDynamicallyJoinClusterOfThreeWithEmptySnapshot() throws Exception
    {
        try (TestCluster cluster = TestCluster.startCluster(3, 1))
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.takeSnapshot(leader);
            cluster.awaitSnapshotCounter(cluster.node(0), 1);
            cluster.awaitSnapshotCounter(cluster.node(1), 1);
            cluster.awaitSnapshotCounter(cluster.node(2), 1);

            final TestNode dynamicMember = cluster.startDynamicNode(3, true);

            Thread.sleep(1000);

            assertThat(dynamicMember.role(), is(Cluster.Role.FOLLOWER));

            cluster.awaitSnapshotLoadedForService(dynamicMember);
        }
    }

    @Test(timeout = 10_000)
    public void shouldDynamicallyJoinClusterOfThreeWithSnapshot() throws Exception
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

            Thread.sleep(1000);

            assertThat(dynamicMember.role(), is(Cluster.Role.FOLLOWER));

            cluster.awaitSnapshotLoadedForService(dynamicMember);
            assertThat(dynamicMember.service().messageCount(), is(messageCount));
        }
    }

    @Test(timeout = 10_000)
    public void shouldDynamicallyJoinClusterOfThreeWithSnapshotThenSend() throws Exception
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

            Thread.sleep(1000);

            assertThat(dynamicMember.role(), is(Cluster.Role.FOLLOWER));

            cluster.awaitSnapshotLoadedForService(dynamicMember);
            assertThat(dynamicMember.service().messageCount(), is(preSnapshotMessageCount));

            cluster.sendMessages(postSnapshotMessageCount);
            cluster.awaitResponses(totalMessageCount);
            cluster.awaitMessageCountForService(dynamicMember, totalMessageCount);
        }
    }

    @Test(timeout = 10_000)
    public void shouldRemoveFollower() throws Exception
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();
            final TestNode follower = cluster.followers().get(0);

            follower.terminationExpected(true);
            leader.removeMember(follower.index(), false);

            cluster.awaitNodeTermination(follower);
            cluster.stopNode(follower);

            final ClusterTool.ClusterMembership clusterMembership = leader.clusterMembership();

            assertThat(clusterMembership.leaderMemberId, is(leader.index()));
            assertThat(numberOfMembers(clusterMembership), is(2));
        }
    }

    @Test(timeout = 10_000)
    public void shouldRemoveLeader() throws Exception
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode initialLeader = cluster.awaitLeader();

            initialLeader.terminationExpected(true);
            initialLeader.removeMember(initialLeader.index(), false);

            cluster.awaitNodeTermination(initialLeader);
            cluster.stopNode(initialLeader);

            final TestNode newLeader = cluster.awaitLeader(initialLeader.index());
            final ClusterTool.ClusterMembership clusterMembership = newLeader.clusterMembership();

            assertThat(clusterMembership.leaderMemberId, is(newLeader.index()));
            assertThat(clusterMembership.leaderMemberId, not(initialLeader.index()));
            assertThat(numberOfMembers(clusterMembership), is(2));
        }
    }

    @Test(timeout = 10_000)
    public void shouldRemoveLeaderAfterDynamicNodeJoined() throws Exception
    {
        try (TestCluster cluster = TestCluster.startCluster(3, 1))
        {
            final TestNode initialLeader = cluster.awaitLeader();
            cluster.startDynamicNode(3, true);

            Thread.sleep(1000);

            initialLeader.terminationExpected(true);
            initialLeader.removeMember(initialLeader.index(), false);

            cluster.awaitNodeTermination(initialLeader);
            cluster.stopNode(initialLeader);

            final TestNode newLeader = cluster.awaitLeader(initialLeader.index());
            final ClusterTool.ClusterMembership clusterMembership = newLeader.clusterMembership();

            assertThat(clusterMembership.leaderMemberId, is(newLeader.index()));
            assertThat(clusterMembership.leaderMemberId, not(initialLeader.index()));
            assertThat(numberOfMembers(clusterMembership), is(3));
        }
    }

    private int numberOfMembers(final ClusterTool.ClusterMembership clusterMembership)
    {
        return clusterMembership.activeMembers.size();
    }
}
