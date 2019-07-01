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

import org.junit.Ignore;
import org.junit.Test;

import static io.aeron.Aeron.NULL_VALUE;
import static org.junit.Assert.*;

@Ignore
public class BackupTest
{
    @Test(timeout = 10_000)
    public void shouldBackupClusterNoSnapshotsAndEmptyLog() throws Exception
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            cluster.awaitLeader();
            final TestBackupNode backupNode = cluster.startClusterBackupNode(true);

            cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
            cluster.awaitBackupLiveLogPosition(cluster.findLeader().service().cluster().logPosition());

            cluster.stopAllNodes();

            final TestNode node = cluster.startStaticNodeFromBackup();
            cluster.awaitLeader();

            assertEquals(0, node.service().messageCount());
            assertFalse(node.service().wasSnapshotLoaded());
        }
    }

    @Test(timeout = 10_000)
    public void shouldBackupClusterNoSnapshotsAndNonEmptyLog() throws Exception
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.connectClient();
            cluster.sendMessages(10);
            cluster.awaitResponses(10);
            cluster.awaitMessageCountForService(leader, 10);

            final long logPosition = leader.service().cluster().logPosition();

            final TestBackupNode backupNode = cluster.startClusterBackupNode(true);

            cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
            cluster.awaitBackupLiveLogPosition(logPosition);

            cluster.stopAllNodes();

            final TestNode node = cluster.startStaticNodeFromBackup();
            cluster.awaitLeader();

            assertEquals(10, node.service().messageCount());
            assertFalse(node.service().wasSnapshotLoaded());
        }
    }

    @Test(timeout = 10_000)
    public void shouldBackupClusterNoSnapshotsAndThenSendMessages() throws Exception
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();
            final TestBackupNode backupNode = cluster.startClusterBackupNode(true);

            cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);

            cluster.connectClient();
            cluster.sendMessages(10);
            cluster.awaitResponses(10);
            cluster.awaitMessageCountForService(leader, 10);

            final long logPosition = leader.service().cluster().logPosition();

            cluster.awaitBackupLiveLogPosition(logPosition);

            cluster.stopAllNodes();

            final TestNode node = cluster.startStaticNodeFromBackup();
            cluster.awaitLeader();

            assertEquals(10, node.service().messageCount());
            assertFalse(node.service().wasSnapshotLoaded());
        }
    }

    @Test(timeout = 10_000)
    public void shouldBackupClusterWithSnapshot() throws Exception
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.connectClient();
            cluster.sendMessages(10);
            cluster.awaitResponses(10);
            cluster.awaitMessageCountForService(leader, 10);

            cluster.takeSnapshot(leader);
            cluster.awaitSnapshotCounter(cluster.node(0), 1);
            cluster.awaitSnapshotCounter(cluster.node(1), 1);
            cluster.awaitSnapshotCounter(cluster.node(2), 1);

            final long logPosition = leader.service().cluster().logPosition();

            final TestBackupNode backupNode = cluster.startClusterBackupNode(true);

            cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
            cluster.awaitBackupLiveLogPosition(logPosition);

            cluster.stopAllNodes();

            final TestNode node = cluster.startStaticNodeFromBackup();
            cluster.awaitLeader();

            assertEquals(10, node.service().messageCount());
            assertTrue(node.service().wasSnapshotLoaded());
        }
    }

    @Test(timeout = 10_000)
    public void shouldBackupClusterWithSnapshotAndNonEmptyLog() throws Exception
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.connectClient();
            final int preSnapshotMessageCount = 10;
            final int postSnapshotMessageCount = 7;
            final int totalMessageCount = preSnapshotMessageCount + postSnapshotMessageCount;
            cluster.sendMessages(preSnapshotMessageCount);
            cluster.awaitResponses(preSnapshotMessageCount);
            cluster.awaitMessageCountForService(leader, preSnapshotMessageCount);

            cluster.takeSnapshot(leader);
            cluster.awaitSnapshotCounter(cluster.node(0), 1);
            cluster.awaitSnapshotCounter(cluster.node(1), 1);
            cluster.awaitSnapshotCounter(cluster.node(2), 1);

            cluster.sendMessages(postSnapshotMessageCount);
            cluster.awaitResponses(totalMessageCount);
            cluster.awaitMessageCountForService(leader, totalMessageCount);

            final long logPosition = leader.service().cluster().logPosition();

            final TestBackupNode backupNode = cluster.startClusterBackupNode(true);

            cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
            cluster.awaitBackupLiveLogPosition(logPosition);

            cluster.stopAllNodes();

            final TestNode node = cluster.startStaticNodeFromBackup();
            cluster.awaitLeader();
            cluster.awaitMessageCountForService(node, totalMessageCount);

            assertEquals(totalMessageCount, node.service().messageCount());
            assertTrue(node.service().wasSnapshotLoaded());
        }
    }

    @Test(timeout = 10_000)
    public void shouldBackupClusterWithSnapshotThenSend() throws Exception
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.connectClient();
            final int preSnapshotMessageCount = 10;
            final int postSnapshotMessageCount = 7;
            final int totalMessageCount = preSnapshotMessageCount + postSnapshotMessageCount;
            cluster.sendMessages(preSnapshotMessageCount);
            cluster.awaitResponses(preSnapshotMessageCount);
            cluster.awaitMessageCountForService(leader, preSnapshotMessageCount);

            cluster.takeSnapshot(leader);
            cluster.awaitSnapshotCounter(cluster.node(0), 1);
            cluster.awaitSnapshotCounter(cluster.node(1), 1);
            cluster.awaitSnapshotCounter(cluster.node(2), 1);

            final TestBackupNode backupNode = cluster.startClusterBackupNode(true);

            cluster.sendMessages(postSnapshotMessageCount);
            cluster.awaitResponses(totalMessageCount);
            cluster.awaitMessageCountForService(leader, totalMessageCount);

            final long logPosition = leader.service().cluster().logPosition();

            cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
            cluster.awaitBackupLiveLogPosition(logPosition);

            cluster.stopAllNodes();

            final TestNode node = cluster.startStaticNodeFromBackup();
            cluster.awaitLeader();
            cluster.awaitMessageCountForService(node, totalMessageCount);

            assertEquals(totalMessageCount, node.service().messageCount());
            assertTrue(node.service().wasSnapshotLoaded());
        }
    }
}
