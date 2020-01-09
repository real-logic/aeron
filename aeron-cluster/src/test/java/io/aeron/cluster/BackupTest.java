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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static io.aeron.Aeron.NULL_VALUE;
import static java.time.Duration.ofSeconds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.*;

@Disabled
public class BackupTest
{
    @Test
    public void shouldBackupClusterNoSnapshotsAndEmptyLog()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                cluster.awaitLeader();
                cluster.startClusterBackupNode(true);

                cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
                cluster.awaitBackupLiveLogPosition(cluster.findLeader().service().cluster().logPosition());

                cluster.stopAllNodes();

                final TestNode node = cluster.startStaticNodeFromBackup();
                cluster.awaitLeader();

                assertEquals(0, node.service().messageCount());
                assertFalse(node.service().wasSnapshotLoaded());
            }
        });
    }

    @Test
    public void shouldBackupClusterNoSnapshotsAndNonEmptyLog()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();

                cluster.connectClient();
                cluster.sendMessages(10);
                cluster.awaitResponses(10);
                cluster.awaitMessageCountForService(leader, 10);

                final long logPosition = leader.service().cluster().logPosition();

                cluster.startClusterBackupNode(true);

                cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
                cluster.awaitBackupLiveLogPosition(logPosition);

                cluster.stopAllNodes();

                final TestNode node = cluster.startStaticNodeFromBackup();
                cluster.awaitLeader();

                assertEquals(10, node.service().messageCount());
                assertFalse(node.service().wasSnapshotLoaded());
            }
        });
    }

    @Test
    public void shouldBackupClusterNoSnapshotsAndThenSendMessages()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();
                cluster.startClusterBackupNode(true);

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
        });
    }

    @Test
    public void shouldBackupClusterWithSnapshot()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
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

                cluster.startClusterBackupNode(true);

                cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
                cluster.awaitBackupLiveLogPosition(logPosition);

                cluster.stopAllNodes();

                final TestNode node = cluster.startStaticNodeFromBackup();
                cluster.awaitLeader();

                assertEquals(10, node.service().messageCount());
                assertTrue(node.service().wasSnapshotLoaded());
            }
        });
    }

    @Test
    public void shouldBackupClusterWithSnapshotAndNonEmptyLog()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
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

                cluster.startClusterBackupNode(true);

                cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
                cluster.awaitBackupLiveLogPosition(logPosition);

                cluster.stopAllNodes();

                final TestNode node = cluster.startStaticNodeFromBackup();
                cluster.awaitLeader();
                cluster.awaitMessageCountForService(node, totalMessageCount);

                assertEquals(totalMessageCount, node.service().messageCount());
                assertTrue(node.service().wasSnapshotLoaded());
            }
        });
    }

    @Test
    public void shouldBackupClusterWithSnapshotThenSend()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
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

                cluster.startClusterBackupNode(true);

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
        });
    }

    @Test
    public void shouldBeAbleToGetTimeOfNextBackupQuery()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                cluster.awaitLeader();
                final TestBackupNode backupNode = cluster.startClusterBackupNode(true);

                cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);

                final long nowMs = backupNode.epochClock().time();

                assertThat(backupNode.nextBackupQueryDeadlineMs(), greaterThan(nowMs));
            }
        });
    }

    @Test
    public void shouldBackupClusterNoSnapshotsAndNonEmptyLogWithReQuery()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
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

                backupNode.nextBackupQueryDeadlineMs(0);

                cluster.sendMessages(5);
                cluster.awaitResponses(15);
                cluster.awaitMessageCountForService(leader, 15);

                final long nextLogPosition = leader.service().cluster().logPosition();

                cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
                cluster.awaitBackupLiveLogPosition(nextLogPosition);

                cluster.stopAllNodes();

                final TestNode node = cluster.startStaticNodeFromBackup();
                cluster.awaitLeader();

                assertEquals(15, node.service().messageCount());
            }
        });
    }

    @Test
    public void shouldBackupClusterNoSnapshotsAndNonEmptyLogAfterFailure()
    {
        assertTimeoutPreemptively(ofSeconds(20), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();

                cluster.connectClient();
                cluster.sendMessages(10);
                cluster.awaitResponses(10);
                cluster.awaitMessageCountForService(leader, 10);

                cluster.stopNode(leader);

                final TestNode nextLeader = cluster.awaitLeader();

                final long logPosition = nextLeader.service().cluster().logPosition();

                cluster.startClusterBackupNode(true);

                cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
                cluster.awaitBackupLiveLogPosition(logPosition);

                cluster.stopAllNodes();

                final TestNode node = cluster.startStaticNodeFromBackup();
                cluster.awaitLeader();

                assertEquals(10, node.service().messageCount());
                assertFalse(node.service().wasSnapshotLoaded());
            }
        });
    }

    @Test
    public void shouldBackupClusterNoSnapshotsAndNonEmptyLogWithFailure()
    {
        assertTimeoutPreemptively(ofSeconds(30), () ->
        {
            try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
            {
                final TestNode leader = cluster.awaitLeader();

                cluster.connectClient();
                cluster.sendMessages(10);
                cluster.awaitResponses(10);
                cluster.awaitMessageCountForService(leader, 10);

                final long logPosition = leader.service().cluster().logPosition();

                cluster.startClusterBackupNode(true);

                cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
                cluster.awaitBackupLiveLogPosition(logPosition);

                cluster.stopNode(leader);

                final TestNode nextLeader = cluster.awaitLeader();

                cluster.sendMessages(5);
                cluster.awaitResponses(15);
                cluster.awaitMessageCountForService(nextLeader, 15);

                final long nextLogPosition = nextLeader.service().cluster().logPosition();

                cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
                cluster.awaitBackupLiveLogPosition(nextLogPosition);

                cluster.stopAllNodes();

                final TestNode node = cluster.startStaticNodeFromBackup();
                cluster.awaitLeader();

                assertEquals(15, node.service().messageCount());
                assertFalse(node.service().wasSnapshotLoaded());
            }
        });
    }
}
