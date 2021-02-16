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

import io.aeron.cluster.client.AeronCluster;
import io.aeron.test.SlowTest;
import io.aeron.test.TimeoutTestWatcher;
import io.aeron.test.cluster.TestBackupNode;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.aeron.Aeron.NULL_VALUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.*;

@SlowTest
public class ClusterBackupTest
{
    @RegisterExtension
    final TimeoutTestWatcher timeoutTestWatcher = new TimeoutTestWatcher();

    @BeforeEach
    void setUp()
    {
        timeoutTestWatcher.monitorTestThread(Thread.currentThread());
    }

    @Test
    @Timeout(30)
    public void shouldBackupClusterNoSnapshotsAndEmptyLog()
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
    }

    @Test
    @Timeout(30)
    public void shouldBackupClusterNoSnapshotsAndNonEmptyLog()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.connectClient();
            final int messageCount = 10;
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);
            cluster.awaitServicesMessageCount(messageCount);

            final long logPosition = leader.service().cluster().logPosition();

            cluster.startClusterBackupNode(true);

            cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
            cluster.awaitBackupLiveLogPosition(logPosition);
            cluster.stopAllNodes();

            final TestNode node = cluster.startStaticNodeFromBackup();
            cluster.awaitLeader();

            assertEquals(messageCount, node.service().messageCount());
            assertFalse(node.service().wasSnapshotLoaded());
        }
    }

    @Test
    @Timeout(30)
    public void shouldBackupClusterNoSnapshotsAndThenSendMessages()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();
            cluster.startClusterBackupNode(true);

            cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);

            cluster.connectClient();
            final int messageCount = 10;
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);
            cluster.awaitServicesMessageCount(messageCount);

            final long logPosition = leader.service().cluster().logPosition();

            cluster.awaitBackupLiveLogPosition(logPosition);
            cluster.stopAllNodes();

            final TestNode node = cluster.startStaticNodeFromBackup();
            cluster.awaitLeader();

            assertEquals(messageCount, node.service().messageCount());
            assertFalse(node.service().wasSnapshotLoaded());
        }
    }

    @Test
    @Timeout(30)
    public void shouldBackupClusterWithSnapshot()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.connectClient();
            final int messageCount = 10;
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);
            cluster.awaitServicesMessageCount(messageCount);

            cluster.takeSnapshot(leader);
            cluster.awaitSnapshotCount(1);

            final long logPosition = leader.service().cluster().logPosition();

            cluster.startClusterBackupNode(true);

            cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
            cluster.awaitBackupLiveLogPosition(logPosition);
            cluster.stopAllNodes();

            final TestNode node = cluster.startStaticNodeFromBackup();
            cluster.awaitLeader();

            assertEquals(messageCount, node.service().messageCount());
            assertTrue(node.service().wasSnapshotLoaded());
        }
    }

    @Test
    @Timeout(30)
    public void shouldBackupClusterAfterCleanShutdown()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            final int messageCount = 10;
            cluster.connectClient();
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);
            cluster.awaitServicesMessageCount(messageCount);

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
            final TestNode newLeader = cluster.awaitLeader();
            final long logPosition = newLeader.service().cluster().logPosition();

            cluster.startClusterBackupNode(true);

            cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
            cluster.awaitBackupLiveLogPosition(logPosition);
        }
    }

    @Test
    @Timeout(30)
    public void shouldBackupClusterWithSnapshotAndNonEmptyLog()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.connectClient();
            final int preSnapshotMessageCount = 10;
            final int postSnapshotMessageCount = 7;
            final int totalMessageCount = preSnapshotMessageCount + postSnapshotMessageCount;
            cluster.sendMessages(preSnapshotMessageCount);
            cluster.awaitResponseMessageCount(preSnapshotMessageCount);
            cluster.awaitServicesMessageCount(preSnapshotMessageCount);

            cluster.takeSnapshot(leader);
            cluster.awaitSnapshotCount(1);

            cluster.sendMessages(postSnapshotMessageCount);
            cluster.awaitResponseMessageCount(totalMessageCount);
            cluster.awaitServiceMessageCount(leader, totalMessageCount);

            final long logPosition = leader.service().cluster().logPosition();

            cluster.startClusterBackupNode(true);

            cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
            cluster.awaitBackupLiveLogPosition(logPosition);
            cluster.stopAllNodes();

            final TestNode node = cluster.startStaticNodeFromBackup();
            cluster.awaitLeader();
            cluster.awaitServiceMessageCount(node, totalMessageCount);

            assertEquals(totalMessageCount, node.service().messageCount());
            assertTrue(node.service().wasSnapshotLoaded());
        }
    }

    @Test
    @Timeout(30)
    public void shouldBackupClusterWithSnapshotThenSend()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.connectClient();
            final int preSnapshotMessageCount = 10;
            final int postSnapshotMessageCount = 7;
            final int totalMessageCount = preSnapshotMessageCount + postSnapshotMessageCount;
            cluster.sendMessages(preSnapshotMessageCount);
            cluster.awaitResponseMessageCount(preSnapshotMessageCount);
            cluster.awaitServicesMessageCount(preSnapshotMessageCount);

            cluster.takeSnapshot(leader);
            cluster.awaitSnapshotCount(1);

            cluster.startClusterBackupNode(true);

            cluster.sendMessages(postSnapshotMessageCount);
            cluster.awaitResponseMessageCount(totalMessageCount);
            cluster.awaitServiceMessageCount(leader, totalMessageCount);

            final long logPosition = leader.service().cluster().logPosition();

            cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
            cluster.awaitBackupLiveLogPosition(logPosition);
            cluster.stopAllNodes();

            final TestNode node = cluster.startStaticNodeFromBackup();
            cluster.awaitLeader();
            cluster.awaitServiceMessageCount(node, totalMessageCount);

            assertEquals(totalMessageCount, node.service().messageCount());
            assertTrue(node.service().wasSnapshotLoaded());
        }
    }

    @Test
    @Timeout(30)
    public void shouldBeAbleToGetTimeOfNextBackupQuery()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            cluster.awaitLeader();
            final TestBackupNode backupNode = cluster.startClusterBackupNode(true);

            cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);

            final long nowMs = backupNode.epochClock().time();
            assertThat(backupNode.nextBackupQueryDeadlineMs(), greaterThan(nowMs));
        }
    }

    @Test
    @Timeout(30)
    public void shouldBackupClusterNoSnapshotsAndNonEmptyLogWithReQuery()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.connectClient();
            final int messageCount = 10;
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);
            cluster.awaitServicesMessageCount(messageCount);

            final long logPosition = leader.service().cluster().logPosition();

            final TestBackupNode backupNode = cluster.startClusterBackupNode(true);

            cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
            cluster.awaitBackupLiveLogPosition(logPosition);

            assertTrue(backupNode.nextBackupQueryDeadlineMs(0));

            cluster.sendMessages(5);
            cluster.awaitResponseMessageCount(messageCount + 5);
            cluster.awaitServiceMessageCount(leader, messageCount + 5);

            final long nextLogPosition = leader.service().cluster().logPosition();
            cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
            cluster.awaitBackupLiveLogPosition(nextLogPosition);
            cluster.stopAllNodes();

            final TestNode node = cluster.startStaticNodeFromBackup();
            cluster.awaitLeader();

            assertEquals(messageCount + 5, node.service().messageCount());
        }
    }

    @Test
    @Timeout(40)
    public void shouldBackupClusterNoSnapshotsAndNonEmptyLogAfterFailure()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            cluster.connectClient();
            final int messageCount = 10;
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);
            cluster.awaitServicesMessageCount(messageCount);

            cluster.stopNode(leader);

            final TestNode nextLeader = cluster.awaitLeader();
            final long logPosition = nextLeader.service().cluster().logPosition();

            cluster.startClusterBackupNode(true);
            cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
            cluster.awaitBackupLiveLogPosition(logPosition);
            cluster.stopAllNodes();

            final TestNode node = cluster.startStaticNodeFromBackup();
            cluster.awaitLeader();

            assertEquals(messageCount, node.service().messageCount());
            assertFalse(node.service().wasSnapshotLoaded());
        }
    }

    @Test
    @Timeout(60)
    public void shouldBackupClusterNoSnapshotsAndNonEmptyLogWithFailure()
    {
        try (TestCluster cluster = TestCluster.startThreeNodeStaticCluster(NULL_VALUE))
        {
            final TestNode leader = cluster.awaitLeader();

            final AeronCluster aeronCluster = cluster.connectClient();
            final int messageCount = 10;
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount);
            cluster.awaitServicesMessageCount(messageCount);

            final long logPosition = leader.service().cluster().logPosition();

            cluster.startClusterBackupNode(true);

            aeronCluster.sendKeepAlive();
            cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
            cluster.awaitBackupLiveLogPosition(logPosition);
            cluster.stopNode(leader);

            final TestNode nextLeader = cluster.awaitLeader();
            cluster.awaitNewLeadershipEvent(1);

            cluster.sendMessages(5);
            cluster.awaitResponseMessageCount(messageCount + 5);

            final long nextLogPosition = nextLeader.service().cluster().logPosition();

            cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
            cluster.awaitBackupLiveLogPosition(nextLogPosition);
            cluster.stopAllNodes();

            final TestNode node = cluster.startStaticNodeFromBackup();
            cluster.awaitLeader();

            assertEquals(messageCount + 5, node.service().messageCount());
            assertFalse(node.service().wasSnapshotLoaded());
        }
    }
}
