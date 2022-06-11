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

import io.aeron.cluster.client.AeronCluster;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestBackupNode;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.*;

@SlowTest
@ExtendWith(InterruptingTestCallback.class)
public class ClusterBackupTest
{
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
    public void shouldBackupClusterNoSnapshotsAndEmptyLog()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

    @Test
    @InterruptAfter(30)
    public void shouldBackupClusterNoSnapshotsAndNonEmptyLog()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
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

    @Test
    @InterruptAfter(30)
    public void shouldBackupClusterNoSnapshotsAndThenSendMessages()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        cluster.startClusterBackupNode(true);

        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);

        final int messageCount = 10;
        cluster.connectClient();
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

    @Test
    @InterruptAfter(30)
    public void shouldBackupClusterWithSnapshot()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
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

    @Test
    @InterruptAfter(30)
    public void shouldBackupClusterAfterCleanShutdown()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

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

    @Test
    @InterruptAfter(30)
    public void shouldBackupClusterWithSnapshotAndNonEmptyLog()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int preSnapshotMessageCount = 10;
        final int postSnapshotMessageCount = 7;
        final int totalMessageCount = preSnapshotMessageCount + postSnapshotMessageCount;
        cluster.connectClient();
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

    @Test
    @InterruptAfter(30)
    public void shouldBackupClusterWithSnapshotThenSend()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int preSnapshotMessageCount = 10;
        final int postSnapshotMessageCount = 7;
        final int totalMessageCount = preSnapshotMessageCount + postSnapshotMessageCount;
        cluster.connectClient();
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

    @Test
    @InterruptAfter(30)
    public void shouldBeAbleToGetTimeOfNextBackupQuery()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        final TestBackupNode backupNode = cluster.startClusterBackupNode(true);

        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);

        final long nowMs = backupNode.epochClock().time();
        assertThat(backupNode.nextBackupQueryDeadlineMs(), greaterThan(nowMs));
    }

    @Test
    @InterruptAfter(30)
    public void shouldBackupClusterNoSnapshotsAndNonEmptyLogWithReQuery()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
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

    @Test
    @InterruptAfter(40)
    public void shouldBackupClusterNoSnapshotsAndNonEmptyLogAfterFailure()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leaderOne = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);

        cluster.stopNode(leaderOne);

        final TestNode leaderTwo = cluster.awaitLeader();
        final long logPosition = leaderTwo.service().cluster().logPosition();

        cluster.startClusterBackupNode(true);
        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
        cluster.awaitBackupLiveLogPosition(logPosition);
        cluster.stopAllNodes();

        final TestNode node = cluster.startStaticNodeFromBackup();
        cluster.awaitLeader();

        assertEquals(messageCount, node.service().messageCount());
        assertFalse(node.service().wasSnapshotLoaded());
    }

    @Test
    @InterruptAfter(60)
    public void shouldBackupClusterNoSnapshotsAndNonEmptyLogWithFailure()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leaderOne = cluster.awaitLeader();

        final int messageCount = 10;
        final AeronCluster aeronCluster = cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);

        final long logPosition = leaderOne.service().cluster().logPosition();

        cluster.startClusterBackupNode(true);

        aeronCluster.sendKeepAlive();
        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
        aeronCluster.sendKeepAlive();
        cluster.awaitBackupLiveLogPosition(logPosition);
        cluster.stopNode(leaderOne);

        final TestNode leaderTwo = cluster.awaitLeader();
        cluster.awaitNewLeadershipEvent(1);

        cluster.sendMessages(5);
        cluster.awaitResponseMessageCount(messageCount + 5);

        final long nextLogPosition = leaderTwo.service().cluster().logPosition();

        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
        cluster.awaitBackupLiveLogPosition(nextLogPosition);
        cluster.stopAllNodes();

        final TestNode node = cluster.startStaticNodeFromBackup();
        cluster.awaitLeader();

        assertEquals(messageCount + 5, node.service().messageCount());
        assertFalse(node.service().wasSnapshotLoaded());
    }
}
