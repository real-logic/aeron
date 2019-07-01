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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
}
