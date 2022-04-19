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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.CountDownLatch;

import static io.aeron.test.cluster.TestCluster.aCluster;

@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
public class ClusterBackgroundSnapshotTest
{
    @RegisterExtension
    public final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(10)
    public void shouldTakeSnapshotWithoutServiceInterruption()
    {
        final CountDownLatch snapshotLatch = new CountDownLatch(1);

        final TestCluster cluster;
        final int messageCount1 = 10;
        final int messageCount2 = 1000;

        try
        {
            cluster = aCluster().withStaticNodes(3).start();
            systemTestWatcher.cluster(cluster);

            final TestNode leader = cluster.awaitLeader();

            cluster.connectClient();
            cluster.sendMessages(messageCount1);
            cluster.awaitResponseMessageCount(messageCount1);

            cluster.setSnapshotLatch(snapshotLatch);

            cluster.takeBackgroundSnapshot(leader);
            cluster.sendMessages(messageCount2);
            cluster.awaitResponseMessageCount(messageCount1 + messageCount2);
        }
        finally
        {
            snapshotLatch.countDown();
        }

        cluster.awaitSnapshotCount(1);
        cluster.awaitServicesMessageCount(messageCount1 + messageCount2);
    }

    @Test
    @InterruptAfter(10)
    public void shouldInterleaveMultipleSnapshotsWithMessages()
    {
        final TestCluster cluster;
        final int messageCount = 10;
        final int snapshotCount = 2;

        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        cluster.connectClient();

        for (int i = 0; i < snapshotCount; i++)
        {
            cluster.sendMessages(messageCount);
            cluster.awaitResponseMessageCount(messageCount * (i + 1));
            cluster.awaitNeutralControlToggle(leader);
            cluster.takeBackgroundSnapshot(leader);
        }

        cluster.awaitServicesMessageCount(snapshotCount * messageCount);
        cluster.awaitSnapshotCount(snapshotCount);
    }
}
