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

import io.aeron.log.EventLogExtension;
import io.aeron.test.*;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.aeron.test.cluster.TestCluster.aCluster;

@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
public class ClusterHotSnapshotTest
{
    @RegisterExtension
    public final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private TestCluster cluster = null;

    @Test
    @InterruptAfter(10)
    public void shouldStopFollowerAndRestartFollower()
    {
        cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

//        Tests.sleep(1_000);
        cluster.takeHotSnapshot(leader);
        //        cluster.awaitSnapshotCount(1);
        Tests.sleep(1_000);
    }
}
