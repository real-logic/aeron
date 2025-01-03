/*
 * Copyright 2014-2025 Real Logic Limited.
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

import io.aeron.cluster.client.ClusterException;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import net.bytebuddy.asm.Advice;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static io.aeron.test.cluster.TestCluster.awaitElectionClosed;

@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class RecoverAfterFailedCatchupClusterTest
{
    static ClusterInstrumentor clusterInstrumentor;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @BeforeAll
    static void beforeAll()
    {
        clusterInstrumentor = new ClusterInstrumentor(
            FailFirstFollowerCatchup.class, "Election", "state");
    }

    public static class FailFirstFollowerCatchup
    {
        static boolean shouldStall = true;

        @Advice.OnMethodEnter
        static void state(final ElectionState state, final long nowNs)
        {
            if (ElectionState.FOLLOWER_CATCHUP == state && shouldStall)
            {
                shouldStall = false;
                throw new ClusterException("For catchup failure");
            }
        }
    }

    @Test
    @InterruptAfter(30)
    void shouldCatchupFromEmptyLog()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);
        systemTestWatcher.ignoreErrorsMatching((s) -> s.contains("For catchup failure"));
        systemTestWatcher.ignoreErrorsMatching((s) -> s.contains("failed to join catchup log"));

        cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        TestNode follower = followers.get(1);

        awaitElectionClosed(follower);
        cluster.stopNode(follower);

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        follower = cluster.startStaticNode(follower.index(), true);
        cluster.awaitServiceMessageCount(follower, messageCount);
    }
}
