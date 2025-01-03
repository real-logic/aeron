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

import io.aeron.test.*;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import net.bytebuddy.asm.Advice;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.concurrent.Exchanger;

import static io.aeron.test.cluster.TestCluster.aCluster;

@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class RacingCatchupClusterTest
{
    static final Exchanger<String> EXCHANGER = new Exchanger<>();
    private static ClusterInstrumentor clusterInstrumentor;

    @BeforeAll
    static void beforeAll()
    {
        clusterInstrumentor = new ClusterInstrumentor(
            StallFollowerCatchupIntercept.class, "Election", "followerCatchupInit");
    }

    @AfterAll
    static void afterAll()
    {
        clusterInstrumentor.reset();
    }

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    static void tagAndWaitForTag()
    {
        try
        {
            EXCHANGER.exchange("a");
            EXCHANGER.exchange("a");
        }
        catch (final InterruptedException ignore)
        {
        }
    }

    static void tag()
    {
        try
        {
            EXCHANGER.exchange("a");
        }
        catch (final InterruptedException ignore)
        {
        }
    }

    public static class StallFollowerCatchupIntercept
    {
        static boolean shouldStall = true;

        @Advice.OnMethodEnter
        static void followerCatchupInit(final long nowNs)
        {
            tagAndWaitForTag();
        }
    }

    @Test
    @InterruptAfter(40)
    @Disabled
    void shouldCatchupIfLogPositionMovesForwardBeforeFollowersCommitPositionWhenCatchingUpNodeIsOnlyFollower()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();

        systemTestWatcher.cluster(cluster);

        final int messageCount = 10;
        int totalMessages = 0;
        final TestNode oldLeader = cluster.awaitLeader();

        cluster.connectClient();
        cluster.sendMessages(messageCount);
        totalMessages += messageCount;
        cluster.awaitResponseMessageCount(totalMessages);
        cluster.awaitServicesMessageCount(totalMessages);

        cluster.stopNode(oldLeader);

        cluster.awaitLeader();
        final List<TestNode> followers = cluster.followers();
        cluster.connectClient();

        for (final TestNode follower : followers)
        {
            cluster.stopNode(follower);
        }
        Tests.sleep(1);

        cluster.sendMessages(messageCount);
        totalMessages += messageCount;

        cluster.startStaticNode(followers.get(0).index(), false);

        tag();

        cluster.sendMessages(messageCount);
        totalMessages += messageCount;

        tag();

        cluster.awaitResponseMessageCount(totalMessages);
    }
}
