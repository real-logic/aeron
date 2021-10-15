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

import io.aeron.cluster.client.ClusterException;
import io.aeron.log.EventLogExtension;
import io.aeron.test.*;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.dynamic.scaffold.TypeValidation;
import net.bytebuddy.matcher.ElementMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.lang.instrument.Instrumentation;

import static io.aeron.test.cluster.TestCluster.*;

@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
public class ClusterWithFailedFirstElectionTest
{
    private static ClusterInstrumentor clusterInstrumentor;

    @BeforeAll
    static void beforeAll()
    {
        clusterInstrumentor = new ClusterInstrumentor(FailFirstElectionIntercept.class, "Election", "onRequestVote");
    }

    @RegisterExtension
    public final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    public static class FailFirstElectionIntercept
    {
        @Advice.OnMethodEnter
        static void onRequestVote(
            final long logLeadershipTermId,
            final long logPosition,
            final long candidateTermId,
            final int candidateId,
            @Advice.This Object thisObject)
        {
            final Election election = (Election)thisObject;
            if (candidateId != election.memberId() && candidateTermId == 0)
            {
                throw new ClusterException(
                    "Forced failure: memberId=" + election.memberId() + ", candidateTermId=" + candidateTermId);
            }
        }
    }

    @Test
    @InterruptAfter(60)
    public void shouldRecoverWhenFollowerIsMultipleTermsBehindFromEmptyLog()
    {
        TestCluster cluster = aCluster().withStaticNodes(3).start(2);

        systemTestWatcher.cluster(cluster);
        systemTestWatcher.ignoreErrorsMatching((s) -> s.contains("Forced failure"));

        final int messageCount = 10;
        final int numTerms = 3;
        int totalMessage = 0;

        for (int i = 0; i < numTerms; i++)
        {
            final TestNode oldLeader = cluster.awaitLeader();

            cluster.connectClient();
            cluster.sendMessages(messageCount);
            totalMessage += messageCount;
            cluster.awaitResponseMessageCount(totalMessage);

            cluster.stopNode(oldLeader);
            cluster.startStaticNode(oldLeader.index(), false);
            cluster.awaitLeader();
        }

        cluster.startStaticNode(2, true);
        final TestNode lateJoiningNode = cluster.node(2);

        while (lateJoiningNode.service().messageCount() < totalMessage)
        {
            Tests.yieldingIdle("Waiting for late joining follower to catch up");
        }

        final TestNode testNode = cluster.awaitLeader();
    }
}
