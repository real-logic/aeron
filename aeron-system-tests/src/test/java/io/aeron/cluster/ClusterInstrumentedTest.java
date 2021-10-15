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

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterException;
import io.aeron.log.EventLogExtension;
import io.aeron.test.*;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.TypeValidation;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.utility.JavaModule;
import org.agrona.collections.Hashing;
import org.agrona.collections.MutableInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.lang.instrument.Instrumentation;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.zip.CRC32;

import static io.aeron.cluster.service.Cluster.Role.FOLLOWER;
import static io.aeron.cluster.service.Cluster.Role.LEADER;
import static io.aeron.logbuffer.FrameDescriptor.computeMaxMessageLength;
import static io.aeron.test.SystemTestWatcher.UNKNOWN_HOST_FILTER;
import static io.aeron.test.Tests.awaitAvailableWindow;
import static io.aeron.test.cluster.ClusterTests.*;
import static io.aeron.test.cluster.TestCluster.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.jupiter.api.Assertions.*;

@SlowTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
public class ClusterInstrumentedTest
{
    private static ResettableClassFileTransformer testTransformer;
    @RegisterExtension
    public final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private TestCluster cluster = null;

    @BeforeAll
    static void beforeAll()
    {
        final Instrumentation instrumentation = ByteBuddyAgent.install();
        System.out.println(instrumentation);

        final AgentBuilder agentBuilder = new AgentBuilder.Default(new ByteBuddy()
            .with(TypeValidation.DISABLED))
            .disableClassFormatChanges()
            .with(new AgentBuilderListener())
            .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION);

//        agentBuilder
//            .type(ElementMatchers.nameEndsWith("Election"))
//            .transform(
//                (builder, typeDescription, classLoader, module) ->
//                    builder.visit(Advice.to(VoteIntercept.class).on(ElementMatchers.named("onRequestVote"))))
//            .installOn(instrumentation);
    }

    public static class VoteIntercept
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
                System.err.println("Forcing failure");
                throw new ClusterException(
                    "Forced failure: memberId=" + election.memberId() + ", candidateTermId=" + candidateTermId);
            }
        }
    }

    @Test
    @InterruptAfter(60)
    public void shouldRecoverWhenFollowerIsMultipleTermsBehindFromEmptyLog()
    {
        cluster = aCluster().withStaticNodes(3).start(2);

        systemTestWatcher.cluster(cluster);

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

        ClusterTool.recordingLog(System.out, testNode.consensusModule().context().clusterDir());
        ClusterTool.recordingLog(System.out, lateJoiningNode.consensusModule().context().clusterDir());
    }

    private static class AgentBuilderListener implements AgentBuilder.Listener
    {
        public void onDiscovery(
            final String typeName, final ClassLoader classLoader, final JavaModule module, final boolean loaded)
        {
        }

        public void onTransformation(
            final TypeDescription typeDescription,
            final ClassLoader classLoader,
            final JavaModule module,
            final boolean loaded,
            final DynamicType dynamicType)
        {
            if (typeDescription.toString().contains("Election"))
            {
                System.out.println("[TRANSFORM] typeDescription = " + typeDescription + ", classLoader = " + classLoader + ", module = " + module + ", loaded = " + loaded + ", dynamicType = " + dynamicType);
            }
        }

        public void onIgnored(
            final TypeDescription typeDescription,
            final ClassLoader classLoader,
            final JavaModule module,
            final boolean loaded)
        {
            if (typeDescription.toString().contains("Election"))
            {
                System.out.println("[IGNORE] typeDescription = " + typeDescription + ", classLoader = " + classLoader + ", module = " + module + ", loaded = " + loaded);
            }
        }

        public void onError(
            final String typeName,
            final ClassLoader classLoader,
            final JavaModule module,
            final boolean loaded,
            final Throwable throwable)
        {
            throwable.printStackTrace();
        }

        public void onComplete(
            final String typeName,
            final ClassLoader classLoader,
            final JavaModule module,
            final boolean loaded)
        {
        }
    }
}
