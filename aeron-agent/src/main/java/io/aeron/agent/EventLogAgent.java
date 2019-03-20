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
package io.aeron.agent;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.TypeValidation;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.utility.JavaModule;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;

import static net.bytebuddy.asm.Advice.to;
import static net.bytebuddy.matcher.ElementMatchers.*;

/**
 * A Java agent which when attached to a JVM will weave byte code to intercept events as defined by
 * {@link DriverEventCode}. Events are recorded to an in-memory {@link org.agrona.concurrent.ringbuffer.RingBuffer}
 * which is consumed and appended asynchronous to a log as defined by the class {@link #READER_CLASSNAME_PROP_NAME}
 * which defaults to {@link EventLogReaderAgent}.
 */
@SuppressWarnings("unused")
public class EventLogAgent
{
    /**
     * Event reader {@link Agent} which consumes the {@link EventConfiguration#EVENT_RING_BUFFER} to output log events.
     */
    public static final String READER_CLASSNAME_PROP_NAME = "aeron.event.log.reader.classname";
    public static final String READER_CLASSNAME_DEFAULT = "io.aeron.agent.EventLogReaderAgent";

    private static final long SLEEP_PERIOD_MS = 1L;

    private static AgentRunner readerAgentRunner;
    private static Instrumentation instrumentation;
    private static volatile ClassFileTransformer logTransformer;

    static final AgentBuilder.Listener LISTENER = new AgentBuilder.Listener()
    {
        public void onDiscovery(
            final String typeName,
            final ClassLoader classLoader,
            final JavaModule module,
            final boolean loaded)
        {
        }

        public void onTransformation(
            final TypeDescription typeDescription,
            final ClassLoader classLoader,
            final JavaModule module,
            final boolean loaded,
            final DynamicType dynamicType)
        {
        }

        public void onIgnored(
            final TypeDescription typeDescription,
            final ClassLoader classLoader,
            final JavaModule module,
            final boolean loaded)
        {
        }

        public void onError(
            final String typeName,
            final ClassLoader classLoader,
            final JavaModule module,
            final boolean loaded,
            final Throwable throwable)
        {
            System.err.println("ERROR " + typeName);
            throwable.printStackTrace(System.out);
        }

        public void onComplete(
            final String typeName,
            final ClassLoader classLoader,
            final JavaModule module,
            final boolean loaded)
        {
        }
    };

    public static void premain(final String agentArgs, final Instrumentation instrumentation)
    {
        agent(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION, instrumentation);
    }

    public static void agentmain(final String agentArgs, final Instrumentation instrumentation)
    {
        agent(AgentBuilder.RedefinitionStrategy.DISABLED, instrumentation);
    }

    public static void removeTransformer()
    {
        if (logTransformer != null)
        {
            readerAgentRunner.close();
            instrumentation.removeTransformer(logTransformer);

            final ElementMatcher.Junction<TypeDescription> junction = nameEndsWith("DriverConductor")
                .or(nameEndsWith("ClientProxy"))
                .or(nameEndsWith("ClientCommandAdapter"))
                .or(nameEndsWith("SenderProxy"))
                .or(nameEndsWith("ReceiverProxy"))
                .or(nameEndsWith("UdpChannelTransport"))
                .or(nameEndsWith("ControlRequestAdapter"))
                .or(nameEndsWith("Election"))
                .or(nameEndsWith("ConsensusModuleAgent"));

            final ResettableClassFileTransformer transformer = new AgentBuilder.Default()
                .type(junction)
                .transform(AgentBuilder.Transformer.NoOp.INSTANCE)
                .installOn(instrumentation);

            instrumentation.removeTransformer(transformer);

            readerAgentRunner = null;
            instrumentation = null;
            logTransformer = null;
        }
    }

    private static void agent(
        final AgentBuilder.RedefinitionStrategy redefinitionStrategy, final Instrumentation instrumentation)
    {
        if (0 == DriverEventLogger.ENABLED_EVENT_CODES &&
            0 == ArchiveEventLogger.ENABLED_EVENT_CODES &&
            0 == ClusterEventLogger.ENABLED_EVENT_CODES)
        {
            return;
        }

        EventLogAgent.instrumentation = instrumentation;

        readerAgentRunner = new AgentRunner(
            new SleepingMillisIdleStrategy(SLEEP_PERIOD_MS), Throwable::printStackTrace, null, getReaderAgent());

        AgentBuilder agentBuilder = new AgentBuilder.Default(
            new ByteBuddy().with(TypeValidation.DISABLED))
            .disableClassFormatChanges()
            .with(LISTENER)
            .with(redefinitionStrategy);

        if (DriverEventLogger.ENABLED_EVENT_CODES != 0)
        {
            agentBuilder = addDriverInstrumentation(agentBuilder);
        }

        if (ArchiveEventLogger.ENABLED_EVENT_CODES != 0)
        {
            agentBuilder = addArchiveInstrumentation(agentBuilder);
        }

        if (ClusterEventLogger.ENABLED_EVENT_CODES != 0)
        {
            agentBuilder = addClusterInstrumentation(agentBuilder);
        }

        logTransformer = agentBuilder.installOn(instrumentation);

        final Thread thread = new Thread(readerAgentRunner);
        thread.setName("event-log-reader");
        thread.setDaemon(true);
        thread.start();
    }

    private static AgentBuilder addDriverInstrumentation(final AgentBuilder agentBuilder)
    {
        return agentBuilder
            .type(nameEndsWith("DriverConductor"))
            .transform((builder, typeDescription, classLoader, javaModule) -> builder
                .visit(to(CleanupInterceptor.CleanupImage.class).on(named("cleanupImage")))
                .visit(to(CleanupInterceptor.CleanupPublication.class).on(named("cleanupPublication")))
                .visit(to(CleanupInterceptor.CleanupSubscriptionLink.class).on(named("cleanupSubscriptionLink"))))
            .type(nameEndsWith("ClientCommandAdapter"))
            .transform((builder, typeDescription, classLoader, javaModule) -> builder
                .visit(to(CmdInterceptor.class).on(named("onMessage"))))
            .type(nameEndsWith("ClientProxy"))
            .transform((builder, typeDescription, classLoader, javaModule) -> builder
                .visit(to(CmdInterceptor.class).on(named("transmit"))))
            .type(nameEndsWith("SenderProxy"))
            .transform((builder, typeDescription, classLoader, javaModule) -> builder
                .visit(to(ChannelEndpointInterceptor.SenderProxyInterceptor.RegisterSendChannelEndpoint.class)
                    .on(named("registerSendChannelEndpoint")))
                .visit(to(ChannelEndpointInterceptor.SenderProxyInterceptor.CloseSendChannelEndpoint.class)
                    .on(named("closeSendChannelEndpoint"))))
            .type(nameEndsWith("ReceiverProxy"))
            .transform((builder, typeDescription, classLoader, javaModule) -> builder
                .visit(to(ChannelEndpointInterceptor.ReceiverProxyInterceptor.RegisterReceiveChannelEndpoint.class)
                    .on(named("registerReceiveChannelEndpoint")))
                .visit(to(ChannelEndpointInterceptor.ReceiverProxyInterceptor.CloseReceiveChannelEndpoint.class)
                    .on(named("closeReceiveChannelEndpoint"))))
            .type(nameEndsWith("UdpChannelTransport"))
            .transform((builder, typeDescription, classLoader, javaModule) -> builder
                .visit(to(ChannelEndpointInterceptor.UdpChannelTransportInterceptor.SendHook.class)
                    .on(named("sendHook")))
                .visit(to(ChannelEndpointInterceptor.UdpChannelTransportInterceptor.ReceiveHook.class)
                    .on(named("receiveHook"))));
    }

    private static AgentBuilder addArchiveInstrumentation(final AgentBuilder agentBuilder)
    {
        return agentBuilder
            .type(nameEndsWith("ControlRequestAdapter"))
            .transform(((builder, typeDescription, classLoader, module) -> builder
                .visit(to(ControlRequestInterceptor.ControlRequest.class).on(named("onFragment")))));
    }

    private static AgentBuilder addClusterInstrumentation(final AgentBuilder agentBuilder)
    {
        return agentBuilder
            .type(nameEndsWith("Election"))
            .transform(((builder, typeDescription, classLoader, module) -> builder
                .visit(to(ClusterEventInterceptor.ElectionStateChange.class).on(named("state")
                    .and(takesArgument(0, nameEndsWith("State")))))))
            .type(nameEndsWith("ConsensusModuleAgent"))
            .transform(((builder, typeDescription, classLoader, module) -> builder
                .visit(to(ClusterEventInterceptor.NewLeadershipTerm.class).on(named("onNewLeadershipTerm")))
                .visit(to(ClusterEventInterceptor.StateChange.class).on(named("state")))
                .visit(to(ClusterEventInterceptor.RoleChange.class).on(named("role")
                    .and(takesArgument(0, nameEndsWith("Role")))))));
    }

    private static Agent getReaderAgent()
    {
        try
        {
            final Class<?> aClass = Class.forName(
                System.getProperty(READER_CLASSNAME_PROP_NAME, READER_CLASSNAME_DEFAULT));

            return (Agent)aClass.getDeclaredConstructor().newInstance();
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
