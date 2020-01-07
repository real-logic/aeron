/*
 * Copyright 2014-2020 Real Logic Limited.
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
import static net.bytebuddy.matcher.ElementMatchers.nameEndsWith;
import static net.bytebuddy.matcher.ElementMatchers.named;

/**
 * A Java agent which when attached to a JVM will weave byte code to intercept events as defined by
 * {@link DriverEventCode}. Events are recorded to an in-memory {@link org.agrona.concurrent.ringbuffer.RingBuffer}
 * which is consumed and appended asynchronous to a log as defined by the class {@link #READER_CLASSNAME_PROP_NAME}
 * which defaults to {@link EventLogReaderAgent}.
 */
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
    private static ClassFileTransformer logTransformer;

    public static void premain(final String agentArgs, final Instrumentation instrumentation)
    {
        agent(AgentBuilder.RedefinitionStrategy.DISABLED, instrumentation);
    }

    public static void agentmain(final String agentArgs, final Instrumentation instrumentation)
    {
        agent(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION, instrumentation);
    }

    public static synchronized void removeTransformer()
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
                .or(nameEndsWith("ControlSessionDemuxer"))
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

    private static synchronized void agent(
        final AgentBuilder.RedefinitionStrategy redefinitionStrategy, final Instrumentation instrumentation)
    {
        if (null != logTransformer)
        {
            throw new IllegalStateException("agent already instrumenting");
        }

        if (0 == DriverEventLogger.ENABLED_EVENT_CODES &&
            0 == ArchiveEventLogger.ENABLED_EVENT_CODES &&
            0 == ClusterEventLogger.ENABLED_EVENT_CODES)
        {
            return;
        }

        EventLogAgent.instrumentation = instrumentation;

        readerAgentRunner = new AgentRunner(
            new SleepingMillisIdleStrategy(SLEEP_PERIOD_MS), Throwable::printStackTrace, null, getReaderAgent());

        AgentBuilder agentBuilder = new AgentBuilder.Default(new ByteBuddy()
            .with(TypeValidation.DISABLED))
            .disableClassFormatChanges()
            .with(new AgentBuilderListener())
            .with(redefinitionStrategy);

        agentBuilder = addDriverInstrumentation(agentBuilder);
        agentBuilder = addArchiveInstrumentation(agentBuilder);
        agentBuilder = addClusterInstrumentation(agentBuilder);

        logTransformer = agentBuilder.installOn(instrumentation);

        final Thread thread = new Thread(readerAgentRunner);
        thread.setName("event-log-reader");
        thread.setDaemon(true);
        thread.start();
    }

    private static AgentBuilder addDriverInstrumentation(final AgentBuilder agentBuilder)
    {
        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addDriverConductorInstrumentation(tempBuilder);
        tempBuilder = addDriverCommandInstrumentation(tempBuilder);
        tempBuilder = addDriverSenderProxyInstrumentation(tempBuilder);
        tempBuilder = addDriverReceiverProxyInstrumentation(tempBuilder);
        tempBuilder = addDriverUdpChannelTransportInstrumentation(tempBuilder);
        return tempBuilder;
    }

    private static AgentBuilder addDriverConductorInstrumentation(final AgentBuilder agentBuilder)
    {
        final boolean cleanupImage =
            DriverEventCode.isEnabled(DriverEventCode.REMOVE_IMAGE_CLEANUP, DriverEventLogger.ENABLED_EVENT_CODES);
        final boolean cleanupPublication =
            DriverEventCode
            .isEnabled(DriverEventCode.REMOVE_PUBLICATION_CLEANUP, DriverEventLogger.ENABLED_EVENT_CODES);
        final boolean cleanupSubscriptionLink =
            DriverEventCode
            .isEnabled(DriverEventCode.REMOVE_SUBSCRIPTION_CLEANUP, DriverEventLogger.ENABLED_EVENT_CODES);

        if (!cleanupImage && !cleanupPublication && !cleanupSubscriptionLink)
        {
            return agentBuilder;
        }

        return agentBuilder.type(nameEndsWith("DriverConductor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
            {
                if (cleanupImage)
                {
                    builder = builder.visit(to(CleanupInterceptor.CleanupImage.class)
                        .on(named("cleanupImage")));
                }
                if (cleanupPublication)
                {
                    builder = builder.visit(to(CleanupInterceptor.CleanupPublication.class)
                        .on(named("cleanupPublication")));
                }
                if (cleanupSubscriptionLink)
                {
                    builder = builder.visit(to(CleanupInterceptor.CleanupSubscriptionLink.class)
                        .on(named("cleanupSubscriptionLink")));
                }
                return builder;
            });
    }

    private static AgentBuilder addDriverCommandInstrumentation(final AgentBuilder agentBuilder)
    {
        if (CmdInterceptor.EVENTS.stream()
            .noneMatch(e -> DriverEventCode.isEnabled(e, DriverEventLogger.ENABLED_EVENT_CODES)))
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith("ClientCommandAdapter"))
            .transform((builder, typeDescription, classLoader, javaModule) -> builder
                .visit(to(CmdInterceptor.class)
                    .on(named("onMessage"))))
            .type(nameEndsWith("ClientProxy"))
            .transform((builder, typeDescription, classLoader, javaModule) -> builder
                .visit(to(CmdInterceptor.class)
                    .on(named("transmit"))));
    }

    private static AgentBuilder addDriverSenderProxyInstrumentation(final AgentBuilder agentBuilder)
    {
        final boolean registerChannel =
            DriverEventCode.isEnabled(DriverEventCode.SEND_CHANNEL_CREATION, DriverEventLogger.ENABLED_EVENT_CODES);
        final boolean closeChannel =
            DriverEventCode.isEnabled(DriverEventCode.SEND_CHANNEL_CLOSE, DriverEventLogger.ENABLED_EVENT_CODES);

        if (!registerChannel && !closeChannel)
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith("SenderProxy"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
            {
                if (registerChannel)
                {
                    builder = builder
                        .visit(to(ChannelEndpointInterceptor.SenderProxy.RegisterSendChannelEndpoint.class)
                            .on(named("registerSendChannelEndpoint")));
                }
                if (closeChannel)
                {
                    builder = builder
                        .visit(to(ChannelEndpointInterceptor.SenderProxy.CloseSendChannelEndpoint.class)
                            .on(named("closeSendChannelEndpoint")));
                }
                return builder;
            });
    }

    private static AgentBuilder addDriverReceiverProxyInstrumentation(final AgentBuilder agentBuilder)
    {
        final boolean registerChannel =
            DriverEventCode.isEnabled(DriverEventCode.RECEIVE_CHANNEL_CREATION, DriverEventLogger.ENABLED_EVENT_CODES);
        final boolean closeChannel =
            DriverEventCode.isEnabled(DriverEventCode.RECEIVE_CHANNEL_CLOSE, DriverEventLogger.ENABLED_EVENT_CODES);

        if (!registerChannel && !closeChannel)
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith("ReceiverProxy"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
            {
                if (registerChannel)
                {
                    builder = builder
                        .visit(to(ChannelEndpointInterceptor.ReceiverProxy.RegisterReceiveChannelEndpoint.class)
                            .on(named("registerReceiveChannelEndpoint")));
                }
                if (closeChannel)
                {
                    builder = builder
                        .visit(to(ChannelEndpointInterceptor.ReceiverProxy.CloseReceiveChannelEndpoint.class)
                            .on(named("closeReceiveChannelEndpoint")));
                }
                return builder;
            });
    }

    private static AgentBuilder addDriverUdpChannelTransportInstrumentation(final AgentBuilder agentBuilder)
    {
        final boolean frameOut =
            DriverEventCode.isEnabled(DriverEventCode.FRAME_OUT, DriverEventLogger.ENABLED_EVENT_CODES);
        final boolean frameIn =
            DriverEventCode.isEnabled(DriverEventCode.FRAME_IN, DriverEventLogger.ENABLED_EVENT_CODES);

        if (!frameOut && !frameIn)
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith("UdpChannelTransport"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
            {
                if (frameOut)
                {
                    builder = builder
                        .visit(to(ChannelEndpointInterceptor.UdpChannelTransport.SendHook.class)
                            .on(named("sendHook")));
                }
                if (frameIn)
                {
                    builder = builder
                        .visit(to(ChannelEndpointInterceptor.UdpChannelTransport.ReceiveHook.class)
                            .on(named("receiveHook")));
                }
                return builder;
            });
    }

    private static AgentBuilder addArchiveInstrumentation(final AgentBuilder agentBuilder)
    {
        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addArchiveControlSessionDemuxerInstrumentation(tempBuilder);
        tempBuilder = addArchiveControlResponseProxyInstrumentation(tempBuilder);
        return tempBuilder;
    }

    private static AgentBuilder addArchiveControlSessionDemuxerInstrumentation(final AgentBuilder agentBuilder)
    {
        if (ArchiveEventLogger.CONTROL_REQUEST_EVENTS.stream()
            .noneMatch(e -> ArchiveEventCode.isEnabled(e, ArchiveEventLogger.ENABLED_EVENT_CODES)))
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith("ControlSessionDemuxer"))
            .transform(((builder, typeDescription, classLoader, module) -> builder
                .visit(to(ControlInterceptor.ControlRequest.class)
                    .on(named("onFragment")))));
    }

    private static AgentBuilder addArchiveControlResponseProxyInstrumentation(final AgentBuilder agentBuilder)
    {
        if (!ArchiveEventCode.isEnabled(ArchiveEventCode.CMD_OUT_RESPONSE, ArchiveEventLogger.ENABLED_EVENT_CODES))
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith("ControlResponseProxy"))
            .transform(((builder, typeDescription, classLoader, module) -> builder
                .visit(to(ControlInterceptor.ControlResponse.class)
                    .on(named("sendResponse")))));
    }

    private static AgentBuilder addClusterInstrumentation(final AgentBuilder agentBuilder)
    {
        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addClusterElectionInstrumentation(tempBuilder);
        tempBuilder = addClusterConsensusModuleAgentInstrumentation(tempBuilder);
        return tempBuilder;
    }

    private static AgentBuilder addClusterElectionInstrumentation(final AgentBuilder agentBuilder)
    {
        if (!ClusterEventCode.isEnabled(ClusterEventCode.ELECTION_STATE_CHANGE, ClusterEventLogger.ENABLED_EVENT_CODES))
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith("Election"))
            .transform(((builder, typeDescription, classLoader, module) -> builder
                .visit(to(ClusterInterceptor.ElectionStateChange.class)
                    .on(named("stateChange")))));
    }

    private static AgentBuilder addClusterConsensusModuleAgentInstrumentation(final AgentBuilder agentBuilder)
    {
        final boolean newLeadershipTerm = ClusterEventCode
            .isEnabled(ClusterEventCode.NEW_LEADERSHIP_TERM, ClusterEventLogger.ENABLED_EVENT_CODES);
        final boolean stateChange = ClusterEventCode
            .isEnabled(ClusterEventCode.STATE_CHANGE, ClusterEventLogger.ENABLED_EVENT_CODES);
        final boolean roleChange = ClusterEventCode
            .isEnabled(ClusterEventCode.ROLE_CHANGE, ClusterEventLogger.ENABLED_EVENT_CODES);

        if (!newLeadershipTerm && !stateChange && !roleChange)
        {
            return agentBuilder;
        }

        return agentBuilder.type(nameEndsWith("ConsensusModuleAgent"))
            .transform(((builder, typeDescription, classLoader, module) ->
            {
                if (newLeadershipTerm)
                {
                    builder = builder
                        .visit(to(ClusterInterceptor.NewLeadershipTerm.class)
                            .on(named("onNewLeadershipTerm")));
                }
                if (stateChange)
                {
                    builder = builder
                        .visit(to(ClusterInterceptor.ConsensusModuleStateChange.class)
                            .on(named("stateChange")));
                }
                if (roleChange)
                {
                    builder = builder
                        .visit(to(ClusterInterceptor.ConsensusModuleRoleChange.class)
                            .on(named("roleChange")));
                }
                return builder;
            }));
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

class AgentBuilderListener implements AgentBuilder.Listener
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
        throwable.printStackTrace(System.err);
    }

    public void onComplete(
        final String typeName,
        final ClassLoader classLoader,
        final JavaModule module,
        final boolean loaded)
    {
    }
}
