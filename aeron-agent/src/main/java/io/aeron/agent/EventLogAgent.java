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
package io.aeron.agent;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.TypeValidation;
import net.bytebuddy.utility.JavaModule;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.lang.instrument.Instrumentation;

import static io.aeron.agent.EventConfiguration.*;
import static net.bytebuddy.asm.Advice.to;
import static net.bytebuddy.matcher.ElementMatchers.nameEndsWith;
import static net.bytebuddy.matcher.ElementMatchers.named;

/**
 * A Java agent which when attached to a JVM will weave byte code to intercept events as defined by
 * {@link DriverEventCode}. Events are recorded to an in-memory {@link org.agrona.concurrent.ringbuffer.RingBuffer}
 * which is consumed and appended asynchronous to a log as defined by the class {@link #READER_CLASSNAME_PROP_NAME}
 * which defaults to {@link EventLogReaderAgent}.
 */
public final class EventLogAgent
{
    /**
     * Event reader {@link Agent} which consumes the {@link EventConfiguration#EVENT_RING_BUFFER} to output log events.
     */
    public static final String READER_CLASSNAME_PROP_NAME = "aeron.event.log.reader.classname";
    public static final String READER_CLASSNAME_DEFAULT = "io.aeron.agent.EventLogReaderAgent";

    private static final long SLEEP_PERIOD_MS = 1L;

    private static AgentRunner readerAgentRunner;
    private static Instrumentation instrumentation;
    private static ResettableClassFileTransformer logTransformer;
    private static Thread thread;

    /**
     * Premain method to run before the main method of the application.
     *
     * @param agentArgs       which are ignored.
     * @param instrumentation for applying to the agent.
     */
    public static void premain(final String agentArgs, final Instrumentation instrumentation)
    {
        agent(AgentBuilder.RedefinitionStrategy.DISABLED, instrumentation);
    }

    /**
     * Agent main method for dynamic attach.
     *
     * @param agentArgs       which are ignored.
     * @param instrumentation for applying to the agent.
     */
    public static void agentmain(final String agentArgs, final Instrumentation instrumentation)
    {
        agent(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION, instrumentation);
    }

    /**
     * Remove the transformer and close the agent runner for the event log reader.
     */
    public static synchronized void removeTransformer()
    {
        if (logTransformer != null)
        {
            logTransformer.reset(instrumentation, AgentBuilder.RedefinitionStrategy.RETRANSFORMATION);
            instrumentation = null;
            logTransformer = null;
            thread = null;

            CloseHelper.close(readerAgentRunner);
            readerAgentRunner = null;
        }
    }

    private static synchronized void agent(
        final AgentBuilder.RedefinitionStrategy redefinitionStrategy, final Instrumentation instrumentation)
    {
        if (null != logTransformer)
        {
            throw new IllegalStateException("agent already instrumented");
        }

        EventConfiguration.init();

        if (DRIVER_EVENT_CODES.isEmpty() && ARCHIVE_EVENT_CODES.isEmpty() && CLUSTER_EVENT_CODES.isEmpty())
        {
            return;
        }

        EventLogAgent.instrumentation = instrumentation;

        readerAgentRunner = new AgentRunner(
            new SleepingMillisIdleStrategy(SLEEP_PERIOD_MS), Throwable::printStackTrace, null, newReaderAgent());

        AgentBuilder agentBuilder = new AgentBuilder.Default(new ByteBuddy()
            .with(TypeValidation.DISABLED))
            .disableClassFormatChanges()
            .with(new AgentBuilderListener())
            .with(redefinitionStrategy);

        agentBuilder = addDriverInstrumentation(agentBuilder);
        agentBuilder = addArchiveInstrumentation(agentBuilder);
        agentBuilder = addClusterInstrumentation(agentBuilder);

        logTransformer = agentBuilder.installOn(instrumentation);

        thread = new Thread(readerAgentRunner);
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
        tempBuilder = addDriverUntetheredSubscriptionInstrumentation(tempBuilder);
        tempBuilder = addDriverNameResolutionNeighbourChangeInstrumentation(
            tempBuilder, DriverEventCode.NAME_RESOLUTION_NEIGHBOR_ADDED, "neighborAdded");
        tempBuilder = addDriverNameResolutionNeighbourChangeInstrumentation(
            tempBuilder, DriverEventCode.NAME_RESOLUTION_NEIGHBOR_REMOVED, "neighborRemoved");

        return tempBuilder;
    }

    private static AgentBuilder addDriverConductorInstrumentation(final AgentBuilder agentBuilder)
    {
        final boolean hasImageHook = DRIVER_EVENT_CODES.contains(DriverEventCode.REMOVE_IMAGE_CLEANUP);
        final boolean hasPublicationHook = DRIVER_EVENT_CODES.contains(DriverEventCode.REMOVE_PUBLICATION_CLEANUP);
        final boolean hasSubscriptionHook = DRIVER_EVENT_CODES.contains(DriverEventCode.REMOVE_SUBSCRIPTION_CLEANUP);

        if (!hasImageHook && !hasPublicationHook && !hasSubscriptionHook)
        {
            return agentBuilder;
        }

        return agentBuilder.type(nameEndsWith("DriverConductor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
            {
                if (hasImageHook)
                {
                    builder = builder.visit(to(CleanupInterceptor.CleanupImage.class)
                        .on(named("cleanupImage")));
                }
                if (hasPublicationHook)
                {
                    builder = builder.visit(to(CleanupInterceptor.CleanupPublication.class)
                        .on(named("cleanupPublication")))
                        .visit(to(CleanupInterceptor.CleanupIpcPublication.class)
                        .on(named("cleanupIpcPublication")));
                }
                if (hasSubscriptionHook)
                {
                    builder = builder.visit(to(CleanupInterceptor.CleanupSubscriptionLink.class)
                        .on(named("cleanupSubscriptionLink")));
                }

                return builder;
            });
    }

    private static AgentBuilder addDriverCommandInstrumentation(final AgentBuilder agentBuilder)
    {
        if (CmdInterceptor.EVENTS.stream().noneMatch(DRIVER_EVENT_CODES::contains))
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
        final boolean hasChannelRegister = DRIVER_EVENT_CODES.contains(DriverEventCode.SEND_CHANNEL_CREATION);
        final boolean hasCloseChannel = DRIVER_EVENT_CODES.contains(DriverEventCode.SEND_CHANNEL_CLOSE);

        if (!hasChannelRegister && !hasCloseChannel)
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith("SenderProxy"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
            {
                if (hasChannelRegister)
                {
                    builder = builder
                        .visit(to(ChannelEndpointInterceptor.SenderProxy.RegisterSendChannelEndpoint.class)
                            .on(named("registerSendChannelEndpoint")));
                }
                if (hasCloseChannel)
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
        final boolean hasRegisterChannel = DRIVER_EVENT_CODES.contains(DriverEventCode.RECEIVE_CHANNEL_CREATION);
        final boolean hasCloseChannel = DRIVER_EVENT_CODES.contains(DriverEventCode.RECEIVE_CHANNEL_CLOSE);

        if (!hasRegisterChannel && !hasCloseChannel)
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith("ReceiverProxy"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
            {
                if (hasRegisterChannel)
                {
                    builder = builder
                        .visit(to(ChannelEndpointInterceptor.ReceiverProxy.RegisterReceiveChannelEndpoint.class)
                            .on(named("registerReceiveChannelEndpoint")));
                }
                if (hasCloseChannel)
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
        final boolean hasFrameOut = DRIVER_EVENT_CODES.contains(DriverEventCode.FRAME_OUT);
        final boolean hasFrameIn = DRIVER_EVENT_CODES.contains(DriverEventCode.FRAME_IN);

        if (!hasFrameOut && !hasFrameIn)
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith("UdpChannelTransport"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
            {
                if (hasFrameOut)
                {
                    builder = builder
                        .visit(to(ChannelEndpointInterceptor.UdpChannelTransport.SendHook.class)
                            .on(named("sendHook")));
                }
                if (hasFrameIn)
                {
                    builder = builder
                        .visit(to(ChannelEndpointInterceptor.UdpChannelTransport.ReceiveHook.class)
                            .on(named("receiveHook")));
                }

                return builder;
            });
    }

    private static AgentBuilder addDriverUntetheredSubscriptionInstrumentation(final AgentBuilder agentBuilder)
    {
        if (!DRIVER_EVENT_CODES.contains(DriverEventCode.UNTETHERED_SUBSCRIPTION_STATE_CHANGE))
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith("UntetheredSubscription"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                builder
                    .visit(to(DriverInterceptor.UntetheredSubscriptionStateChange.class)
                        .on(named("stateChange"))));
    }

    private static AgentBuilder addDriverNameResolutionNeighbourChangeInstrumentation(
        final AgentBuilder agentBuilder, final DriverEventCode code, final String methodName)
    {
        if (!DRIVER_EVENT_CODES.contains(code))
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith("Neighbor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                builder
                    .visit(to(DriverInterceptor.Neighbour.class)
                        .on(named(methodName))));
    }

    private static AgentBuilder addArchiveInstrumentation(final AgentBuilder agentBuilder)
    {
        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addArchiveControlSessionDemuxerInstrumentation(tempBuilder);
        tempBuilder = addArchiveControlResponseProxyInstrumentation(tempBuilder);
        tempBuilder = addArchiveReplicationSessionInstrumentation(tempBuilder);
        tempBuilder = addArchiveControlSessionInstrumentation(tempBuilder);
        tempBuilder = addArchiveReplaySessionInstrumentation(tempBuilder);
        tempBuilder = addArchiveCatalogInstrumentation(tempBuilder);

        return tempBuilder;
    }

    private static AgentBuilder addArchiveControlSessionDemuxerInstrumentation(final AgentBuilder agentBuilder)
    {
        if (ArchiveEventLogger.CONTROL_REQUEST_EVENTS.stream().noneMatch(ARCHIVE_EVENT_CODES::contains))
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
        if (!ARCHIVE_EVENT_CODES.contains(ArchiveEventCode.CMD_OUT_RESPONSE))
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith("ControlResponseProxy"))
            .transform(((builder, typeDescription, classLoader, module) -> builder
                .visit(to(ControlInterceptor.ControlResponse.class)
                    .on(named("sendResponseHook")))));
    }

    private static AgentBuilder addArchiveReplicationSessionInstrumentation(final AgentBuilder agentBuilder)
    {
        if (!ARCHIVE_EVENT_CODES.contains(ArchiveEventCode.REPLICATION_SESSION_STATE_CHANGE))
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith("ReplicationSession"))
            .transform(((builder, typeDescription, classLoader, module) -> builder
                .visit(to(ArchiveInterceptor.ReplicationSessionStateChange.class)
                    .on(named("stateChange")))));
    }

    private static AgentBuilder addArchiveControlSessionInstrumentation(final AgentBuilder agentBuilder)
    {
        if (!ARCHIVE_EVENT_CODES.contains(ArchiveEventCode.CONTROL_SESSION_STATE_CHANGE))
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith("ControlSession"))
            .transform(((builder, typeDescription, classLoader, module) -> builder
                .visit(to(ArchiveInterceptor.ControlSessionStateChange.class)
                    .on(named("stateChange")))));
    }

    private static AgentBuilder addArchiveReplaySessionInstrumentation(final AgentBuilder agentBuilder)
    {
        if (!ARCHIVE_EVENT_CODES.contains(ArchiveEventCode.REPLAY_SESSION_ERROR))
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith("ReplaySession"))
            .transform(((builder, typeDescription, classLoader, module) -> builder
                .visit(to(ArchiveInterceptor.ReplaySession.class)
                    .on(named("onPendingError")))));
    }

    private static AgentBuilder addArchiveCatalogInstrumentation(final AgentBuilder agentBuilder)
    {
        if (!ARCHIVE_EVENT_CODES.contains(ArchiveEventCode.CATALOG_RESIZE))
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith("Catalog"))
            .transform(((builder, typeDescription, classLoader, module) -> builder
                .visit(to(ArchiveInterceptor.Catalog.class)
                    .on(named("catalogResized")))));
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
        if (!CLUSTER_EVENT_CODES.contains(ClusterEventCode.ELECTION_STATE_CHANGE))
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
        final boolean hasNewLeadershipTerm = CLUSTER_EVENT_CODES.contains(ClusterEventCode.NEW_LEADERSHIP_TERM);
        final boolean hasStateChange = CLUSTER_EVENT_CODES.contains(ClusterEventCode.STATE_CHANGE);
        final boolean hasRoleChange = CLUSTER_EVENT_CODES.contains(ClusterEventCode.ROLE_CHANGE);

        if (!hasNewLeadershipTerm && !hasStateChange && !hasRoleChange)
        {
            return agentBuilder;
        }

        return agentBuilder.type(nameEndsWith("ConsensusModuleAgent"))
            .transform(((builder, typeDescription, classLoader, module) ->
            {
                if (hasNewLeadershipTerm)
                {
                    builder = builder
                        .visit(to(ClusterInterceptor.NewLeadershipTerm.class)
                            .on(named("onNewLeadershipTerm")));
                }
                if (hasStateChange)
                {
                    builder = builder
                        .visit(to(ClusterInterceptor.ConsensusModuleStateChange.class)
                            .on(named("stateChange")));
                }
                if (hasRoleChange)
                {
                    builder = builder
                        .visit(to(ClusterInterceptor.ConsensusModuleRoleChange.class)
                            .on(named("roleChange")));
                }

                return builder;
            }));
    }

    private static Agent newReaderAgent()
    {
        try
        {
            final String className = System.getProperty(READER_CLASSNAME_PROP_NAME, READER_CLASSNAME_DEFAULT);
            final Class<?> aClass = Class.forName(className);

            return (Agent)aClass.getDeclaredConstructor().newInstance();
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}

final class AgentBuilderListener implements AgentBuilder.Listener
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
