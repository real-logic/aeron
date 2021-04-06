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
import java.util.EnumSet;

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

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            DRIVER_EVENT_CODES,
            DriverEventCode.UNTETHERED_SUBSCRIPTION_STATE_CHANGE,
            "UntetheredSubscription",
            DriverInterceptor.UntetheredSubscriptionStateChange.class,
            "stateChange");

        tempBuilder = addDriverNameResolutionInstrumentation(tempBuilder);

        tempBuilder = addDriverFlowControlInstrumentation(tempBuilder);

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
        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addEventInstrumentation(
            tempBuilder,
            DRIVER_EVENT_CODES,
            DriverEventCode.SEND_CHANNEL_CREATION,
            "SenderProxy",
            ChannelEndpointInterceptor.SenderProxy.RegisterSendChannelEndpoint.class,
            "registerSendChannelEndpoint");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            DRIVER_EVENT_CODES,
            DriverEventCode.SEND_CHANNEL_CLOSE,
            "SenderProxy",
            ChannelEndpointInterceptor.SenderProxy.CloseSendChannelEndpoint.class,
            "closeSendChannelEndpoint");

        return tempBuilder;
    }

    private static AgentBuilder addDriverReceiverProxyInstrumentation(final AgentBuilder agentBuilder)
    {
        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addEventInstrumentation(
            tempBuilder,
            DRIVER_EVENT_CODES,
            DriverEventCode.RECEIVE_CHANNEL_CREATION,
            "ReceiverProxy",
            ChannelEndpointInterceptor.ReceiverProxy.RegisterReceiveChannelEndpoint.class,
            "registerReceiveChannelEndpoint");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            DRIVER_EVENT_CODES,
            DriverEventCode.RECEIVE_CHANNEL_CLOSE,
            "ReceiverProxy",
            ChannelEndpointInterceptor.ReceiverProxy.CloseReceiveChannelEndpoint.class,
            "closeReceiveChannelEndpoint");

        return tempBuilder;
    }

    private static AgentBuilder addDriverUdpChannelTransportInstrumentation(final AgentBuilder agentBuilder)
    {
        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addEventInstrumentation(
            tempBuilder,
            DRIVER_EVENT_CODES,
            DriverEventCode.FRAME_OUT,
            "UdpChannelTransport",
            ChannelEndpointInterceptor.UdpChannelTransport.SendHook.class,
            "sendHook");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            DRIVER_EVENT_CODES,
            DriverEventCode.FRAME_IN,
            "UdpChannelTransport",
            ChannelEndpointInterceptor.UdpChannelTransport.ReceiveHook.class,
            "receiveHook");

        return tempBuilder;
    }

    private static AgentBuilder addDriverNameResolutionInstrumentation(final AgentBuilder agentBuilder)
    {
        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addEventInstrumentation(
            tempBuilder,
            DRIVER_EVENT_CODES,
            DriverEventCode.NAME_RESOLUTION_NEIGHBOR_ADDED,
            "Neighbor",
            DriverInterceptor.NameResolution.NeighborAdded.class,
            "neighborAdded");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            DRIVER_EVENT_CODES,
            DriverEventCode.NAME_RESOLUTION_NEIGHBOR_REMOVED,
            "Neighbor",
            DriverInterceptor.NameResolution.NeighborRemoved.class,
            "neighborRemoved");
        return tempBuilder;
    }

    private static AgentBuilder addDriverFlowControlInstrumentation(final AgentBuilder agentBuilder)
    {
        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addEventInstrumentation(
            tempBuilder,
            DRIVER_EVENT_CODES,
            DriverEventCode.FLOW_CONTROL_RECEIVER_ADDED,
            "AbstractMinMulticastFlowControl",
            DriverInterceptor.FlowControl.ReceiverAdded.class,
            "receiverAdded");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            DRIVER_EVENT_CODES,
            DriverEventCode.FLOW_CONTROL_RECEIVER_REMOVED,
            "AbstractMinMulticastFlowControl",
            DriverInterceptor.FlowControl.ReceiverRemoved.class,
            "receiverRemoved");
        return tempBuilder;
    }

    private static AgentBuilder addArchiveInstrumentation(final AgentBuilder agentBuilder)
    {
        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addArchiveControlSessionDemuxerInstrumentation(tempBuilder);

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            ARCHIVE_EVENT_CODES,
            ArchiveEventCode.CMD_OUT_RESPONSE,
            "ControlResponseProxy",
            ControlInterceptor.ControlResponse.class,
            "sendResponseHook");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            ARCHIVE_EVENT_CODES,
            ArchiveEventCode.REPLICATION_SESSION_STATE_CHANGE,
            "ReplicationSession",
            ArchiveInterceptor.ReplicationSessionStateChange.class,
            "stateChange");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            ARCHIVE_EVENT_CODES,
            ArchiveEventCode.CONTROL_SESSION_STATE_CHANGE,
            "ControlSession",
            ArchiveInterceptor.ControlSessionStateChange.class,
            "stateChange");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            ARCHIVE_EVENT_CODES,
            ArchiveEventCode.REPLAY_SESSION_ERROR,
            "ReplaySession",
            ArchiveInterceptor.ReplaySession.class,
            "onPendingError");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            ARCHIVE_EVENT_CODES,
            ArchiveEventCode.CATALOG_RESIZE,
            "Catalog",
            ArchiveInterceptor.Catalog.class,
            "catalogResized");

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

    private static AgentBuilder addClusterInstrumentation(final AgentBuilder agentBuilder)
    {
        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addEventInstrumentation(
            tempBuilder,
            CLUSTER_EVENT_CODES,
            ClusterEventCode.ELECTION_STATE_CHANGE,
            "Election",
            ClusterInterceptor.ElectionStateChange.class,
            "stateChange");

        tempBuilder = addClusterConsensusModuleAgentInstrumentation(tempBuilder);

        return tempBuilder;
    }

    private static AgentBuilder addClusterConsensusModuleAgentInstrumentation(final AgentBuilder agentBuilder)
    {
        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addEventInstrumentation(
            tempBuilder,
            CLUSTER_EVENT_CODES,
            ClusterEventCode.NEW_LEADERSHIP_TERM,
            "ConsensusModuleAgent",
            ClusterInterceptor.NewLeadershipTerm.class,
            "onNewLeadershipTerm");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            CLUSTER_EVENT_CODES,
            ClusterEventCode.STATE_CHANGE,
            "ConsensusModuleAgent",
            ClusterInterceptor.ConsensusModuleStateChange.class,
            "stateChange");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            CLUSTER_EVENT_CODES,
            ClusterEventCode.ROLE_CHANGE,
            "ConsensusModuleAgent",
            ClusterInterceptor.ConsensusModuleRoleChange.class,
            "roleChange");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            CLUSTER_EVENT_CODES,
            ClusterEventCode.CANVASS_POSITION,
            "ConsensusModuleAgent",
            ClusterInterceptor.CanvassPosition.class,
            "onCanvassPosition");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            CLUSTER_EVENT_CODES,
            ClusterEventCode.REQUEST_VOTE,
            "ConsensusModuleAgent",
            ClusterInterceptor.RequestVote.class,
            "onRequestVote");
        return tempBuilder;
    }

    private static <E extends Enum<E>> AgentBuilder addEventInstrumentation(
        final AgentBuilder agentBuilder,
        final EnumSet<E> enabledEvents,
        final E code,
        final String typeName,
        final Class<?> interceptorClass,
        final String interceptorMethod)
    {
        if (!enabledEvents.contains(code))
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith(typeName))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                builder.visit(to(interceptorClass).on(named(interceptorMethod))));
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
