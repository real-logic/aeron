/*
 * Copyright 2016 Real Logic Ltd.
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

import io.aeron.driver.EventLog;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.TypeValidation;
import net.bytebuddy.utility.JavaModule;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SleepingIdleStrategy;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.util.concurrent.TimeUnit;

import static net.bytebuddy.asm.Advice.to;
import static net.bytebuddy.matcher.ElementMatchers.*;

public class EventLogAgent
{
    private static final long SLEEP_PERIOD_NS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final EventLogReaderAgent EVENT_LOG_READER_AGENT = new EventLogReaderAgent();

    private static final AgentRunner EVENT_LOG_READER_AGENT_RUNNER = new AgentRunner(
        new SleepingIdleStrategy(SLEEP_PERIOD_NS),
        EventLogAgent::errorHandler,
        null,
        EVENT_LOG_READER_AGENT);

    private static final Thread EVENT_LOG_READER_THREAD = new Thread(EVENT_LOG_READER_AGENT_RUNNER);

    private static volatile ClassFileTransformer logTransformer;
    private static volatile Instrumentation instrumentation;

    private static final AgentBuilder.Listener LISTENER = new AgentBuilder.Listener()
    {
        public void onTransformation(
            final TypeDescription typeDescription,
            final ClassLoader classLoader,
            final JavaModule module,
            final boolean loaded,
            final DynamicType dynamicType)
        {
            System.out.format("TRANSFORM %s%n", typeDescription.getName());
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
            System.out.format("ERROR %s%n", typeName);
            throwable.printStackTrace(System.out);
        }

        public void onComplete(
            final String typeName, final ClassLoader classLoader, final JavaModule module, final boolean loaded)
        {
        }
    };

    private static void errorHandler(final Throwable throwable)
    {
    }

    private static void agent(final boolean shouldRedefine, final Instrumentation instrumentation)
    {
        if (EventConfiguration.ENABLED_EVENT_CODES == 0)
        {
            return;
        }

        /*
         * Intercept based on enabled events:
         *  SenderProxy
         *  ReceiverProxy
         *  ClientProxy
         *  DriverConductor (onClientCommand)
         *  SendChannelEndpoint
         *  ReceiveChannelEndpoint
         */

        EventLogAgent.instrumentation = instrumentation;

        logTransformer = new AgentBuilder.Default(new ByteBuddy().with(TypeValidation.DISABLED))
            .with(LISTENER)
            .disableClassFormatChanges()
            .with(shouldRedefine ?
                AgentBuilder.RedefinitionStrategy.RETRANSFORMATION :
                AgentBuilder.RedefinitionStrategy.DISABLED)
            .type(nameEndsWith("DriverConductor"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                builder
                    .visit(to(CmdInterceptor.class).on(named("onClientCommand")))
                    .visit(to(CleanupInterceptor.DriverConductorInterceptor.CleanupImage.class)
                        .on(named("cleanupImage")))
                    .visit(to(CleanupInterceptor.DriverConductorInterceptor.CleanupPublication.class)
                        .on(named("cleanupPublication")))
                    .visit(to(CleanupInterceptor.DriverConductorInterceptor.CleanupSubscriptionLink.class)
                        .on(named("cleanupSubscriptionLink"))))
            .type(nameEndsWith("ClientProxy"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                builder.visit(to(CmdInterceptor.class).on(named("transmit"))))
            .type(nameEndsWith("SenderProxy"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                builder
                    .visit(to(ChannelEndpointInterceptor.SenderProxyInterceptor.RegisterSendChannelEndpoint.class)
                        .on(named("registerSendChannelEndpoint")))
                    .visit(to(ChannelEndpointInterceptor.SenderProxyInterceptor.CloseSendChannelEndpoint.class)
                        .on(named("closeSendChannelEndpoint"))))
            .type(nameEndsWith("ReceiverProxy"))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                builder
                    .visit(to(ChannelEndpointInterceptor.ReceiverProxyInterceptor.RegisterReceiveChannelEndpoint.class)
                        .on(named("registerReceiveChannelEndpoint")))
                    .visit(to(ChannelEndpointInterceptor.ReceiverProxyInterceptor.CloseReceiveChannelEndpoint.class)
                        .on(named("closeReceiveChannelEndpoint"))))
            .type(inheritsAnnotation(EventLog.class))
            .transform((builder, typeDescription, classLoader, javaModule) ->
                builder
                    .visit(to(ChannelEndpointInterceptor.SendChannelEndpointInterceptor.Presend.class)
                        .on(named("presend")))
                    .visit(to(ChannelEndpointInterceptor.ReceiveChannelEndpointInterceptor.SendTo.class)
                        .on(named("sendTo")))
                    .visit(to(ChannelEndpointInterceptor.SendChannelEndpointInterceptor.OnStatusMessage.class)
                        .on(named("onStatusMessage")))
                    .visit(to(ChannelEndpointInterceptor.SendChannelEndpointInterceptor.OnNakMessage.class)
                        .on(named("onNakMessage")))
                    .visit(to(ChannelEndpointInterceptor.SendChannelEndpointInterceptor.OnRttMeasurement.class)
                        .on(named("onRttMeasurement")))
                    .visit(to(ChannelEndpointInterceptor.ReceiveChannelEndpointInterceptor.OnDataPacket.class)
                        .on(named("onDataPacket")))
                    .visit(to(ChannelEndpointInterceptor.ReceiveChannelEndpointInterceptor.OnSetupMessage.class)
                        .on(named("onSetupMessage")))
                    .visit(to(ChannelEndpointInterceptor.ReceiveChannelEndpointInterceptor.OnRttMeasurement.class)
                        .on(named("onRttMeasurement"))))
            .installOn(instrumentation);

        EVENT_LOG_READER_THREAD.setName("event log reader");
        EVENT_LOG_READER_THREAD.setDaemon(true);
        EVENT_LOG_READER_THREAD.start();
    }

    public static void premain(final String agentArgs, final Instrumentation instrumentation)
    {
        agent(false, instrumentation);
    }

    public static void agentmain(final String agentArgs, final Instrumentation instrumentation)
    {
        agent(true, instrumentation);
    }

    public static void removeTransformer()
    {
        if (logTransformer != null)
        {
            instrumentation.removeTransformer(logTransformer);

            instrumentation.removeTransformer(new AgentBuilder.Default()
                .type(nameEndsWith("DriverConductor")
                    .or(nameEndsWith("ClientProxy"))
                    .or(nameEndsWith("SenderProxy"))
                    .or(nameEndsWith("ReceiverProxy"))
                    .or(inheritsAnnotation(EventLog.class)))
                .transform(AgentBuilder.Transformer.NoOp.INSTANCE)
                .installOn(instrumentation));
        }
    }
}
