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
package uk.co.real_logic.aeron.agent;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.SuperMethodCall;
import net.bytebuddy.matcher.BooleanMatcher;
import uk.co.real_logic.aeron.driver.EventLog;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SleepingIdleStrategy;

import java.lang.instrument.Instrumentation;

import static net.bytebuddy.matcher.ElementMatchers.*;

public class EventLogAgent
{
    private static final EventLogReaderAgent EVENT_LOG_READER_AGENT = new EventLogReaderAgent();

    private static final AgentRunner EVENT_LOG_READER_AGENT_RUNNER =
        new AgentRunner(new SleepingIdleStrategy(1), EventLogAgent::errorHandler, null, EVENT_LOG_READER_AGENT);

    private static final Thread EVENT_LOG_READER_THREAD = new Thread(EVENT_LOG_READER_AGENT_RUNNER);

    private static final AgentBuilder.Listener LISTENER = new AgentBuilder.Listener()
    {
        public void onTransformation(final TypeDescription typeDescription, final DynamicType dynamicType)
        {
            System.out.format("TRANSFORM %s\n", typeDescription.getName());
        }

        public void onIgnored(final TypeDescription typeDescription)
        {
        }

        public void onError(final String typeName, final Throwable throwable)
        {
            System.out.format("ERROR %s\n", typeName);
            throwable.printStackTrace(System.out);
        }

        public void onComplete(final String typeName)
        {
        }
    };

    private static void errorHandler(final Throwable throwable)
    {
    }

    public static void premain(final String agentArgs, final Instrumentation instrumentation)
    {
        if (EventConfiguration.ENABLED_EVENT_CODES != 0)
        {
            /*
             * Intercept based on enabled events:
             *  SenderProxy
             *  ReceiverProxy
             *  ClientProxy
             *  DriverConductor (onClientCommand)
             *  SendChannelEndpoint
             *  ReceiveChannelEndpoint
             */

            new AgentBuilder.Default()
                .with(LISTENER)
                .type(nameEndsWith("DriverConductor"))
                .transform(
                    (builder, typeDescription, classLoader) ->
                        builder
                            .method(named("onClientCommand").and(new BooleanMatcher<>(true)))
                            .intercept(MethodDelegation.to(CmdInterceptor.class)
                                .andThen(SuperMethodCall.INSTANCE))
                            .method(named("cleanupImage"))
                            .intercept(MethodDelegation.to(CleanupInterceptor.DriverConductorInterceptor.class)
                                .andThen(SuperMethodCall.INSTANCE))
                            .method(named("cleanupPublication"))
                            .intercept(MethodDelegation.to(CleanupInterceptor.DriverConductorInterceptor.class)
                                .andThen(SuperMethodCall.INSTANCE))
                            .method(named("cleanupSubscriptionLink"))
                            .intercept(MethodDelegation.to(CleanupInterceptor.DriverConductorInterceptor.class)
                                .andThen(SuperMethodCall.INSTANCE)))
                .type(nameEndsWith("ClientProxy"))
                .transform(
                    (builder, typeDescription, classLoader) ->
                        builder
                            .method(named("transmit"))
                            .intercept(MethodDelegation.to(CmdInterceptor.class)
                                .andThen(SuperMethodCall.INSTANCE)))
                .type(nameEndsWith("SenderProxy"))
                .transform(
                    (builder, typeDescription, classLoader) ->
                        builder
                            .method(named("registerSendChannelEndpoint"))
                            .intercept(MethodDelegation.to(ChannelEndpointInterceptor.SenderProxyInterceptor.class)
                                .andThen(SuperMethodCall.INSTANCE))
                            .method(named("closeSendChannelEndpoint"))
                            .intercept(MethodDelegation.to(ChannelEndpointInterceptor.SenderProxyInterceptor.class)
                                .andThen(SuperMethodCall.INSTANCE)))
                .type(nameEndsWith("ReceiverProxy"))
                .transform(
                    (builder, typeDescription, classLoader) ->
                        builder
                            .method(named("registerReceiveChannelEndpoint"))
                            .intercept(MethodDelegation.to(ChannelEndpointInterceptor.ReceiverProxyInterceptor.class)
                                .andThen(SuperMethodCall.INSTANCE))
                            .method(named("closeReceiveChannelEndpoint"))
                            .intercept(MethodDelegation.to(ChannelEndpointInterceptor.ReceiverProxyInterceptor.class)
                                .andThen(SuperMethodCall.INSTANCE)))
                .type(inheritsAnnotation(EventLog.class))
                .transform(
                    (builder, typeDescription, classLoader) ->
                        builder
                            .method(named("presend"))
                            .intercept(MethodDelegation.to(ChannelEndpointInterceptor.SendChannelEndpointInterceptor.class)
                                .andThen(SuperMethodCall.INSTANCE))
                            .method(named("sendTo"))
                            .intercept(MethodDelegation.to(ChannelEndpointInterceptor.ReceiveChannelEndpointInterceptor.class)
                                .andThen(SuperMethodCall.INSTANCE))
                            .method(named("dispatch"))
                            .intercept(MethodDelegation.to(ChannelEndpointInterceptor.ReceiveChannelEndpointInterceptor.class)
                                .andThen(SuperMethodCall.INSTANCE)))
                .installOn(instrumentation);

            EVENT_LOG_READER_THREAD.setName("event log reader");
            EVENT_LOG_READER_THREAD.setDaemon(true);
            EVENT_LOG_READER_THREAD.start();
        }
    }

    public static void agentmain(final String agentArgs, final Instrumentation instrumentation)
    {
    }
}
