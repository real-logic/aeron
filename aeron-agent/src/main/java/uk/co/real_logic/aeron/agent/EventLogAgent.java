/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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

import uk.co.real_logic.aeron.driver.event.EventConfiguration;
import uk.co.real_logic.agrona.concurrent.AgentRunner;
import uk.co.real_logic.agrona.concurrent.SleepingIdleStrategy;

import java.lang.instrument.Instrumentation;

public class EventLogAgent
{
    private static final EventLogReaderAgent EVENT_LOG_READER_AGENT = new EventLogReaderAgent();

    private static final AgentRunner EVENT_LOG_READER_AGENT_RUNNER =
        new AgentRunner(new SleepingIdleStrategy(1), EventLogAgent::errorHandler, null, EVENT_LOG_READER_AGENT);

    private static final Thread EVENT_LOG_READER_THREAD = new Thread(EVENT_LOG_READER_AGENT_RUNNER);

    public static void errorHandler(final Throwable throwable)
    {
    }

    public static void premain(final String agentArgs, final Instrumentation instrumentation)
    {
        if (EventConfiguration.ENABLED_EVENT_CODES != 0)
        {
            /*
             * Intercept based on enabled events:
             *  SenderProxy
             *  ClientProxy
             *  DriverCondcutor (onClientCommand)
             *  SendChannelEndpoint
             *  ReceiveChannelEndpoint
             */

            EVENT_LOG_READER_THREAD.setName("event log reader");
            EVENT_LOG_READER_THREAD.start();
        }
    }

    public static void agentmain(final String agentArgs, final Instrumentation instrumentation)
    {
    }
}
