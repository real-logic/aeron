/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.examples;

import uk.co.real_logic.aeron.common.BackoffIdleStrategy;
import uk.co.real_logic.aeron.common.event.EventReader;

import java.util.concurrent.TimeUnit;

/**
 * Simple application for reading of event log
 */
public class EventReaderApp
{
    public static final long AGENT_IDLE_MAX_SPINS = 100;
    public static final long AGENT_IDLE_MAX_YIELDS = 100;
    public static final long AGENT_IDLE_MIN_PARK_NS = TimeUnit.NANOSECONDS.toNanos(10);
    public static final long AGENT_IDLE_MAX_PARK_NS = TimeUnit.MICROSECONDS.toNanos(100);

    public static void main(final String args[]) throws Exception
    {
        final EventReader.Context context = new EventReader.Context()
                .deleteOnExit(true)
                .backoffStrategy(new BackoffIdleStrategy(AGENT_IDLE_MAX_SPINS, AGENT_IDLE_MAX_YIELDS,
                        AGENT_IDLE_MIN_PARK_NS, AGENT_IDLE_MAX_PARK_NS))
                .handler(System.out::println);

        try(final EventReader eventReader = new EventReader(context))
        {
            eventReader.run();
        }
    }
}
