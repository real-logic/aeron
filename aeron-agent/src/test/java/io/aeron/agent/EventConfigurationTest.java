/*
 * Copyright 2014-2024 Real Logic Limited.
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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.EnumSet;

import static io.aeron.agent.EventConfiguration.parseEventCodes;
import static io.aeron.driver.Configuration.ASYNC_TASK_EXECUTOR_THREAD_COUNT_PROP_NAME;
import static io.aeron.driver.Configuration.asyncTaskExecutorThreadCount;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class EventConfigurationTest
{
    @Test
    public void nullValueMeansNoEventsEnabled()
    {
        final EnumSet<TestEvent> parsedEvents = parseEventCodes(
            TestEvent.class, null, Collections.emptyMap(), i -> TestEvent.values()[i], TestEvent::valueOf);

        assertEquals(EnumSet.noneOf(TestEvent.class), parsedEvents);
    }

    @Test
    public void parseEventCodesShouldIgnoreInvalidEventCodes()
    {
        final PrintStream err = System.err;
        final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        System.setErr(new PrintStream(stderr));
        try
        {
            final EnumSet<TestEvent> parsedEvents = parseEventCodes(
                TestEvent.class, "A,FOO,2", Collections.emptyMap(), i -> TestEvent.values()[i], TestEvent::valueOf);
            assertEquals(EnumSet.of(TestEvent.FOO, TestEvent.BAZ), parsedEvents);
            assertThat(stderr.toString(), startsWith("unknown event code: A"));
        }
        finally
        {
            System.setErr(err);
        }
    }

    @Test
    void asyncTaskExecutorThreadCountReturnsOneByDefault()
    {
        assertEquals(1, asyncTaskExecutorThreadCount());
        try
        {
        }
        finally
        {
            System.clearProperty(ASYNC_TASK_EXECUTOR_THREAD_COUNT_PROP_NAME);
        }
    }

    @Test
    void asyncTaskExecutorThreadCountReturnsZeroIfNegative()
    {
        System.setProperty(ASYNC_TASK_EXECUTOR_THREAD_COUNT_PROP_NAME, "-123");
        try
        {
            assertEquals(0, asyncTaskExecutorThreadCount());
        }
        finally
        {
            System.clearProperty(ASYNC_TASK_EXECUTOR_THREAD_COUNT_PROP_NAME);
        }
    }

    @Test
    void asyncTaskExecutorThreadCountReturnsAssignedValue()
    {
        System.setProperty(ASYNC_TASK_EXECUTOR_THREAD_COUNT_PROP_NAME, "4");
        try
        {
            assertEquals(4, asyncTaskExecutorThreadCount());
        }
        finally
        {
            System.clearProperty(ASYNC_TASK_EXECUTOR_THREAD_COUNT_PROP_NAME);
        }
    }

    @Test
    void asyncTaskExecutorThreadCountReturnsOneIfInvalid()
    {
        System.setProperty(ASYNC_TASK_EXECUTOR_THREAD_COUNT_PROP_NAME, "abc");
        try
        {
            assertEquals(1, asyncTaskExecutorThreadCount());
        }
        finally
        {
            System.clearProperty(ASYNC_TASK_EXECUTOR_THREAD_COUNT_PROP_NAME);
        }
    }

    enum TestEvent
    {
        FOO, BAR, BAZ
    }
}
