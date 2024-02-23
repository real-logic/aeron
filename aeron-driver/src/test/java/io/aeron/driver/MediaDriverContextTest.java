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
package io.aeron.driver;

import io.aeron.driver.MediaDriver.Context;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.test.Tests;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static io.aeron.driver.Configuration.*;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MAX_LENGTH;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MediaDriverContextTest
{
    private final Context context = new Context();

    @AfterEach
    void afterEach()
    {
        context.close();
    }

    @Test
    void nakMulticastMaxBackoffNsDefaultValue()
    {
        assertEquals(NAK_MAX_BACKOFF_DEFAULT_NS, context.nakMulticastMaxBackoffNs());
    }

    @Test
    void nakMulticastMaxBackoffNsValueFromSystemProperty()
    {
        System.setProperty(NAK_MULTICAST_MAX_BACKOFF_PROP_NAME, "333");
        try
        {
            final Context context = new Context();
            assertEquals(333, context.nakMulticastMaxBackoffNs());
        }
        finally
        {
            System.clearProperty(NAK_MULTICAST_MAX_BACKOFF_PROP_NAME);
        }
    }

    @Test
    void nakMulticastMaxBackoffNsExplicitValue()
    {
        context.nakMulticastMaxBackoffNs(Long.MIN_VALUE);
        assertEquals(Long.MIN_VALUE, context.nakMulticastMaxBackoffNs());
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -5, 0, 1024 * 1024, 1024 * 1024 + 64 * 12 - 1 })
    void conductorBufferLengthMustBeWithinRange(final int length)
    {
        context.conductorBufferLength(length);

        final ConfigurationException exception = assertThrows(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().contains("conductorBufferLength"));
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -5, 0, 1024 * 1024, 1024 * 1024 + 64 * 2 - 1 })
    void toClientsBufferLengthMustBeWithinRange(final int length)
    {
        context.toClientsBufferLength(length);

        final ConfigurationException exception = assertThrows(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().contains("toClientsBufferLength"));
    }

    @ParameterizedTest
    @ValueSource(ints = { -76, 0, 1024 * 1024 - 1, 1024 * 1024 * 1024 })
    void counterValuesBufferLengthMustBeWithinRange(final int length)
    {
        context.counterValuesBufferLength(length);

        final ConfigurationException exception = assertThrows(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().contains("counterValuesBufferLength"));
    }

    @ParameterizedTest
    @ValueSource(ints = { -76, 0, ERROR_BUFFER_LENGTH_DEFAULT - 1 })
    void errorBufferLengthMustBeWithinRange(final int length)
    {
        context.errorBufferLength(length);

        final ConfigurationException exception = assertThrows(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().contains("errorBufferLength"));
    }

    @ParameterizedTest
    @ValueSource(ints = { -6, 0, 5, LOSS_REPORT_BUFFER_LENGTH_DEFAULT - 4096 })
    void lossReportBufferLengthMustBeWithinRange(final int length, final @TempDir Path temp) throws IOException
    {
        final Path aeronDir = temp.resolve("aeron");
        Files.createDirectories(aeronDir);

        context.aeronDirectoryName(aeronDir.toString());
        context.lossReportBufferLength(length);

        final ConfigurationException exception = assertThrows(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().contains("lossReportBufferLength"));
    }

    @ParameterizedTest
    @ValueSource(ints = { -76, -3, TERM_MAX_LENGTH + 1 })
    void publicationTermWindowLengthMustBeWithinRange(final int length)
    {
        context.publicationTermWindowLength(length);

        final ConfigurationException exception = assertThrows(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().contains("publicationTermWindowLength"));
    }

    @ParameterizedTest
    @ValueSource(ints = { -76, -3, TERM_MAX_LENGTH + 1 })
    void ipcPublicationTermWindowLengthMustBeWithinRange(final int length)
    {
        context.ipcPublicationTermWindowLength(length);

        final ConfigurationException exception = assertThrows(ConfigurationException.class, context::conclude);
        assertTrue(exception.getMessage().contains("ipcPublicationTermWindowLength"));
    }

    @ParameterizedTest
    @ValueSource(ints = { -100, 42, Integer.MAX_VALUE })
    void asyncTaskExecutorThreadCount(final int threadCount)
    {
        assertEquals(1, context.asyncTaskExecutorThreadCount());

        context.asyncTaskExecutorThreadCount(threadCount);
        assertEquals(threadCount, context.asyncTaskExecutorThreadCount());
    }

    @ParameterizedTest
    @ValueSource(ints = { -5, 0 })
    void shouldDisableAsyncExecutionIfThreadsAreNotConfigured(final int asyncExecutorThreadCount)
    {
        context.asyncTaskExecutorThreadCount(asyncExecutorThreadCount);
        assertNull(context.asyncTaskExecutor());
        assertFalse(context.ownsAsyncTaskExecutor());

        context.concludeNullProperties();

        final Executor asyncTaskExecutor = context.asyncTaskExecutor();
        assertNotNull(asyncTaskExecutor);
        assertSame(CALLER_RUNS_TASK_EXECUTOR, asyncTaskExecutor);
        assertTrue(context.ownsAsyncTaskExecutor());
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 4 })
    void shouldCreateFixedThreadPoolExecutor(final int asyncExecutorThreadCount)
    {
        assertNull(context.asyncTaskExecutor());
        assertFalse(context.ownsAsyncTaskExecutor());
        context.asyncTaskExecutorThreadCount(asyncExecutorThreadCount);

        context.concludeNullProperties();

        final Executor asyncTaskExecutor = context.asyncTaskExecutor();
        assertNotNull(asyncTaskExecutor);
        assertInstanceOf(ThreadPoolExecutor.class, asyncTaskExecutor);
        assertTrue(context.ownsAsyncTaskExecutor());

        final ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor)asyncTaskExecutor;
        assertEquals(asyncExecutorThreadCount, threadPoolExecutor.getCorePoolSize());
        assertEquals(0, threadPoolExecutor.getPoolSize());
        assertEquals(0, threadPoolExecutor.getActiveCount());

        final AtomicInteger count = new AtomicInteger();
        final CopyOnWriteArraySet<Thread> threads = new CopyOnWriteArraySet<>();
        final Runnable task = () ->
        {
            count.incrementAndGet();
            threads.add(Thread.currentThread());
        };
        final int numTasks = asyncExecutorThreadCount * 3;
        for (int i = 0; i < numTasks; i++)
        {
            asyncTaskExecutor.execute(task);
        }

        Tests.await(() -> numTasks == count.get());

        assertEquals(asyncExecutorThreadCount, threads.size());
        assertEquals(asyncExecutorThreadCount, threadPoolExecutor.getPoolSize());
        for (final Thread t : threads)
        {
            MatcherAssert.assertThat(t.getName(), CoreMatchers.startsWith("async-task-executor-"));
            assertTrue(t.isDaemon());
        }
    }

    @Test
    void shouldAllowSettingTheAsyncTaskExecutor()
    {
        final Executor asynTaskExecutor = mock(Executor.class);
        context.asyncTaskExecutor(asynTaskExecutor);
        assertSame(asynTaskExecutor, context.asyncTaskExecutor());
        assertFalse(context.ownsAsyncTaskExecutor());

        context.concludeNullProperties();

        assertSame(asynTaskExecutor, context.asyncTaskExecutor());
        assertFalse(context.ownsAsyncTaskExecutor());
    }

    @Test
    void shouldCloseAsyncTaskExecutorWhenOwned()
    {
        final ExecutorService asyncTaskExecutor = mock(ExecutorService.class);
        context.asyncTaskExecutor(asyncTaskExecutor);
        context.ownsAsyncTaskExecutor(true);

        context.close();

        verify(asyncTaskExecutor).shutdownNow();
    }

    @Test
    void shouldNotCloseAsyncTaskExecutorIfNotOwned()
    {
        final ExecutorService asyncTaskExecutor = mock(ExecutorService.class);
        context.asyncTaskExecutor(asyncTaskExecutor);

        context.close();

        verifyNoInteractions(asyncTaskExecutor);
    }
}
