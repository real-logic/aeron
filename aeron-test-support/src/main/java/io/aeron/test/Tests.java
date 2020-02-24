/*
 * Copyright 2014-2020 Real Logic Ltd.
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
package io.aeron.test;

import org.agrona.LangUtil;

import java.time.Duration;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doAnswer;

public class Tests
{
    /**
     * Check if the interrupt flag has been set on the current thread and fail the test if it has.
     * <p>
     * This is useful for terminating tests stuck in a loop on timeout otherwise JUnit will proceed to the next test
     * and leave the thread spinning and consuming CPU resource.
     */
    public static void checkInterruptStatus()
    {
        if (Thread.interrupted())
        {
            unexpectedInterruptStackTrace();
            fail("unexpected interrupt");
        }
    }

    public static void unexpectedInterruptStackTrace()
    {
        final StringBuilder sb = new StringBuilder();

        sb.append("*** unexpected interrupt - test likely to have timed out%n");

        final StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        for (int i = 1, length = stackTraceElements.length; i < length; i++)
        {
            sb.append(stackTraceElements[i]).append("%n");
        }

        System.out.format(sb.toString());
        System.out.flush();
    }

    /**
     * Same as {@link Thread#sleep(long)} but without the checked exception.
     *
     * @param durationMs to sleep.
     */
    public static void sleep(final long durationMs)
    {
        try
        {
            Thread.sleep(durationMs);
        }
        catch (final InterruptedException ex)
        {
            unexpectedInterruptStackTrace();
            LangUtil.rethrowUnchecked(ex);
        }
    }

    /**
     * Helper method to mock {@link AutoCloseable#close()} method to throw exception.
     *
     * @param mock      to have it's method mocked
     * @param exception exception to be thrown
     * @throws Exception to make compiler happy
     */
    public static void throwOnClose(final AutoCloseable mock, final Throwable exception) throws Exception
    {
        doAnswer(
            (invocation) ->
            {
                LangUtil.rethrowUnchecked(exception);
                return null;
            }).when(mock).close();
    }

    public static void yieldingWait(final BooleanSupplier isDone)
    {
        final Timeout timeout = Objects.requireNonNull(
            TEST_TIMEOUT.get(),
            "Timeout has not be initialized.  " +
            "Make sure Tests.withTimeout(Duration) is called in your @BeforeEach method");

        while (!isDone.getAsBoolean())
        {
            if (timeout.deadlineNs <= System.nanoTime())
            {
                fail("[Timeout after " + timeout.duration + "]");
            }

            Thread.yield();
            checkInterruptStatus();
        }
    }

    public static void yieldingWait(final Supplier<String> messageSupplier)
    {
        final Timeout timeout = Objects.requireNonNull(
            TEST_TIMEOUT.get(),
            "Timeout has not be initialized.  " +
            "Make sure Tests.withTimeout(Duration) is called in your @BeforeEach method");

        if (timeout.deadlineNs <= System.nanoTime())
        {
            fail("[Timeout after " + timeout.duration + "] " + messageSupplier.get());
        }

        Thread.yield();
        checkInterruptStatus();
    }

    public static void yieldingWait(final String format, final Object... params)
    {
        final Timeout timeout = Objects.requireNonNull(
            TEST_TIMEOUT.get(),
            "Timeout has not be initialized.  " +
            "Make sure Tests.withTimeout(Duration) is called in your @BeforeEach method");

        if (timeout.deadlineNs <= System.nanoTime())
        {
            fail("[Timeout after " + timeout.duration + "] " + String.format(format, params));
        }

        Thread.yield();
        checkInterruptStatus();
    }

    public static void withTimeout(final Duration duration)
    {
        TEST_TIMEOUT.set(new Timeout(duration, System.nanoTime()));
    }

    private static final ThreadLocal<Timeout> TEST_TIMEOUT = new ThreadLocal<>();
    private static final class Timeout
    {
        private final Duration duration;
        private final long deadlineNs;

        private Timeout(final Duration duration, final long startNs)
        {
            this.duration = duration;
            this.deadlineNs = startNs + duration.toNanos();
        }
    }
}
