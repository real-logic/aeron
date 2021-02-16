/*
 * Copyright 2014-2021 Real Logic Limited.
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

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.RegistrationException;
import io.aeron.exceptions.TimeoutException;
import org.agrona.LangUtil;
import org.agrona.SystemUtil;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doAnswer;

/**
 * Utilities to help with writing tests.
 */
public class Tests
{
    public static final IdleStrategy SLEEP_1_MS = new SleepingMillisIdleStrategy(1);

    /**
     * Set a private field in a class for testing.
     *
     * @param instance  of the object to set the field value.
     * @param fieldName to be set.
     * @param value     to be set on the field.
     */
    public static void setField(final Object instance, final String fieldName, final Object value)
    {
        try
        {
            final Field field = instance.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(instance, value);
        }
        catch (final Throwable t)
        {
            LangUtil.rethrowUnchecked(t);
        }
    }

    /**
     * Check if the interrupt flag has been set on the current thread and fail the test if it has.
     * <p>
     * This is useful for terminating tests stuck in a loop on timeout otherwise JUnit will proceed to the next test
     * and leave the thread spinning and consuming CPU resource.
     */
    public static void checkInterruptStatus()
    {
        if (Thread.currentThread().isInterrupted())
        {
            unexpectedInterruptStackTrace(null);
            fail("unexpected interrupt");
        }
    }

    /**
     * Check if the interrupt flag has been set on the current thread and fail the test if it has.
     * <p>
     * This is useful for terminating tests stuck in a loop on timeout otherwise JUnit will proceed to the next test
     * and leave the thread spinning and consuming CPU resource.
     *
     * @param messageSupplier additional context information to include in the failure message
     */
    public static void checkInterruptStatus(final Supplier<String> messageSupplier)
    {
        if (Thread.currentThread().isInterrupted())
        {
            final String message = messageSupplier.get();
            unexpectedInterruptStackTrace(message);
            fail("unexpected interrupt - " + message);
        }
    }

    /**
     * Check if the interrupt flag has been set on the current thread and fail the test if it has.
     * <p>
     * This is useful for terminating tests stuck in a loop on timeout otherwise JUnit will proceed to the next test
     * and leave the thread spinning and consuming CPU resource.
     *
     * @param format A format string, {@link java.util.Formatter} to use as additional context information in the
     *               failure message
     * @param args   arguments to the format string
     */
    public static void checkInterruptStatus(final String format, final Object... args)
    {
        if (Thread.currentThread().isInterrupted())
        {
            final String message = String.format(format, args);
            unexpectedInterruptStackTrace(message);
            fail("unexpected interrupt - " + message);
        }
    }

    public static void checkInterruptStatus(final String message)
    {
        if (Thread.currentThread().isInterrupted())
        {
            unexpectedInterruptStackTrace(message);
            fail("unexpected interrupt - " + message);
        }
    }

    public static void unexpectedInterruptStackTrace(final String message)
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("*** unexpected interrupt");

        if (null != message)
        {
            sb.append(" - ").append(message);
        }

        appendStackTrace(sb).append('\n');

        System.out.println(sb.toString());
        System.out.println(SystemUtil.threadDump());
    }

    public static StringBuilder appendStackTrace(final StringBuilder sb)
    {
        return appendStackTrace(sb, Thread.currentThread().getStackTrace());
    }

    public static StringBuilder appendStackTrace(final StringBuilder sb, final StackTraceElement[] stackTraceElements)
    {
        sb.append(System.lineSeparator());

        for (int i = 1, length = stackTraceElements.length; i < length; i++)
        {
            sb.append(stackTraceElements[i]).append(System.lineSeparator());
        }

        return sb;
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
            unexpectedInterruptStackTrace(null);
            LangUtil.rethrowUnchecked(ex);
        }
    }

    /**
     * Same as {@link Thread#sleep(long)} but without the checked exception.
     *
     * @param durationMs      to sleep.
     * @param messageSupplier of message to be reported on interrupt.
     */
    public static void sleep(final long durationMs, final Supplier<String> messageSupplier)
    {
        try
        {
            Thread.sleep(durationMs);
        }
        catch (final InterruptedException ex)
        {
            unexpectedInterruptStackTrace(messageSupplier.get());
            LangUtil.rethrowUnchecked(ex);
        }
    }

    /**
     * Same as {@link Thread#sleep(long)} but without the checked exception.
     *
     * @param durationMs to sleep.
     * @param format     of the message.
     * @param params     to be formatted.
     */
    public static void sleep(final long durationMs, final String format, final Object... params)
    {
        try
        {
            Thread.sleep(durationMs);
        }
        catch (final InterruptedException ex)
        {
            unexpectedInterruptStackTrace(String.format(format, params));
            LangUtil.rethrowUnchecked(ex);
        }
    }

    /**
     * Yield the thread then check for interrupt in a test.
     *
     * @see #checkInterruptStatus()
     */
    public static void yield()
    {
        Thread.yield();
        checkInterruptStatus();
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

    public static void wait(final IdleStrategy idleStrategy, final Supplier<String> messageSupplier)
    {
        idleStrategy.idle();
        checkInterruptStatus(messageSupplier);
    }

    public static void wait(final IdleStrategy idleStrategy, final String format, final Object... params)
    {
        idleStrategy.idle();
        checkInterruptStatus(format, params);
    }

    public static void wait(final IdleStrategy idleStrategy, final String message)
    {
        idleStrategy.idle();
        checkInterruptStatus(message);
    }

    public static void yieldingWait(final Supplier<String> messageSupplier)
    {
        wait(YieldingIdleStrategy.INSTANCE, messageSupplier);
    }

    public static void yieldingWait(final String format, final Object... params)
    {
        wait(YieldingIdleStrategy.INSTANCE, format, params);
    }

    public static void yieldingWait(final String message)
    {
        wait(YieldingIdleStrategy.INSTANCE, message);
    }

    /**
     * Execute a task until a condition is satisfied, or a maximum number of iterations, or a timeout is reached.
     *
     * @param condition         keep executing while true.
     * @param iterationConsumer to be invoked with the iteration count.
     * @param maxIterations     to be executed.
     * @param timeoutNs         to stay within.
     */
    public static void executeUntil(
        final BooleanSupplier condition,
        final IntConsumer iterationConsumer,
        final int maxIterations,
        final long timeoutNs)
    {
        final long startNs = System.nanoTime();
        long nowNs;
        int i = 0;

        do
        {
            checkInterruptStatus();
            iterationConsumer.accept(i);
            nowNs = System.nanoTime();
        }
        while (!condition.getAsBoolean() && ((nowNs - startNs) < timeoutNs) && i++ < maxIterations);
    }

    public static void await(final BooleanSupplier conditionSupplier, final long timeoutNs)
    {
        final long deadlineNs = System.nanoTime() + timeoutNs;
        while (!conditionSupplier.getAsBoolean())
        {
            if ((deadlineNs - System.nanoTime()) <= 0)
            {
                throw new TimeoutException();
            }

            Tests.yield();
        }
    }

    public static void await(final BooleanSupplier conditionSupplier)
    {
        while (!conditionSupplier.getAsBoolean())
        {
            Tests.yield();
        }
    }

    public static void onError(final Throwable ex)
    {
        if (ex instanceof AeronException && ((AeronException)ex).category() == AeronException.Category.WARN)
        {
            //System.out.println("Warning: " + ex.getMessage());
            return;
        }

        ex.printStackTrace();
    }

    public static void awaitValue(final AtomicLong counter, final long value)
    {
        long counterValue;
        while ((counterValue = counter.get()) < value)
        {
            Thread.yield();
            if (Thread.currentThread().isInterrupted())
            {
                unexpectedInterruptStackTrace("awaiting=" + value + " counter=" + counterValue);
                fail("unexpected interrupt");
            }
        }
    }

    public static void awaitValue(final AtomicCounter counter, final long value)
    {
        long counterValue;
        while ((counterValue = counter.get()) < value)
        {
            Thread.yield();
            if (Thread.currentThread().isInterrupted())
            {
                unexpectedInterruptStackTrace("awaiting=" + value + " counter=" + counterValue);
                fail("unexpected interrupt");
            }

            if (counter.isClosed())
            {
                unexpectedInterruptStackTrace("awaiting=" + value + " counter=" + counterValue);
            }
        }
    }

    public static void awaitCounterDelta(final CountersReader reader, final int counterId, final long delta)
    {
        awaitCounterDelta(reader, counterId, reader.getCounterValue(counterId), delta);
    }

    public static void awaitCounterDelta(
        final CountersReader reader, final int counterId, final long initialValue, final long delta)
    {
        final long expectedValue = initialValue + delta;
        final Supplier<String> counterMessage = () ->
            "Timed out waiting for counter '" + reader.getCounterLabel(counterId) +
            "' to increase to at least " + expectedValue;

        while (reader.getCounterValue(counterId) < expectedValue)
        {
            wait(SLEEP_1_MS, counterMessage);
        }
    }

    public static Subscription reAddSubscription(final Aeron aeron, final String channel, final int streamId)
    {
        // In cases where a subscription is added immediately after closing one it is possible that
        // the second one can fail, so retry in that case.
        while (true)
        {
            try
            {
                return aeron.addSubscription(channel, streamId);
            }
            catch (final RegistrationException ex)
            {
                if (ex.category() != AeronException.Category.WARN)
                {
                    throw ex;
                }

                yieldingWait(ex.getMessage());
            }
        }
    }

    public static void awaitConnected(final Publication publication)
    {
        while (!publication.isConnected())
        {
            Tests.yield();
        }
    }

    public static void awaitConnected(final Subscription subscription)
    {
        while (!subscription.isConnected())
        {
            Tests.yield();
        }
    }

    public static void awaitConnections(final Subscription subscription, final int connectionCount)
    {
        while (subscription.imageCount() < connectionCount)
        {
            Tests.yield();
        }
    }

    public static String generateStringWithSuffix(final String prefix, final String suffix, final int repeatSuffixTimes)
    {
        final StringBuilder builder = new StringBuilder(prefix);

        for (int i = 0; i < repeatSuffixTimes; i++)
        {
            builder.append(suffix);
        }

        return builder.toString();
    }
}
