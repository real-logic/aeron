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
package uk.co.real_logic.aeron.util;

import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TimerWheelTest
{
    private static final long ONE_MSEC_OF_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    private long controlTimestamp;

    public long getControlTimestamp()
    {
        return controlTimestamp;
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldExceptionOnNonPowerOf2TicksPerWheel()
    {
        new TimerWheel(100, TimeUnit.MILLISECONDS, 10);
    }

    @Test
    public void shouldBeAbleToCalculateDelayWithRealTime()
    {
        final TimerWheel wheel = new TimerWheel(100, TimeUnit.MILLISECONDS, 512);

        assertThat(wheel.calculateDelayInMs(), allOf(greaterThanOrEqualTo(90L), lessThanOrEqualTo(110L)));
    }

    @Test
    public void shouldBeAbleToCalculateDelay()
    {
        controlTimestamp = 0;
        final TimerWheel wheel = new TimerWheel(this::getControlTimestamp, 1, TimeUnit.MILLISECONDS, 512);

        assertThat(wheel.calculateDelayInMs(), is(1L));
    }

    @Test(timeout = 1000)
    public void shouldBeAbleToScheduleTimerOnEdgeOfTick()
    {
        controlTimestamp = 0;
        final AtomicLong firedTimestamp = new AtomicLong(-1);
        final TimerWheel wheel = new TimerWheel(this::getControlTimestamp, 1, TimeUnit.MILLISECONDS, 1024);
        final Runnable task = () -> firedTimestamp.set(wheel.now());

        wheel.newTimeout(5000, TimeUnit.MICROSECONDS, task);

        processTimersUntil(wheel, ONE_MSEC_OF_NANOS, () -> firedTimestamp.get() != -1);

        // this is the first tick after the timer, so it should be on this edge
        assertThat(firedTimestamp.get(), is(TimeUnit.MILLISECONDS.toNanos(6)));
    }

    @Test(timeout = 1000)
    public void shouldHandleNon0StartTime()
    {
        controlTimestamp = TimeUnit.MILLISECONDS.toNanos(100);
        final AtomicLong firedTimestamp = new AtomicLong(-1);
        final TimerWheel wheel = new TimerWheel(this::getControlTimestamp, 1, TimeUnit.MILLISECONDS, 1024);
        final Runnable task = () -> firedTimestamp.set(wheel.now());

        wheel.newTimeout(5000, TimeUnit.MICROSECONDS, task);

        processTimersUntil(wheel, ONE_MSEC_OF_NANOS, () -> firedTimestamp.get() != -1);

        // this is the first tick after the timer, so it should be on this edge
        assertThat(firedTimestamp.get(), is(TimeUnit.MILLISECONDS.toNanos(6)));  // relative to start time
    }

    @Test
    public void shouldHandleNanoTimeUnitTimers()
    {
        controlTimestamp = 0;
        final AtomicLong firedTimestamp = new AtomicLong(-1);
        final TimerWheel wheel = new TimerWheel(this::getControlTimestamp, 1, TimeUnit.MILLISECONDS, 1024);
        final Runnable task = () -> firedTimestamp.set(wheel.now());

        wheel.newTimeout(5000001, TimeUnit.NANOSECONDS, task);

        processTimersUntil(wheel, ONE_MSEC_OF_NANOS, () -> firedTimestamp.get() != -1);

        // this is the first tick after the timer, so it should be on this edge
        assertThat(firedTimestamp.get(), is(TimeUnit.MILLISECONDS.toNanos(6)));
    }

    @Test
    public void shouldHandleMultipleRounds()
    {
        controlTimestamp = 0;
        final AtomicLong firedTimestamp = new AtomicLong(-1);
        final TimerWheel wheel = new TimerWheel(this::getControlTimestamp, 1, TimeUnit.MILLISECONDS, 16);
        final Runnable task = () -> firedTimestamp.set(wheel.now());

        wheel.newTimeout(63, TimeUnit.MILLISECONDS, task);

        processTimersUntil(wheel, ONE_MSEC_OF_NANOS, () -> firedTimestamp.get() != -1);

        // this is the first tick after the timer, so it should be on this edge
        assertThat(firedTimestamp.get(), is(TimeUnit.MILLISECONDS.toNanos(64)));  // relative to start time
    }

    @Test
    public void shouldBeAbleToCancelTimer()
    {
        controlTimestamp = 0;
        final AtomicLong firedTimestamp = new AtomicLong(-1);
        final TimerWheel wheel = new TimerWheel(this::getControlTimestamp, 1, TimeUnit.MILLISECONDS, 256);
        final Runnable task = () -> firedTimestamp.set(wheel.now());

        final TimerWheel.Timer timeout = wheel.newTimeout(63, TimeUnit.MILLISECONDS, task);

        processTimersUntil(wheel, ONE_MSEC_OF_NANOS, () -> wheel.now() > TimeUnit.MILLISECONDS.toNanos(16));

        timeout.cancel();

        processTimersUntil(wheel, ONE_MSEC_OF_NANOS, () -> wheel.now() > TimeUnit.MILLISECONDS.toNanos(128));

        assertThat(firedTimestamp.get(), is(-1L));
    }

    @Test
    public void shouldHandleExpiringTimersInPreviousTicks()
    {
        controlTimestamp = 0;
        final AtomicLong firedTimestamp = new AtomicLong(-1);
        final TimerWheel wheel = new TimerWheel(this::getControlTimestamp, 1, TimeUnit.MILLISECONDS, 256);
        final Runnable task = () -> firedTimestamp.set(wheel.now());

        wheel.newTimeout(15, TimeUnit.MILLISECONDS, task);

        controlTimestamp += TimeUnit.MILLISECONDS.toNanos(32);

        processTimersUntil(wheel, ONE_MSEC_OF_NANOS, () -> wheel.now() > TimeUnit.MILLISECONDS.toNanos(128));

        assertThat(firedTimestamp.get(), is(TimeUnit.MILLISECONDS.toNanos(32))); // time of first expireTimers call
    }

    @Test
    public void shouldHandleMultipleTimersInDifferentTicks()
    {
        controlTimestamp = 0;
        final AtomicLong firedTimestamp1 = new AtomicLong(-1);
        final AtomicLong firedTimestamp2 = new AtomicLong(-1);
        final TimerWheel wheel = new TimerWheel(this::getControlTimestamp, 1, TimeUnit.MILLISECONDS, 256);
        final Runnable task1 = () -> firedTimestamp1.set(wheel.now());
        final Runnable task2 = () -> firedTimestamp2.set(wheel.now());

        wheel.newTimeout(15, TimeUnit.MILLISECONDS, task1);
        wheel.newTimeout(23, TimeUnit.MILLISECONDS, task2);

        processTimersUntil(wheel, ONE_MSEC_OF_NANOS, () -> wheel.now() > TimeUnit.MILLISECONDS.toNanos(128));

        assertThat(firedTimestamp1.get(), is(TimeUnit.MILLISECONDS.toNanos(16)));
        assertThat(firedTimestamp2.get(), is(TimeUnit.MILLISECONDS.toNanos(24)));
    }

    @Test
    public void shouldHandleMultipleTimersInSameTickSameRound()
    {
        controlTimestamp = 0;
        final AtomicLong firedTimestamp1 = new AtomicLong(-1);
        final AtomicLong firedTimestamp2 = new AtomicLong(-1);
        final TimerWheel wheel = new TimerWheel(this::getControlTimestamp, 1, TimeUnit.MILLISECONDS, 8);
        final Runnable task1 = () -> firedTimestamp1.set(wheel.now());
        final Runnable task2 = () -> firedTimestamp2.set(wheel.now());

        wheel.newTimeout(15, TimeUnit.MILLISECONDS, task1);
        wheel.newTimeout(15, TimeUnit.MILLISECONDS, task2);

        processTimersUntil(wheel, ONE_MSEC_OF_NANOS, () -> wheel.now() > TimeUnit.MILLISECONDS.toNanos(128));

        assertThat(firedTimestamp1.get(), is(TimeUnit.MILLISECONDS.toNanos(16)));
        assertThat(firedTimestamp2.get(), is(TimeUnit.MILLISECONDS.toNanos(16)));
    }

    @Test
    public void shouldHandleMultipleTimersInSameTickDifferentRound()
    {
        controlTimestamp = 0;
        final AtomicLong firedTimestamp1 = new AtomicLong(-1);
        final AtomicLong firedTimestamp2 = new AtomicLong(-1);
        final TimerWheel wheel = new TimerWheel(this::getControlTimestamp, 1, TimeUnit.MILLISECONDS, 8);
        final Runnable task1 = () -> firedTimestamp1.set(wheel.now());
        final Runnable task2 = () -> firedTimestamp2.set(wheel.now());

        wheel.newTimeout(15, TimeUnit.MILLISECONDS, task1);
        wheel.newTimeout(23, TimeUnit.MILLISECONDS, task2);

        processTimersUntil(wheel, ONE_MSEC_OF_NANOS, () -> wheel.now() > TimeUnit.MILLISECONDS.toNanos(128));

        assertThat(firedTimestamp1.get(), is(TimeUnit.MILLISECONDS.toNanos(16)));
        assertThat(firedTimestamp2.get(), is(TimeUnit.MILLISECONDS.toNanos(24)));
    }

    @Test
    public void shouldHandleRescheduledTimers()
    {
        controlTimestamp = 0;
        final AtomicLong firedTimestamp1 = new AtomicLong(-1);
        final AtomicLong firedTimestamp2 = new AtomicLong(-1);
        final TimerWheel wheel = new TimerWheel(this::getControlTimestamp, 1, TimeUnit.MILLISECONDS, 8);
        final Runnable task1 = () -> firedTimestamp1.set(wheel.now());
        final Runnable task2 = () -> firedTimestamp2.set(wheel.now());

        TimerWheel.Timer timer = wheel.newTimeout(15, TimeUnit.MILLISECONDS, task1);

        processTimersUntil(wheel, ONE_MSEC_OF_NANOS, () -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(50));

        assertTrue(timer.isExpired());
        assertFalse(timer.isActive());

        assertThat(firedTimestamp1.get(), is(TimeUnit.MILLISECONDS.toNanos(16)));
        assertThat(firedTimestamp2.get(), is(-1L));

        wheel.rescheduleTimeout(23, TimeUnit.MILLISECONDS, timer, task2);

        processTimersUntil(wheel, ONE_MSEC_OF_NANOS, () -> wheel.now() >= TimeUnit.MILLISECONDS.toNanos(50 + 50));

        assertTrue(timer.isExpired());
        assertFalse(timer.isActive());

        assertThat(firedTimestamp1.get(), is(TimeUnit.MILLISECONDS.toNanos(16)));
        assertThat(firedTimestamp2.get(), is(TimeUnit.MILLISECONDS.toNanos(24 + 50)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldExceptionOnReschedulingActiveTimer()
    {
        controlTimestamp = 0;
        final TimerWheel wheel = new TimerWheel(this::getControlTimestamp, 1, TimeUnit.MILLISECONDS, 8);
        final Runnable task = () -> wheel.now();

        TimerWheel.Timer timer = wheel.newTimeout(15, TimeUnit.MILLISECONDS, task);
        wheel.rescheduleTimeout(23, TimeUnit.MILLISECONDS, timer);
    }

    private long processTimersUntil(final TimerWheel wheel, final long increment, final BooleanSupplier condition)
    {
        final long startTime = wheel.now();

        while (!condition.getAsBoolean())
        {
            if (wheel.calculateDelayInMs() > 0)
            {
                controlTimestamp += increment;
            }

            wheel.expireTimers();
        }

        return (wheel.now() - startTime);
    }
}
