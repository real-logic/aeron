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
package uk.co.real_logic.aeron.common;

import uk.co.real_logic.agrona.concurrent.NanoClock;
import uk.co.real_logic.agrona.concurrent.SystemNanoClock;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Timer Wheel (NOT thread safe)
 * <p>
 * Assumes single-writer principle and timers firing on processing thread.
 * Low (or NO) garbage.
 * <h3>Implementation Details</h3>
 * <p>
 * Based on netty's HashedTimerWheel, which is based on
 * <a href="http://cseweb.ucsd.edu/users/varghese/">George Varghese</a> and
 * Tony Lauck's paper,
 * <a href="http://cseweb.ucsd.edu/users/varghese/PAPERS/twheel.ps.Z">'Hashed
 * and Hierarchical Timing Wheels: data structures to efficiently implement a
 * timer facility'</a>.  More comprehensive slides are located
 * <a href="http://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt">here</a>.
 * <p>
 * Wheel is backed by arrays. Timer cancellation is O(1). Timer scheduling might be slightly
 * longer if a lot of timers are in the same tick. The underlying tick contains an array. That
 * array grows when needed, but does not currently shrink.
 * <p>
 * Timer objects may be reused if desired, but all reuse must be done with timer cancellation, expiration,
 * and timeouts in consideration.
 * <p>
 * Caveats
 * <p>
 * Timers that expire in the same tick will not be ordered with one another. As ticks are
 * fairly large normally, this means that some timers may expire out of order.
 */
public class TimerWheel
{
    public static final int INITIAL_TICK_DEPTH = 16;

    private final long mask;
    private final long startTime;
    private final long tickDurationInNs;
    private final NanoClock clock;
    private final Timer[][] wheel;

    private long currentTick;

    /**
     * Construct a timer wheel for use in scheduling timers.
     *
     * @param tickDuration  of each tick of the wheel
     * @param timeUnit      for the tick duration
     * @param ticksPerWheel of the wheel. Must be a power of 2.
     */
    public TimerWheel(final long tickDuration, final TimeUnit timeUnit, final int ticksPerWheel)
    {
        this(new SystemNanoClock(), tickDuration, timeUnit, ticksPerWheel);
    }

    /**
     * Construct a timer wheel for use in scheduling timers.
     * <p>
     * This constructor allows a custom function to return the current time instead of {@link System#nanoTime()}.
     *
     * @param clock         to use for system time
     * @param tickDuration  of each tick of the wheel
     * @param timeUnit      for the tick duration
     * @param ticksPerWheel of the wheel. Must be a power of 2.
     */
    public TimerWheel(final NanoClock clock, final long tickDuration, final TimeUnit timeUnit, final int ticksPerWheel)
    {
        checkTicksPerWheel(ticksPerWheel);

        this.mask = ticksPerWheel - 1;
        this.clock = clock;
        this.startTime = clock.time();
        this.tickDurationInNs = timeUnit.toNanos(tickDuration);

        if (tickDurationInNs >= (Long.MAX_VALUE / ticksPerWheel))
        {
            throw new IllegalArgumentException(
                String.format("tickDuration: %d (expected: 0 < tickDurationInNs < %d",
                    tickDuration,
                    Long.MAX_VALUE / ticksPerWheel));
        }

        wheel = new Timer[ticksPerWheel][];

        for (int i = 0; i < ticksPerWheel; i++)
        {
            wheel[i] = new Timer[INITIAL_TICK_DEPTH];
        }
    }

    /**
     * Get the {@link NanoClock} used by this timer wheel.
     *
     * @return the {@link NanoClock} used by this timer wheel.
     */
    public NanoClock clock()
    {
        return clock;
    }

    /**
     * Return the current time as number of nanoseconds since start of the wheel.
     *
     * @return number of nanoseconds since start of the wheel
     */
    private long ticks()
    {
        return clock.time() - startTime;
    }

    /**
     * Return a blank {@link Timer} suitable for rescheduling.
     * <p>
     * NOTE: Appears to be a cancelled timer
     *
     * @return new blank timer
     */
    public Timer newBlankTimer()
    {
        return new Timer();
    }

    /**
     * Schedule a new timer that runs {@code task} when it expires.
     *
     * @param delay until timer should expire
     * @param unit  of time for {@code delay}
     * @param task  to execute when timer expires
     * @return {@link Timer} for timer
     */
    public Timer newTimeout(final long delay, final TimeUnit unit, final Runnable task)
    {
        final long deadline = ticks() + unit.toNanos(delay);
        final Timer timeout = new Timer(deadline, task);

        wheel[timeout.wheelIndex] = addTimeoutToArray(wheel[timeout.wheelIndex], timeout);

        return timeout;
    }

    /**
     * Reschedule an expired timer, reusing the {@link Timer} object.
     *
     * @param delay until timer should expire
     * @param unit  of time for {@code delay}
     * @param timer to reschedule
     * @throws IllegalArgumentException if timer is active
     */
    public void rescheduleTimeout(final long delay, final TimeUnit unit, final Timer timer)
    {
        rescheduleTimeout(delay, unit, timer, timer.task);
    }

    /**
     * Reschedule an expired timer, reusing the {@link Timer} object.
     *
     * @param delay until timer should expire
     * @param unit  of time for {@code delay}
     * @param timer to reschedule
     * @param task  to execute when timer expires
     * @throws IllegalArgumentException if timer is active
     */
    public void rescheduleTimeout(final long delay, final TimeUnit unit, final Timer timer, final Runnable task)
    {
        if (timer.isActive())
        {
            throw new IllegalArgumentException("timer is active");
        }

        final long deadline = ticks() + unit.toNanos(delay);

        timer.reset(deadline, task);

        wheel[timer.wheelIndex] = addTimeoutToArray(wheel[timer.wheelIndex], timer);
    }

    /**
     * Compute delay in milliseconds until next tick.
     *
     * @return number of milliseconds to next tick of the wheel.
     */
    public long computeDelayInMs()
    {
        final long deadline = tickDurationInNs * (currentTick + 1);

//        return ((deadline - ticks()) + 999999) / 1000000;
        return ((deadline - ticks()) + 1048575) / 1048576;  // TODO: temporary attempt...
    }

    /**
     * Process timers and execute any expired timers.
     *
     * @return number of timers expired.
     */
    public int expireTimers()
    {
        int timersExpired = 0;
        final long now = ticks();

        for (final Timer timer : wheel[(int)(currentTick & mask)])
        {
            if (null == timer)
            {
                continue;
            }

            if (0 >= timer.remainingRounds)
            {
                timer.remove();
                timer.state = TimerState.EXPIRED;

                if (now >= timer.deadline)
                {
                    ++timersExpired;
                    timer.task.run();
                }
            }
            else
            {
                timer.remainingRounds--;
            }
        }

        currentTick++;

        return timersExpired;
    }

    private static void checkTicksPerWheel(final int ticksPerWheel)
    {
        if (ticksPerWheel < 2 || 1 != Integer.bitCount(ticksPerWheel))
        {
            final String msg = "ticksPerWheel must be a positive power of 2: ticksPerWheel=" + ticksPerWheel;
            throw new IllegalArgumentException(msg);
        }
    }

    private static Timer[] addTimeoutToArray(final Timer[] oldArray, final Timer timeout)
    {
        for (int i = 0; i < oldArray.length; i++)
        {
            if (null == oldArray[i])
            {
                oldArray[i] = timeout;
                timeout.tickIndex = i;

                return oldArray;
            }
        }

        final Timer[] newArray = Arrays.copyOf(oldArray, oldArray.length + 1);
        newArray[oldArray.length] = timeout;
        timeout.tickIndex = oldArray.length;

        return newArray;
    }

    public enum TimerState
    {
        ACTIVE,
        CANCELLED,
        EXPIRED
    }

    public final class Timer
    {
        private int wheelIndex;
        private long deadline;
        private Runnable task;
        private int tickIndex;
        private long remainingRounds;
        private TimerState state;

        public Timer()
        {
            this.state = TimerState.CANCELLED;
        }

        public Timer(final long deadline, final Runnable task)
        {
            reset(deadline, task);
        }

        public void reset(final long deadline, final Runnable task)
        {
            this.deadline = deadline;
            this.task = task;

            final long calculatedIndex = deadline / tickDurationInNs;
            final long ticks = Math.max(calculatedIndex, currentTick);
            this.wheelIndex = (int)(ticks & mask);
            this.remainingRounds = (calculatedIndex - currentTick) / wheel.length;
            this.state = TimerState.ACTIVE;
        }

        /**
         * Cancel pending timer. Idempotent.
         *
         * @return indication of success or failure
         */
        public boolean cancel()
        {
            if (isActive())
            {
                remove();
                state = TimerState.CANCELLED;
            }

            return true;
        }

        /**
         * Is timer active or not
         *
         * @return boolean indicating if timer is active or not
         */
        public boolean isActive()
        {
            return TimerState.ACTIVE == state;
        }

        /**
         * Was timer cancelled or not
         *
         * @return boolean indicating if timer was cancelled or not
         */
        public boolean isCancelled()
        {
            return TimerState.CANCELLED == state;
        }

        /**
         * Has timer expired or not
         *
         * @return boolean indicating if timer has expired or not
         */
        public boolean isExpired()
        {
            return TimerState.EXPIRED == state;
        }

        public void remove()
        {
            wheel[this.wheelIndex][this.tickIndex] = null;
        }

        public String toString()
        {
            return "Timer{" +
                "wheelIndex=\'" + wheelIndex + "\'" +
                ", tickIndex=\'" + tickIndex + "\'" +
                ", deadline=\'" + deadline + "\'" +
                ", remainingRounds=\'" + remainingRounds + "\'" +
                "}";
        }
    }
}
