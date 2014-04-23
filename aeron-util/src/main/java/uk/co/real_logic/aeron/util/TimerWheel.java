package uk.co.real_logic.aeron.util;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Timer Wheel (NOT thread safe)
 *
 * assumes single-writer principle and timers firing on processing thread
 *
 *  <h3>Implementation Details</h3>
 *
 * is based on netty's HashedTimerWheel, which is based on
 * <a href="http://cseweb.ucsd.edu/users/varghese/">George Varghese</a> and
 * Tony Lauck's paper,
 * <a href="http://cseweb.ucsd.edu/users/varghese/PAPERS/twheel.ps.Z">'Hashed
 * and Hierarchical Timing Wheels: data structures to efficiently implement a
 * timer facility'</a>.  More comprehensive slides are located
 * <a href="http://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt">here</a>.
 */
public class TimerWheel
{
    public static final int INITIAL_TICK_DEPTH = 16;

    private final long mask;
    private final long startTime;
    private final long tickDurationInNanos;
    private final Supplier<Long> timeFunc;
    private final Object[] wheel;

    private long currentTick;

    public TimerWheel(final long tickDuration,
                      final TimeUnit timeUnit,
                      final int ticksPerWheel)
    {
        this(System::nanoTime, tickDuration, timeUnit, ticksPerWheel);
    }

    public TimerWheel(final Supplier<Long> timeFunc,
                      final long tickDuration,
                      final TimeUnit timeUnit,
                      final int ticksPerWheel)
    {
        checkTicksPerWheel(ticksPerWheel);

        this.mask = ticksPerWheel - 1;
        this.timeFunc = timeFunc;
        this.startTime = timeFunc.get();
        this.tickDurationInNanos = timeUnit.toNanos(tickDuration);

        if (tickDurationInNanos >= Long.MAX_VALUE / ticksPerWheel)
        {
            throw new IllegalArgumentException(String.format(
                    "tickDuration: %d (expected: 0 < tickDurationInNanos < %d",
                    tickDuration, Long.MAX_VALUE / ticksPerWheel));
        }

        this.wheel = new Object[ticksPerWheel];

        for (int i = 0; i < ticksPerWheel; i++)
        {
            this.wheel[i] = new Timer[INITIAL_TICK_DEPTH];
        }
    }

    public Timer newTimeout(final Runnable task,
                            final long delay,
                            final TimeUnit unit)
    {
        final long deadline = timeFunc.get() + unit.toNanos(delay) - this.startTime;
        final Timer timeout = new Timer(deadline, task);

        wheel[timeout.wheelIndex] = addTimeoutToArray((Timer[]) wheel[timeout.wheelIndex], timeout);

        return timeout;
    }

    public long calculateDelayInMsec()
    {
        final long deadline = tickDurationInNanos * (currentTick + 1);

        return (deadline - currentTime() + 999999) / 1000000;
    }

    public void expireTimers()
    {
        final Timer[] array = (Timer[])wheel[(int)(currentTick & mask)];
        final long deadline = currentTime();

        for (final Timer timer : array)
        {
            if (null == timer)
            {
                continue;
            }

            if (0 >= timer.remainingRounds)
            {
                timer.remove();

                if (timer.deadline <= deadline)
                {
                    timer.task.run();
                }
            }
            else
            {
                timer.remainingRounds--;
            }
        }
        currentTick++;
    }

    private static void checkTicksPerWheel(final int ticksPerWheel)
    {
        if (ticksPerWheel < 2 || 1 != Integer.bitCount(ticksPerWheel))
        {
            final String msg =
                    "ticksPerWheel must be a positive power of 2: ticksPerWheel=" + ticksPerWheel;
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

    private long currentTime()
    {
        return (timeFunc.get() - startTime);
    }

    public final class Timer
    {
        private final int wheelIndex;
        private int tickIndex;
        private final long deadline;
        private final Runnable task;
        private long remainingRounds;

        public Timer(final long deadline, final Runnable task)
        {
            this.deadline = deadline;
            this.task = task;

            final long calculatedIndex = deadline / tickDurationInNanos;
            final long ticks = Math.max(calculatedIndex, currentTick);
            this.wheelIndex = (int)(ticks & mask);
            this.remainingRounds = (calculatedIndex - currentTick) / wheel.length;

        }

        public boolean cancel()
        {
            remove();
            return true;
        }

        public void remove()
        {
            Timer[] array = (Timer[])(wheel[this.wheelIndex]);

            array[this.tickIndex] = null;
        }
    }
}
