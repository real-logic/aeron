/*
 * Copyright 2015 Kaazing Corporation
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
package uk.co.real_logic.aeron.tools;

import uk.co.real_logic.agrona.concurrent.SystemNanoClock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

public class RateController
{
    private static final long WARMUP_IDLES = 5000;
    private static final long CALLIBRATION_IDLES = 50000;
    /* Some platforms (like Windows) have a very coarse parkNanos resolution on the
     * order of a nearly 16 millisecond minimum sleep time (!).  Don't spend more
     * than a second on the warmup or measurement loops. */
    private static final long MAX_PARK_NANOS_CALLIBRATION_TIME_NANOS = 1000000000;

    private static final SystemNanoClock CLOCK = new SystemNanoClock();

    private static final long PARK_NANOS;
    private static final long PARK_NANOS_FUDGE_FACTOR = 2048;
    private static final long YIELD_NANOS;
    private static final long YIELD_NANOS_FUDGE_FACTOR = 1024;
    private static final long TIME_NANOS;
    private static final long TIME_NANOS_FUDGE_FACTOR = 2;

    private final Callback sendFunc;

    private final ArrayList<IntervalInternal> intervals = new ArrayList<>();
    private IntervalInternal activeInterval;
    private int activeIntervalIndex;

    /* Total messages and bytes sent across all intervals. */
    private long messagesSent;
    private long bytesSent;

    private final long iterations;
    private long currentIteration;

    static
    {
        /* Calibrate our "sleep strategy" so we know what our
         * minimum sleep resolution is. */

        /* How long does it take just to call clock.time() itself? */
        for (int i = 0; i < WARMUP_IDLES; i++)
        {
            if (CLOCK.nanoTime() < 0)
            {
                throw new RuntimeException("Time travel is not permitted.");
            }
        }
        /* And now do a bunch and record how long it takes. */
        final long timeStartNs = CLOCK.nanoTime();
        for (int i = 0; i < CALLIBRATION_IDLES; i++)
        {
            if (CLOCK.nanoTime() < 0)
            {
                throw new RuntimeException("Time travel is not permitted.");
            }
        }
        final long timeEndNs = CLOCK.nanoTime();
        TIME_NANOS = ((timeEndNs - timeStartNs) / CALLIBRATION_IDLES) * TIME_NANOS_FUDGE_FACTOR;

        /* OK, test yield()s. */
        for (int i = 0; i < WARMUP_IDLES; i++)
        {
            Thread.yield();
        }
        /* And now do a bunch and record how long it takes. */
        final long yieldStartNs = CLOCK.nanoTime();
        for (int i = 0; i < CALLIBRATION_IDLES; i++)
        {
            Thread.yield();
        }
        final long yieldEndNs = CLOCK.nanoTime();
        final long yieldNs = ((yieldEndNs - yieldStartNs) / CALLIBRATION_IDLES);

        /* Now that we have a value for how long yields take, let's try parking for at least that long.
         * (We won't ever call park() with a smaller value, since we could've just called yield instead.) */
        for (int i = 0; i < WARMUP_IDLES; i++)
        {
            LockSupport.parkNanos(yieldNs);
            if (CLOCK.nanoTime() > (yieldEndNs + MAX_PARK_NANOS_CALLIBRATION_TIME_NANOS))
            {
                break;
            }
        }

        /* And now do a bunch and record how long it takes. */
        final long parkStartNs = CLOCK.nanoTime();
        int parkNanosLoops;
        for (parkNanosLoops = 0; parkNanosLoops < CALLIBRATION_IDLES; parkNanosLoops++)
        {
            LockSupport.parkNanos(yieldNs);
            if (CLOCK.nanoTime() > (parkStartNs + MAX_PARK_NANOS_CALLIBRATION_TIME_NANOS))
            {
                parkNanosLoops++;
                break;
            }
        }

        final long parkEndNanos = CLOCK.nanoTime();
        /* better to over-estimate the time yield takes than to underestimate it, therefore the fudge factor. */
        YIELD_NANOS = yieldNs * YIELD_NANOS_FUDGE_FACTOR;
        /* better to over-estimate the time park takes than to underestimate it, therefore the fudge factor. */
        PARK_NANOS = ((parkEndNanos - parkStartNs) / parkNanosLoops) * PARK_NANOS_FUDGE_FACTOR;
    }

    /**
     * This is the most commonly-called RateController method; it calls the Callback associated
     * with this RateController and then pauses the current thread for the time needed, if any,
     * before the next event should occur.  next() will usually be called in an empty loop, like this:
     * while (rateController.next())
     * {
     * }
     *
     * @return true if next should be called again, false if the RateController has now finished
     * all intervals
     */
    public boolean next()
    {
        if (activeInterval == null)
        {
            return false;
        }

        if (!activeInterval.active)
        {
            activeInterval.play();
        }

        if (!(activeInterval.sendNext()))
        {
            /* This interval's done; change to next interval. */
            if (++activeIntervalIndex == intervals.size())
            {
                if (++currentIteration == iterations)
                {
                    /* We're done. */
                    return false;
                }
                /* Start a new iteration at the beginning of the interval list. */
                activeIntervalIndex = 0;
            }
            activeInterval = intervals.get(activeIntervalIndex);
        }

        return true;
    }

    /**
     * Rate controller callback, called at the beginning of each call to {@link #next() next}
     * and supplied by the user of RateController.
     */
    public interface Callback
    {
        /**
         * Returns the number of bytes "sent", or -1 to indicate sending should stop.
         */
        int onNext();
    }

    /**
     * Resets the RateController back to the beginning, starting at the first interval.
     */
    public void rewind()
    {
        if (activeInterval == null)
        {
            return;
        }

        /* Reset all intervals and set active interval back to 0. */
        intervals.forEach(RateController.IntervalInternal::reset);

        activeIntervalIndex = 0;
        activeInterval = intervals.get(0);
    }

    private static void nanoSleep(final long nanoseconds)
    {
        long nanoSecondsRemaining = nanoseconds;
        final long startNanos = CLOCK.nanoTime();
        while (nanoSecondsRemaining > PARK_NANOS)
        {
            LockSupport.parkNanos(nanoSecondsRemaining - PARK_NANOS);
            nanoSecondsRemaining = nanoseconds - (CLOCK.nanoTime() - startNanos);
        }

        while (nanoSecondsRemaining > YIELD_NANOS)
        {
            Thread.yield();
            nanoSecondsRemaining = nanoseconds - (CLOCK.nanoTime() - startNanos);
        }

        while (nanoSecondsRemaining > TIME_NANOS)
        {
            nanoSecondsRemaining = nanoseconds - (CLOCK.nanoTime() - startNanos);
        }
    }

    private void addIntervals(final List<RateControllerInterval> intervals) throws Exception
    {
        for (final RateControllerInterval interval : intervals)
        {
            this.intervals.add(interval.makeInternal(this));
        }
        if (activeInterval == null)
        {
            activeInterval = this.intervals.get(0);
        }
    }

    class MessagesAtBitsPerSecondInternal extends IntervalInternal
    {
        /* The rate we _want_ to achieve, if possible.  Might not be able
         * to hit it exactly due to receiver pacing, etc.  But it's what
         * we're aiming for. */
        private final long goalBitsPerSecond;
        /* Number of messages to send; for this interval type, this is a
         * hard number, not just a goal.  We _have_ to send this many
         * messages, no matter how long it takes or how slowly we end up
         * sending them. */
        private final long messages;

        protected MessagesAtBitsPerSecondInternal(
            final RateController rateController, final long messages, final long bitsPerSecond) throws Exception
        {
            if (messages <= 0)
            {
                throw new Exception("The number of messages in a MessagesAtBitsPerSecond interval must be >= 1.");
            }
            if (bitsPerSecond <= 0)
            {
                throw new Exception("The bits per second in a MessagesAtBitsPerSecond interval must be >= 1.");
            }
            this.rateController = rateController;
            this.goalBitsPerSecond = bitsPerSecond;
            this.messages = messages;
        }

        /**
         * Returns true if you should keep sending.
         */
        public boolean sendNext()
        {
            /* We've got a fixed size message and a fixed bits/sec. rate.
             * So we can calculate the total amount of wall-clock time it
             * _should_ take to send the message, if we can send at the
             * rate we want to. */

            /* Always start out sending immediately; if the previous
             * interval needed to delay a bit after its last send,
             * then it should have done so. */
            final long sizeInBytes = rateController.sendFunc.onNext();
            if (sizeInBytes < 0)
            {
                /* Just stop here; returned size < 0 means the user wants us to stop. */
                stop();
                return false;
            }

            rateController.messagesSent++;
            rateController.bytesSent += sizeInBytes;
            bitsSent += (sizeInBytes * 8);

            /* How many bits have we sent so far between now and when the interval
             * first started?  How long has it taken to send all those bits?  How
             * long _should_ it have taken?  If we're over-budget, sleep for a bit. */
            final double seconds = (double)bitsSent / (double)goalBitsPerSecond;
            final long nanoSeconds = (long)(seconds * 1000000000L);
            final long sendEndTime = CLOCK.nanoTime();

            /* Now - how long should we sleep, if at all? Be sure to count
             * all the wall-clock time that's elapsed since the last time
             * we were called - we might not want to sleep at all. */
            nanoSleep(nanoSeconds - (sendEndTime - beginTimeNanos));

            if (++messagesSent == messages)
            {
                stop();
                return false;
            }

            return true;
        }

        protected IntervalInternal makeInternal(final RateController rateController)
        {
            return this;
        }
    }

    class MessagesAtMessagesPerSecondInternal extends IntervalInternal
    {
        /* The rate we _want_ to achieve, if possible.  Might not be able
         * to hit it exactly due to receiver pacing, etc.  But it's what
         * we're aiming for. */
        private final double goalMessagesPerSecond;
        /* Number of messages to send; for this interval type, this is a
         * hard number, not just a goal.  We _have_ to send this many
         * messages, no matter how long it takes or how slowly we end up
         * sending them. */
        private final long messages;

        protected MessagesAtMessagesPerSecondInternal(
            final RateController rateController, final long messages, final double messagesPerSecond) throws Exception
        {
            if (messages <= 0)
            {
                throw new Exception("The number of messages in a MessagesAtMessagesPerSecond interval must be >= 1.");
            }
            if (messagesPerSecond <= 0)
            {
                throw new Exception("The messages per second in a MessagesAtMessagesPerSecond interval must be > 0.");
            }
            this.rateController = rateController;
            this.goalMessagesPerSecond = messagesPerSecond;
            this.messages = messages;
        }

        /**
         * Returns true if you should keep sending.
         */
        public boolean sendNext()
        {
            /* Always start out sending immediately; if the previous
             * interval needed to delay a bit after its last send,
             * then it should have done so. */
            final long sizeInBytes = rateController.sendFunc.onNext();
            if (sizeInBytes < 0)
            {
                /* Just stop here; returned size < 0 means the user wants us to stop. */
                stop();
                return false;
            }

            rateController.messagesSent++;
            rateController.bytesSent += sizeInBytes;
            bitsSent += (sizeInBytes * 8);
            messagesSent++;

            /* How many bits have we sent so far between now and when the interval
             * first started?  How long has it taken to send all those bits?  How
             * long _should_ it have taken?  If we're over-budget, sleep for a bit. */
            final double seconds = messagesSent / goalMessagesPerSecond;
            final long nanoSeconds = (long)(seconds * 1000000000L);
            final long sendEndTime = CLOCK.nanoTime();

            /* Now - how long should we sleep, if at all? Be sure to count
             * all the wall-clock time that's elapsed since the last time
             * we were called - we might not want to sleep at all. */
            nanoSleep(nanoSeconds - (sendEndTime - beginTimeNanos));

            if (messagesSent == messages)
            {
                stop();
                return false;
            }

            return true;
        }

        protected IntervalInternal makeInternal(final RateController rateController)
        {
            return this;
        }
    }

    class SecondsAtMessagesPerSecondInternal extends IntervalInternal
    {
        /* The rate we _want_ to achieve, if possible.  Might not be able
         * to hit it exactly due to receiver pacing, etc.  But it's what
         * we're aiming for. */
        private final double goalMessagesPerSecond;
        /* Number of messages to send; for this interval type, this is a
         * hard number, not just a goal.  We _have_ to send this many
         * messages, no matter how long it takes or how slowly we end up
         * sending them. */
        private final double seconds;

        protected SecondsAtMessagesPerSecondInternal(
            final RateController rateController, final double seconds, final double messagesPerSecond) throws Exception
        {
            if (seconds <= 0)
            {
                throw new Exception("The number of seconds in a SecondsAtMessagesPerSecond interval must be > 0.");
            }
            if (messagesPerSecond < 0)
            {
                throw new Exception("The messages per second in a SecondsAtMessagesPerSecond interval cannot be negative.");
            }
            this.rateController = rateController;
            this.goalMessagesPerSecond = messagesPerSecond;
            this.seconds = seconds;
        }

        /**
         * Returns true if you should keep sending.
         */
        public boolean sendNext()
        {
            /* As a special case... If we're sending 0 messages per second, then this is really just a sleep.
             * So sleep the appropriate amount of time and then end. */
            if (goalMessagesPerSecond == 0)
            {
                nanoSleep((long)(seconds * 1000000000));
                stop();
                return false;
            }

            /* Always start out sending immediately; if the previous
             * interval needed to delay a bit after its last send,
             * then it should have done so. */
            final long sizeInBytes = rateController.sendFunc.onNext();
            if (sizeInBytes < 0)
            {
                /* Just stop here; returned size < 0 means the user wants us to stop. */
                stop();
                return false;
            }

            rateController.messagesSent++;
            rateController.bytesSent += sizeInBytes;
            bitsSent += (sizeInBytes * 8);
            messagesSent++;

            /* How many bits have we sent so far between now and when the interval
             * first started?  How long has it taken to send all those bits?  How
             * long _should_ it have taken?  If we're over-budget, sleep for a bit. */
            final double secondsWanted = messagesSent / goalMessagesPerSecond;
            final long nanoSeconds = (long)(secondsWanted * 1000000000L);
            final long sendEndTime = CLOCK.nanoTime();

            /* Now - how long should we sleep, if at all? Be sure to count
             * all the wall-clock time that's elapsed since the last time
             * we were called - we might not want to sleep at all. */
            nanoSleep(nanoSeconds - (sendEndTime - beginTimeNanos));

            /* End condition. */
            if ((CLOCK.nanoTime() - beginTimeNanos) >= (seconds * 1000000000))
            {
                stop();
                return false;
            }

            return true;
        }

        protected IntervalInternal makeInternal(final RateController rateController)
        {
            return this;
        }
    }

    class SecondsAtBitsPerSecondInternal extends IntervalInternal
    {
        /* The rate we _want_ to achieve, if possible.  Might not be able
         * to hit it exactly due to receiver pacing, etc.  But it's what
         * we're aiming for. */
        private final long goalBitsPerSecond;
        /* Number of messages to send; for this interval type, this is a
         * hard number, not just a goal.  We _have_ to send this many
         * messages, no matter how long it takes or how slowly we end up
         * sending them. */
        private final double seconds;

        protected SecondsAtBitsPerSecondInternal(
            final RateController rateController, final double seconds, final long bitsPerSecond) throws Exception
        {
            if (seconds <= 0)
            {
                throw new Exception("The number of seconds in a SecondsAtBitsPerSecond interval must be > 0.");
            }
            if (bitsPerSecond < 0)
            {
                throw new Exception("The bits per second in a SecondsAtBitsPerSecond interval cannot be negative.");
            }
            this.rateController = rateController;
            this.goalBitsPerSecond = bitsPerSecond;
            this.seconds = seconds;
        }

        /**
         * Returns true if you should keep sending.
         */
        public boolean sendNext()
        {
            /* As a special case... If we're sending 0 bits per second, then this is really just a sleep.
             * So sleep the appropriate amount of time and then end. */
            if (goalBitsPerSecond == 0)
            {
                nanoSleep((long)(seconds * 1000000000));
                stop();
                return false;
            }

            /* Always start out sending immediately; if the previous
             * interval needed to delay a bit after its last send,
             * then it should have done so. */
            final long sizeInBytes = rateController.sendFunc.onNext();
            if (sizeInBytes < 0)
            {
                /* Just stop here; returned size < 0 means the user wants us to stop. */
                stop();
                return false;
            }

            rateController.messagesSent++;
            rateController.bytesSent += sizeInBytes;
            bitsSent += (sizeInBytes * 8);
            messagesSent++;

            /* How many bits have we sent so far between now and when the interval
             * first started?  How long has it taken to send all those bits?  How
             * long _should_ it have taken?  If we're over-budget, sleep for a bit. */
            final double secondsWanted = bitsSent / goalBitsPerSecond;
            final long nanoSeconds = (long)(secondsWanted * 1000000000L);
            final long sendEndTime = CLOCK.nanoTime();

            /* Now - how long should we sleep, if at all? Be sure to count
             * all the wall-clock time that's elapsed since the last time
             * we were called - we might not want to sleep at all. */
            nanoSleep(nanoSeconds - (sendEndTime - beginTimeNanos));

            /* End condition. */
            if ((CLOCK.nanoTime() - beginTimeNanos) >= (seconds * 1000000000))
            {
                stop();
                return false;
            }

            return true;
        }

        protected IntervalInternal makeInternal(final RateController rateController)
        {
            return this;
        }
    }

    public abstract class IntervalInternal extends RateControllerInterval
    {
        private final SystemNanoClock clock = new SystemNanoClock();

        protected long lastBitsSent;

        protected RateController rateController;

        public void reset()
        {
            active = false;
            bitsSent = 0;
            messagesSent = 0;
        }

        public abstract boolean sendNext();

        public void play()
        {
            beginTimeNanos = clock.nanoTime();
            lastBitsSent = rateController.bytesSent * 8;
            active = true;
        }

        public void stop()
        {
            endTimeNanos = clock.nanoTime();
            reset();
        }

    }

    public RateController(final Callback callback, final List<RateControllerInterval> intervals,
        final long iterations) throws Exception
    {
        if (iterations <= 0)
        {
            throw new Exception("Iterations must be >= 1.");
        }
        if (callback == null)
        {
            throw new Exception("Must specify a callback method.");
        }
        this.sendFunc = callback;
        if (intervals == null)
        {
            throw new Exception("Intervals list must not be null.");
        }
        if (intervals.isEmpty())
        {
            throw new Exception("Intervals list must contain at least one interval.");
        }
        addIntervals(intervals);
        this.iterations = iterations;
    }

    public RateController(final Callback callback, final List<RateControllerInterval> intervals) throws Exception
    {
        this(callback, intervals, 1);
    }

    public long messages()
    {
        return messagesSent;
    }

    public long bytes()
    {
        return bytesSent;
    }
}
