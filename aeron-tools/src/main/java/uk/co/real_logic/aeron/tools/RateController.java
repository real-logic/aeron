package uk.co.real_logic.aeron.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

import uk.co.real_logic.agrona.concurrent.SystemNanoClock;

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

    private final ArrayList<IntervalInternal> intervals = new ArrayList<RateController.IntervalInternal>();
    private IntervalInternal activeInterval;
    private int activeIntervalIndex;

    /* Total messages and bytes sent across all intervals. */
    private long messagesSent;
    private long bytesSent;

    private final long iterations;
    private long currentIteration;

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

    public interface Callback
    {
        /** Returns the number of bytes "sent", or -1 to indicate sending should stop. */
        int onNext();
    }

    public void rewind()
    {
        if (activeInterval == null)
        {
            return;
        }
        /* Reset all intervals and set active interval back to 0. */
        for (IntervalInternal interval : intervals)
        {
            interval.reset();
        }
        activeIntervalIndex = 0;
        activeInterval = intervals.get(0);
    }

    private static void nanoSleep(final long nanoseconds)
    {
        long nanoSecondsRemaining = nanoseconds;
        final long startNanos = CLOCK.time();
        while (nanoSecondsRemaining > PARK_NANOS)
        {
            LockSupport.parkNanos(nanoSecondsRemaining - PARK_NANOS);
            nanoSecondsRemaining = nanoseconds - (CLOCK.time() - startNanos);
        }
        while (nanoSecondsRemaining > YIELD_NANOS)
        {
            Thread.yield();
            nanoSecondsRemaining = nanoseconds - (CLOCK.time() - startNanos);
        }
        while (nanoSecondsRemaining > TIME_NANOS)
        {
            nanoSecondsRemaining = nanoseconds - (CLOCK.time() - startNanos);
        }
    }

    static
    {
        /* Calibrate our "sleep strategy" so we know what our
         * minimum sleep resolution is. */

        /* How long does it take just to call clock.time() itself? */
        for (int i = 0; i < WARMUP_IDLES; i++)
        {
            if (CLOCK.time() < 0)
            {
                throw new RuntimeException("Time travel is not permitted.");
            }
        }
        /* And now do a bunch and record how long it takes. */
        final long timeStartNanos = CLOCK.time();
        for (int i = 0; i < CALLIBRATION_IDLES; i++)
        {
            if (CLOCK.time() < 0)
            {
                throw new RuntimeException("Time travel is not permitted.");
            }
        }
        final long timeEndNanos = CLOCK.time();
        TIME_NANOS = ((timeEndNanos - timeStartNanos) / CALLIBRATION_IDLES) * TIME_NANOS_FUDGE_FACTOR;

        /* OK, test yield()s. */
        for (int i = 0; i < WARMUP_IDLES; i++)
        {
            Thread.yield();
        }
        /* And now do a bunch and record how long it takes. */
        final long yieldStartNanos = CLOCK.time();
        for (int i = 0; i < CALLIBRATION_IDLES; i++)
        {
            Thread.yield();
        }
        final long yieldEndNanos = CLOCK.time();
        final long yieldNanos = ((yieldEndNanos - yieldStartNanos) / CALLIBRATION_IDLES);

        /* Now that we have a value for how long yields take, let's try parking for at least that long.
         * (We won't ever call park() with a smaller value, since we could've just called yield instead.) */
        for (int i = 0; i < WARMUP_IDLES; i++)
        {
            LockSupport.parkNanos(yieldNanos);
            if (CLOCK.time() > (yieldEndNanos + MAX_PARK_NANOS_CALLIBRATION_TIME_NANOS))
            {
                break;
            }
        }
        /* And now do a bunch and record how long it takes. */
        final long parkStartNanos = CLOCK.time();
        int parkNanosLoops;
        for (parkNanosLoops = 0; parkNanosLoops < CALLIBRATION_IDLES; parkNanosLoops++)
        {
            LockSupport.parkNanos(yieldNanos);
            if (CLOCK.time() > (parkStartNanos + MAX_PARK_NANOS_CALLIBRATION_TIME_NANOS))
            {
                parkNanosLoops++;
                break;
            }
        }
        final long parkEndNanos = CLOCK.time();
        /* better to over-estimate the time yield takes than to underestimate it, therefore the fudge factor. */
        YIELD_NANOS = yieldNanos * YIELD_NANOS_FUDGE_FACTOR;
        /* better to over-estimate the time park takes than to underestimate it, therefore the fudge factor. */
        PARK_NANOS = ((parkEndNanos - parkStartNanos) / parkNanosLoops) * PARK_NANOS_FUDGE_FACTOR;
    }

    private void addIntervals(List<RateControllerInterval> intervals) throws Exception
    {
        for (RateControllerInterval interval : intervals)
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
                RateController rateController, long messages, long bitsPerSecond) throws Exception
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

        /** Returns true if you should keep sending. */
        @Override
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
            final long sendEndTime = CLOCK.time();

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

        @Override
        protected IntervalInternal makeInternal(RateController rateController)
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
                RateController rateController, long messages, double messagesPerSecond) throws Exception
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

        /** Returns true if you should keep sending. */
        @Override
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
            final long sendEndTime = CLOCK.time();

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

        @Override
        protected IntervalInternal makeInternal(RateController rateController)
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
                RateController rateController, double seconds, double messagesPerSecond) throws Exception
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

        /** Returns true if you should keep sending. */
        @Override
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
            final long sendEndTime = CLOCK.time();

            /* Now - how long should we sleep, if at all? Be sure to count
             * all the wall-clock time that's elapsed since the last time
             * we were called - we might not want to sleep at all. */
            nanoSleep(nanoSeconds - (sendEndTime - beginTimeNanos));

            /* End condition. */
            if ((CLOCK.time() - beginTimeNanos) >= (seconds * 1000000000))
            {
                stop();
                return false;
            }

            return true;
        }

        @Override
        protected IntervalInternal makeInternal(RateController rateController)
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
                RateController rateController, double seconds, long bitsPerSecond) throws Exception
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

        /** Returns true if you should keep sending. */
        @Override
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
            final long sendEndTime = CLOCK.time();

            /* Now - how long should we sleep, if at all? Be sure to count
             * all the wall-clock time that's elapsed since the last time
             * we were called - we might not want to sleep at all. */
            nanoSleep(nanoSeconds - (sendEndTime - beginTimeNanos));

            /* End condition. */
            if ((CLOCK.time() - beginTimeNanos) >= (seconds * 1000000000))
            {
                stop();
                return false;
            }

            return true;
        }

        @Override
        protected IntervalInternal makeInternal(RateController rateController)
        {
            return this;
        }
    }

    public abstract class IntervalInternal extends RateControllerInterval
    {
        public static final long MAX = Long.MAX_VALUE;

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
            beginTimeNanos = clock.time();
            lastBitsSent = rateController.bytesSent * 8;
            active = true;
        }

        public void stop()
        {
            endTimeNanos = clock.time();
            reset();
        }

    }

    public RateController(final Callback callback, List<RateControllerInterval> intervals, long iterations) throws Exception
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

    public RateController(final Callback callback, List<RateControllerInterval> intervals) throws Exception
    {
        this(callback, intervals, 1);
    }

    public long getMessages()
    {
        return messagesSent;
    }

    public long getBytes()
    {
        return bytesSent;
    }
}
