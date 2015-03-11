package uk.co.real_logic.aeron.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

import uk.co.real_logic.agrona.concurrent.SystemNanoClock;

public class RateController
{

	private static final long WARMUP_IDLES = 5000;
	private static final long CALLIBRATION_IDLES = 50000;

	private static final SystemNanoClock CLOCK = new SystemNanoClock();

	private static final long PARK_NANOS;
	private static final long PARK_NANOS_FUDGE_FACTOR = 2048;
	private static final long YIELD_NANOS;
	private static final long YIELD_NANOS_FUDGE_FACTOR = 1024;
	private static final long TIME_NANOS;
	private static final long TIME_NANOS_FUDGE_FACTOR = 2;

	private final Sender sendFunc;

	private final ArrayList<IntervalInternal> intervals = new ArrayList<RateController.IntervalInternal>();
	private IntervalInternal activeInterval;
	private int activeIntervalIndex;

	/* Total messages and bytes sent across all intervals. */
	protected long messagesSent;
	protected long bytesSent;

	public boolean sendNext()
	{
		if (activeInterval == null)
		{
			return false;
		}

		if (!(activeInterval.sendNext()))
		{
			/* This interval's done; change to next interval. */
			if (++activeIntervalIndex == intervals.size())
			{
				activeIntervalIndex = 0;
			}
			activeInterval = intervals.get(activeIntervalIndex);
			return false;
		}

		return true;
	}

	public interface Sender
	{
		/** Returns the number of bytes sent, or -1 to indicate sending should stop. */
		int send();
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
		}
		/* And now do a bunch and record how long it takes. */
		final long parkStartNanos = CLOCK.time();
		for (int i = 0; i < CALLIBRATION_IDLES; i++)
		{
			LockSupport.parkNanos(yieldNanos);
		}
		final long parkEndNanos = CLOCK.time();
		/* better to over-estimate the time yield takes than to underestimate it, therefore the fudge factor. */
		YIELD_NANOS = yieldNanos * YIELD_NANOS_FUDGE_FACTOR;
		/* better to over-estimate the time park takes than to underestimate it, therefore the fudge factor. */
		PARK_NANOS = ((parkEndNanos - parkStartNanos) / CALLIBRATION_IDLES) * PARK_NANOS_FUDGE_FACTOR;
	}

	private void addIntervals(List<RateControllerInterval> intervals)
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

		protected MessagesAtBitsPerSecondInternal(RateController rateController, long messages, long bitsPerSecond)
		{
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
			final long sizeInBytes = rateController.sendFunc.send();
			if (sizeInBytes < 0)
			{
				/* Just stop here. */
				stop();
				return false;
			}
			final long sizeInBits = sizeInBytes * 8;
			final double seconds = (double)sizeInBits / (double)goalBitsPerSecond;
			final long nanoSeconds = (long)(seconds * 1000000000L);
			final long sendEndTime = CLOCK.time();

			rateController.messagesSent++;
			rateController.bytesSent += sizeInBytes;

			if (++messagesSent == messages)
			{
				lastSendTimeNanos = sendEndTime;
				stop();
				return false;
			}

			/* Now - how long should we sleep, if at all? Be sure to count
			 * all the wall-clock time that's elapsed since the last time
			 * we were called - we might not want to sleep at all. */
			nanoSleep(nanoSeconds - (sendEndTime - lastSendTimeNanos));
			lastSendTimeNanos = CLOCK.time();
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
		private final long goalMessagesPerSecond;
		/* Number of messages to send; for this interval type, this is a
		 * hard number, not just a goal.  We _have_ to send this many
		 * messages, no matter how long it takes or how slowly we end up
		 * sending them. */
		private final long messages;

		protected MessagesAtMessagesPerSecondInternal(RateController rateController, long messages, long messagesPerSecond)
		{
			this.rateController = rateController;
			this.goalMessagesPerSecond = messagesPerSecond;
			this.messages = messages;
		}

		/** Returns true if you should keep sending. */
		@Override
		public boolean sendNext()
		{
			return true;
//			/* We've got a fixed size message and a fixed bits/sec. rate.
//			 * So we can calculate the total amount of wall-clock time it
//			 * _should_ take to send the message, if we can send at the
//			 * rate we want to. */
//			final long sizeInBytes = rateController.sendFunc.send();
//			if (sizeInBytes < 0)
//			{
//				/* Just stop here. */
//				stop();
//				return false;
//			}
//			final long sizeInBits = sizeInBytes * 8;
//			final double seconds = sizeInBits / (double)goalBitsPerSecond;
//			final long nanoSeconds = (long)(seconds * 1000000000L);
//			final long sendEndTime = CLOCK.time();
//
//			/* Now - how long should we sleep, if at all? Be sure to count
//			 * all the wall-clock time that's elapsed since the last time
//			 * we were called - we might not want to sleep at all. */
//			nanoSleep(nanoSeconds - (sendEndTime - lastSendTimeNanos));
//
//			rateController.messagesSent++;
//			rateController.bytesSent += sizeInBytes;
//
//			lastSendTimeNanos = CLOCK.time();
//
//			if (++messagesSent == messages)
//			{
//				stop();
//				return false;
//			}
//			return true;
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
		private final long goalMessagesPerSecond;
		/* Number of messages to send; for this interval type, this is a
		 * hard number, not just a goal.  We _have_ to send this many
		 * messages, no matter how long it takes or how slowly we end up
		 * sending them. */
		private final double seconds;

		protected SecondsAtMessagesPerSecondInternal(RateController rateController, double seconds, long messagesPerSecond)
		{
			this.rateController = rateController;
			this.goalMessagesPerSecond = messagesPerSecond;
			this.seconds = seconds;
		}

		/** Returns true if you should keep sending. */
		@Override
		public boolean sendNext()
		{
			return true;
//			/* We've got a fixed size message and a fixed bits/sec. rate.
//			 * So we can calculate the total amount of wall-clock time it
//			 * _should_ take to send the message, if we can send at the
//			 * rate we want to. */
//			final long sizeInBytes = rateController.sendFunc.send();
//			if (sizeInBytes < 0)
//			{
//				/* Just stop here. */
//				stop();
//				return false;
//			}
//			final long sizeInBits = sizeInBytes * 8;
//			final double seconds = sizeInBits / (double)goalBitsPerSecond;
//			final long nanoSeconds = (long)(seconds * 1000000000L);
//			final long sendEndTime = CLOCK.time();
//
//			/* Now - how long should we sleep, if at all? Be sure to count
//			 * all the wall-clock time that's elapsed since the last time
//			 * we were called - we might not want to sleep at all. */
//			nanoSleep(nanoSeconds - (sendEndTime - lastSendTimeNanos));
//
//			rateController.messagesSent++;
//			rateController.bytesSent += sizeInBytes;
//
//			lastSendTimeNanos = CLOCK.time();
//
//			if (++messagesSent == messages)
//			{
//				stop();
//				return false;
//			}
//			return true;
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

		protected SecondsAtBitsPerSecondInternal(RateController rateController, double seconds, long bitsPerSecond)
		{
			this.rateController = rateController;
			this.goalBitsPerSecond = bitsPerSecond;
			this.seconds = seconds;
		}

		/** Returns true if you should keep sending. */
		@Override
		public boolean sendNext()
		{
			return true;
//			/* We've got a fixed size message and a fixed bits/sec. rate.
//			 * So we can calculate the total amount of wall-clock time it
//			 * _should_ take to send the message, if we can send at the
//			 * rate we want to. */
//			final long sizeInBytes = rateController.sendFunc.send();
//			if (sizeInBytes < 0)
//			{
//				/* Just stop here. */
//				stop();
//				return false;
//			}
//			final long sizeInBits = sizeInBytes * 8;
//			final double seconds = sizeInBits / (double)goalBitsPerSecond;
//			final long nanoSeconds = (long)(seconds * 1000000000L);
//			final long sendEndTime = CLOCK.time();
//
//			/* Now - how long should we sleep, if at all? Be sure to count
//			 * all the wall-clock time that's elapsed since the last time
//			 * we were called - we might not want to sleep at all. */
//			nanoSleep(nanoSeconds - (sendEndTime - lastSendTimeNanos));
//
//			rateController.messagesSent++;
//			rateController.bytesSent += sizeInBytes;
//
//			lastSendTimeNanos = CLOCK.time();
//
//			if (++messagesSent == messages)
//			{
//				stop();
//				return false;
//			}
//			return true;
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

		protected long lastSendTimeNanos;

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
			active = true;
		}

		public void stop()
		{
			endTimeNanos = clock.time();
			reset();
		}

	}

	public RateController(final Sender sendFunc, List<RateControllerInterval> intervals) throws Exception
	{
		if (sendFunc == null)
		{
			throw new Exception("Must specify a send method.");
		}
		this.sendFunc = sendFunc;
		if (intervals == null)
		{
			throw new Exception("Intervals list must not be null.");
		}
		if (intervals.isEmpty())
		{
			throw new Exception("Intervals list must contain at least one interval.");
		}
		addIntervals(intervals);
	}

	public long getMessagesSent()
	{
		return messagesSent;
	}

	public long getBytesSent()
	{
		return bytesSent;
	}
}
