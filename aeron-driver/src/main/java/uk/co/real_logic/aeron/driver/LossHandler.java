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
package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.aeron.common.FeedbackDelayGenerator;
import uk.co.real_logic.aeron.common.TermHelper;
import uk.co.real_logic.aeron.common.TimerWheel;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.Counter;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.GapScanner;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracking and handling of gaps in a stream
 * <p>
 * This handler only sends a single NAK at a time.
 */
public class LossHandler
{
    private final GapScanner[] scanners;
    private final TimerWheel wheel;
    private final Counter naksSent;
    private final Gap[] gaps = new Gap[2];
    private final Gap activeGap = new Gap();
    private final FeedbackDelayGenerator delayGenerator;
    private final AtomicLong highestPosition;
    private final int positionBitsToShift;
    private final int initialTermId;

    private NakMessageSender nakMessageSender;
    private TimerWheel.Timer timer;

    private int activeIndex = 0;
    private int gapIndex = 0;
    private int activeTermId;

    /**
     * Create a loss handler for a channel.
     *
     * @param scanners       for the gaps attached to LogBuffers
     * @param wheel          for timer management
     * @param delayGenerator to use for delay determination
     * @param nakMessageSender to call when sending a NAK is indicated
     */
    public LossHandler(final GapScanner[] scanners,
                       final TimerWheel wheel,
                       final FeedbackDelayGenerator delayGenerator,
                       final NakMessageSender nakMessageSender,
                       final int activeTermId,
                       final Counter naksSent)
    {
        this.scanners = scanners;
        this.wheel = wheel;
        this.naksSent = naksSent;
        this.timer = wheel.newBlankTimer();
        this.delayGenerator = delayGenerator;
        this.nakMessageSender = nakMessageSender;
        this.positionBitsToShift = Integer.numberOfTrailingZeros(scanners[0].capacity());
        this.highestPosition = new AtomicLong(TermHelper.calculatePosition(activeTermId, 0, positionBitsToShift, activeTermId));

        for (int i = 0, max = gaps.length; i < max; i++)
        {
            this.gaps[i] = new Gap();
        }

        this.activeIndex = TermHelper.termIdToBufferIndex(activeTermId);
        this.activeTermId = activeTermId;
        this.initialTermId = activeTermId;
    }

    /**
     * Scan for gaps and handle received data.
     * <p>
     * The handler keeps track from scan to scan what is a gap and what must have been repaired.
     * @return whether a scan should be done soon or could wait
     */
    public boolean scan()
    {
        gapIndex = 0;
        final GapScanner scanner = scanners[activeIndex];
        final int numGaps = scanner.scan(this::onGap);
        onScanComplete();

        if (0 == numGaps)
        {
            if (scanner.isComplete())
            {
                activeIndex = TermHelper.rotateNext(activeIndex);
                activeTermId = activeTermId + 1;

                return true; // signal another scan should be done soon
            }
            else
            {
                // Account for 0 length heartbeat packet
                final int tail = scanner.tailVolatile();
                final long tailPosition =
                    TermHelper.calculatePosition(activeTermId, tail, positionBitsToShift, initialTermId);
                final long currentHighPosition = highestPosition.get();

                if (currentHighPosition > tailPosition)
                {
                    activateGap(activeTermId, tail, (int)(currentHighPosition - tailPosition));
                }
            }
        }

        return false;
    }

    /**
     * Called on reception of a NAK
     *
     * @param termId     in the NAK
     * @param termOffset in the NAK
     */
    public void onNak(final int termId, final int termOffset)
    {
        if (timer.isActive() && activeGap.matches(termId, termOffset))
        {
            suppressNak();
        }
    }

    /**
     * Return the active Term Id being used.
     *
     * @return active Term Id
     */
    public int activeTermId()
    {
        return activeTermId;
    }

    /**
     * Return the active scanner index
     *
     * @return active scanner index
     */
    public int activeIndex()
    {
        return activeIndex;
    }

    /**
     * A new high position may have been seen, handle that accordingly
     *
     * @param position new position in the stream
     */
    public void highestPositionCandidate(final long position)
    {
        // only set from this method which comes from the Receiver thread!
        if (highestPosition.get() < position)
        {
            highestPosition.lazySet(position);
        }
    }

    /**
     * Return the current position of the tail
     *
     * @return current tail position
     */
    public long tailPosition()
    {
        final int tail = scanners[activeIndex].tailVolatile();
        return TermHelper.calculatePosition(activeTermId, tail, positionBitsToShift, initialTermId);
    }

    /**
     * The highest position seen by the Receiver for this connection.
     *
     * @return the highest position seen by the Receiver for this connection.
     */
    public long highestPosition()
    {
        return highestPosition.get();
    }

    private void suppressNak()
    {
        scheduleTimer();
    }

    private boolean onGap(final AtomicBuffer buffer, final int offset, final int length)
    {
        if (gapIndex < gaps.length)
        {
            gaps[gapIndex].reset(activeTermId, offset, length);

            gapIndex++;

            return gapIndex == gaps.length;
        }

        return false;
    }

    private void onScanComplete()
    {
        final Gap firstGap = gaps[0];
        final boolean hasGap = gapIndex > 0;
        if (!timer.isActive() && hasGap)
        {
            activateGap(firstGap.termId, firstGap.termOffset, firstGap.length);
        }
        // if there are no gaps then the gap has probably been filled in
        else if (!hasGap)
        {
            timer.cancel();
        }
        // there's a new gap to fill
        else if (!firstGap.matches(activeGap.termId, activeGap.termOffset))
        {
            activateGap(firstGap.termId, firstGap.termOffset, firstGap.length);
        }
    }

    private void activateGap(final int termId, final int termOffset, final int length)
    {
        activeGap.reset(termId, termOffset, length);
        scheduleTimer();

        if (delayGenerator.shouldFeedbackImmediately())
        {
            sendNakMessage();
        }
    }

    private void onTimerExpire()
    {
        sendNakMessage();
        scheduleTimer();
    }

    private void sendNakMessage()
    {
        naksSent.increment();
        nakMessageSender.send(activeGap.termId, activeGap.termOffset, activeGap.length);
    }

    private long determineNakDelay()
    {
        return delayGenerator.generateDelay();
    }

    private void scheduleTimer()
    {
        final long delay = determineNakDelay();

        if (timer.isActive())
        {
            timer.cancel();
        }

        wheel.rescheduleTimeout(delay, TimeUnit.NANOSECONDS, timer, this::onTimerExpire);
    }

    static class Gap
    {
        private int termId;
        private int termOffset;
        private int length;

        public void reset(final int termId, final int termOffset, final int length)
        {
            this.termId = termId;
            this.termOffset = termOffset;
            this.length = length;
        }

        public boolean matches(final int termId, final int termOffset)
        {
            return termId == this.termId && termOffset == this.termOffset;
        }
    }
}
