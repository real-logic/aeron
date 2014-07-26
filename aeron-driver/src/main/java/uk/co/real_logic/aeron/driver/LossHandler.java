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
    private final Gap[] gaps = new Gap[2];
    private final Gap activeGap = new Gap();
    private final FeedbackDelayGenerator delayGenerator;
    private final AtomicLong highPosition;
    private final int positionBitsToShift;
    private final int initialTermId;

    private NakMessageSender nakMessageSender;
    private TimerWheel.Timer timer;

    private int activeIndex = 0;
    private int scanCursor = 0;
    private int activeTermId;

    private long nakSentTimestamp;

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
                       final int activeTermId)
    {
        this.scanners = scanners;
        this.wheel = wheel;
        this.timer = wheel.newBlankTimer();
        this.delayGenerator = delayGenerator;
        this.nakMessageSender = nakMessageSender;
        this.nakSentTimestamp = wheel.now();
        this.positionBitsToShift = Integer.numberOfTrailingZeros(scanners[0].capacity());
        this.highPosition = new AtomicLong(TermHelper.calculatePosition(activeTermId, 0, positionBitsToShift, activeTermId));

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
        scanCursor = 0;
        final GapScanner currentScanner = scanners[activeIndex];
        int numGaps = currentScanner.scan(this::onGap);
        onScanComplete();

        if (0 == numGaps)
        {
            if (isGapScannerComplete(currentScanner))
            {
                // current scanner is complete, move to next one
                activeIndex = TermHelper.rotateNext(activeIndex);
                activeTermId = activeTermId + 1;

                return true; // signal another scan should be done soon
            }
            else
            {
                final int tail = currentScanner.tailVolatile();
                final long tailPosition =
                    TermHelper.calculatePosition(activeTermId, tail, positionBitsToShift, initialTermId);
                final long currentHighPosition = highPosition.get();

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
     * Return the tail of the current GapScanner
     *
     * @return tail of the current buffer being scanned
     */
    public int highestContiguousOffset()
    {
        return scanners[activeIndex].tailVolatile();
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
    public void potentialHighPosition(final long position)
    {
        // only set from this method which comes from the Receiver thread!
        if (highPosition.get() < position)
        {
            highPosition.lazySet(position);
        }
    }

    private static boolean isGapScannerComplete(final GapScanner activeScanner)
    {
        return (activeScanner.tailVolatile() >= activeScanner.capacity());
    }

    private void suppressNak()
    {
        nakSentTimestamp = wheel.now();
        scheduleTimer();
    }

    private boolean onGap(final AtomicBuffer buffer, final int offset, final int length)
    {
        if (scanCursor < gaps.length)
        {
            gaps[scanCursor].reset(activeTermId, offset, length);

            scanCursor++;

            return scanCursor == gaps.length;
        }

        return false;
    }

    private void onScanComplete()
    {
        final Gap firstGap = gaps[0];
        if (!timer.isActive())
        {
            if (scanCursor > 0)
            {
                activateGap(firstGap.termId, firstGap.termOffset, firstGap.length);
//                activeGap.reset(firstGap.termId, firstGap.termOffset, firstGap.length);
//                scheduleTimer();
//                nakSentTimestamp = wheel.now();
//
//                if (delayGenerator.shouldFeedbackImmediately())
//                {
//                    nakMessageSender.send(activeGap.termId, activeGap.termOffset, activeGap.length);
//                }
            }
        }
        else if (scanCursor == 0)
        {
            timer.cancel();
        }
        else if (!firstGap.matches(activeGap.termId, activeGap.termOffset))
        {
            activateGap(firstGap.termId, firstGap.termOffset, firstGap.length);
//            activeGap.reset(firstGap.termId, firstGap.termOffset, firstGap.length);
//            scheduleTimer();
//            nakSentTimestamp = wheel.now();
//
//            if (delayGenerator.shouldFeedbackImmediately())
//            {
//                nakMessageSender.send(activeGap.termId, activeGap.termOffset, activeGap.length);
//            }
        }
    }

    private void activateGap(final int termId, final int termOffset, final int length)
    {
        activeGap.reset(termId, termOffset, length);
        scheduleTimer();
        nakSentTimestamp = wheel.now();

        if (delayGenerator.shouldFeedbackImmediately())
        {
            nakMessageSender.send(activeGap.termId, activeGap.termOffset, activeGap.length);
        }
    }

    private void onTimerExpire()
    {
        nakMessageSender.send(activeGap.termId, activeGap.termOffset, activeGap.length);
        scheduleTimer();
        nakSentTimestamp = wheel.now();
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

    private static class Gap
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
