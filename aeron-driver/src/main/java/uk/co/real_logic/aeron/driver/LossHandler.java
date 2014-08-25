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

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.GapScanner.GapHandler;

/**
 * Tracking and handling of gaps in a stream
 * <p>
 * This handler only sends a single NAK at a time.
 */
public class LossHandler
{
    private final GapScanner[] scanners;
    private final TimerWheel wheel;
    private final SystemCounters systemCounters;
    private final Gap scannedGap = new Gap();
    private final Gap activeGap = new Gap();
    private final FeedbackDelayGenerator delayGenerator;
    private final AtomicLong highestPosition;
    private final int positionBitsToShift;
    private final int initialTermId;

    private final NakMessageSender nakMessageSender;
    private final TimerWheel.Timer timer;
    private final GapHandler onGapFunc;
    private final Runnable onTimerExpireFunc;

    private int activeIndex = 0;
    private int activeTermId;

    /**
     * Create a loss handler for a channel.
     *
     * @param scanners         for the gaps attached to LogBuffers
     * @param wheel            for timer management
     * @param delayGenerator   to use for delay determination
     * @param nakMessageSender to call when sending a NAK is indicated
     */
    public LossHandler(
        final GapScanner[] scanners,
        final TimerWheel wheel,
        final FeedbackDelayGenerator delayGenerator,
        final NakMessageSender nakMessageSender,
        final int activeTermId,
        final SystemCounters systemCounters)
    {
        this.scanners = scanners;
        this.wheel = wheel;
        this.systemCounters = systemCounters;
        this.timer = wheel.newBlankTimer();
        this.delayGenerator = delayGenerator;
        this.nakMessageSender = nakMessageSender;
        this.positionBitsToShift = Integer.numberOfTrailingZeros(scanners[0].capacity());
        this.highestPosition = new AtomicLong(TermHelper.calculatePosition(activeTermId, 0, positionBitsToShift, activeTermId));

        this.activeIndex = TermHelper.termIdToBufferIndex(activeTermId);
        this.activeTermId = activeTermId;
        this.initialTermId = activeTermId;
        onGapFunc = this::onGap;
        onTimerExpireFunc = this::onTimerExpire;
    }

    /**
     * Scan for gaps and handle received data.
     * <p>
     * The handler keeps track from scan to scan what is a gap and what must have been repaired.
     *
     * @return whether a scan should be done soon or could wait
     */
    public int scan()
    {
        final GapScanner scanner = scanners[activeIndex];
        final int numGaps = scanner.scan(onGapFunc);

        if (numGaps > 0)
        {
            final Gap gap = scannedGap;
            if (!timer.isActive() || !gap.matches(activeGap.termId, activeGap.termOffset))
            {
                activateGap(gap.termId, gap.termOffset, gap.length);
            }

            return 0; // got a gap to handle, we are good until this is fixed
        }
        else if (scanner.isComplete())
        {
            activeIndex = TermHelper.rotateNext(activeIndex);
            activeTermId = activeTermId + 1;

            return 1; // signal another scan should be done soon
        }
        else
        {
            // Account for 0 length heartbeat packet
            final int tail = scanner.tailVolatile();
            final long tailPosition = TermHelper.calculatePosition(
                activeTermId, tail, positionBitsToShift, initialTermId);
            final long currentHighPosition = highestPosition.get();

            if (currentHighPosition > tailPosition)
            {
                if (!timer.isActive() || !activeGap.matches(activeTermId, tail))
                {
                    activateGap(activeTermId, tail, (int)(currentHighPosition - tailPosition));
                }
            }
            else if (timer.isActive())
            {
                timer.cancel();
            }
        }

        return 0; // We processed gaps so don't do it again too soon.
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
     * @return the highest position after checking the new candidate
     */
    public long highestPositionCandidate(final long position)
    {
        final long highestPosition = Math.max(this.highestPosition.get(), position);
        this.highestPosition.lazySet(highestPosition);

        return highestPosition;
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

    private void suppressNak()
    {
        scheduleTimer();
    }

    private boolean onGap(final AtomicBuffer buffer, final int offset, final int length)
    {
        scannedGap.reset(activeTermId, offset, length);

        return false;  // only do one gap and have it stop
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
        systemCounters.naksSent().orderedIncrement();
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

        wheel.rescheduleTimeout(delay, TimeUnit.NANOSECONDS, timer, onTimerExpireFunc);
    }

    private static final class Gap
    {
        int termId;
        int termOffset;
        int length;

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
