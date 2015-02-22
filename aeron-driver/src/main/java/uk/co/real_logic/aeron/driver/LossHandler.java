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
import uk.co.real_logic.aeron.common.TimerWheel;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.GapScanner;

import java.util.concurrent.TimeUnit;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.GapScanner.GapHandler;

/**
 * Tracking and handling of gaps in a stream
 * <p>
 * This handler only sends a single NAK at a time.
 */
public class LossHandler
{
    private final UnsafeBuffer[] termBuffers;
    private final TimerWheel wheel;
    private final AtomicCounter naksSent;
    private final Gap scannedGap = new Gap();
    private final Gap activeGap = new Gap();
    private final FeedbackDelayGenerator delayGenerator;
    private final int positionBitsToShift;
    private final int mask;
    private final int initialTermId;

    private final NakMessageSender nakMessageSender;
    private final TimerWheel.Timer timer;
    private final GapHandler onGapFunc = this::onGap;
    private final Runnable onTimerExpireFunc = this::onTimerExpire;


    /**
     * Create a loss handler for a channel.
     *
     * @param termBuffers       for the gaps attached to LogBuffers
     * @param wheel             for timer management
     * @param delayGenerator    to use for delay determination
     * @param nakMessageSender  to call when sending a NAK is indicated
     * @param initialTermId     to use
     * @param systemCounters    to use for tracking purposes
     */
    public LossHandler(
        final UnsafeBuffer[] termBuffers,
        final TimerWheel wheel,
        final FeedbackDelayGenerator delayGenerator,
        final NakMessageSender nakMessageSender,
        final int initialTermId,
        final SystemCounters systemCounters)
    {
        this.termBuffers = termBuffers;
        this.wheel = wheel;
        this.naksSent = systemCounters.naksSent();
        this.timer = wheel.newBlankTimer();
        this.delayGenerator = delayGenerator;
        this.nakMessageSender = nakMessageSender;

        final int capacity = termBuffers[0].capacity();
        this.positionBitsToShift = Integer.numberOfTrailingZeros(capacity);
        this.mask = (int)((long)capacity - 1);

        this.initialTermId = initialTermId;
    }

    /**
     * Scan for gaps and handle received data.
     * <p>
     * The handler keeps track from scan to scan what is a gap and what must have been repaired.
     *
     * @return whether a scan should be done soon or could wait
     */
    public int scan(final long completedPosition, final long hwmPosition)
    {
        if (completedPosition >= hwmPosition)
        {
            if (timer.isActive())
            {
                timer.cancel();
            }

            return 0;
        }

        final int partitionCompleted = mask & (int)completedPosition;
        final int partitionHwm = mask & (int)hwmPosition;
        final int completedTerms = (int)(completedPosition >>> positionBitsToShift);
        final int hwmTerms = (int)(hwmPosition >>> positionBitsToShift);

        final int activeIndex = partitionIndex(completedTerms);
        final int activeTermId = initialTermId + completedTerms;

        final UnsafeBuffer termBuffer = termBuffers[activeIndex];
        final int activePartitionHwm = (completedTerms == hwmTerms) ? partitionHwm : partitionCompleted;
        final int numGaps = GapScanner.scan(termBuffer, activeTermId, partitionCompleted, activePartitionHwm, onGapFunc);

        if (numGaps > 0)
        {
            final Gap gap = scannedGap;
            if (!timer.isActive() || !gap.matches(activeGap.termId, activeGap.termOffset))
            {
                activateGap(gap.termId, gap.termOffset, gap.length);
            }

            return 0;
        }
        else if (!timer.isActive() || !activeGap.matches(activeTermId, partitionCompleted))
        {
            activateGap(activeTermId, partitionCompleted, (int)(hwmPosition - completedPosition));
        }

        return 1;
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

    private void suppressNak()
    {
        scheduleTimer();
    }

    private boolean onGap(final int termId, final UnsafeBuffer buffer, final int offset, final int length)
    {
        scannedGap.reset(termId, offset, length);

        return false;
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
        naksSent.orderedIncrement();
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

    static final class Gap
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
