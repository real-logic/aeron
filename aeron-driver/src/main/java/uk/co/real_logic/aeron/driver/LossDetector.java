/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

import uk.co.real_logic.agrona.TimerWheel;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;

import static uk.co.real_logic.aeron.logbuffer.TermGapScanner.GapHandler;
import static uk.co.real_logic.aeron.logbuffer.TermGapScanner.scanForGap;

/**
 * Detecting and handling of gaps in a stream
 * <p>
 * This detector only notifies a single NAK at a time.
 */
public class LossDetector
{
    private final FeedbackDelayGenerator delayGenerator;
    private final NakMessageSender nakMessageSender;
    private final TimerWheel.Timer timer;
    private final TimerWheel wheel;
    private final Gap scannedGap = new Gap();
    private final Gap activeGap = new Gap();
    private final GapHandler onGapFunc = this::onGap;
    private final Runnable onTimerExpireFunc = this::onTimerExpire;

    private int rebuildOffset = 0;

    /**
     * Create a loss handler for a channel.
     *
     * @param wheel            for timer management
     * @param delayGenerator   to use for delay determination
     * @param nakMessageSender to call when sending a NAK is indicated
     */
    public LossDetector(
        final TimerWheel wheel, final FeedbackDelayGenerator delayGenerator, final NakMessageSender nakMessageSender)
    {
        this.wheel = wheel;
        this.timer = wheel.newBlankTimer();
        this.delayGenerator = delayGenerator;
        this.nakMessageSender = nakMessageSender;
    }

    /**
     * Get the offset to which the term is rebuilt after a {@link #scan(UnsafeBuffer, long, long, int, int, int)}.
     *
     * @return the offset to which the term is rebuilt after a {@link #scan(UnsafeBuffer, long, long, int, int, int)}.
     */
    public int rebuildOffset()
    {
        return rebuildOffset;
    }

    /**
     * Scan for gaps and handle received data.
     * <p>
     * The handler keeps track from scan to scan what is a gap and what must have been repaired.
     *
     * @return the work count for this operation.
     */
    public int scan(
        final UnsafeBuffer termBuffer,
        final long rebuildPosition,
        final long hwmPosition,
        final int termLengthMask,
        final int positionBitsToShift,
        final int initialTermId)
    {
        int workCount = 1;

        final int rebuildTermOffset = (int)rebuildPosition & termLengthMask;
        final int hwmTermOffset = (int)hwmPosition & termLengthMask;

        if (rebuildPosition < hwmPosition)
        {
            final int rebuildTermsCount = (int)(rebuildPosition >>> positionBitsToShift);
            final int hwmTermsCount = (int)(hwmPosition >>> positionBitsToShift);

            final int activeTermId = initialTermId + rebuildTermsCount;
            final int activeTermLimit = (rebuildTermsCount == hwmTermsCount) ? hwmTermOffset : termBuffer.capacity();
            rebuildOffset = activeTermLimit;

            rebuildOffset = scanForGap(termBuffer, activeTermId, rebuildTermOffset, activeTermLimit, onGapFunc);
            if (rebuildOffset < activeTermLimit)
            {
                final Gap gap = scannedGap;
                if (!timer.isActive() || !gap.matches(activeGap.termId, activeGap.termOffset))
                {
                    activateGap(gap.termId, gap.termOffset, gap.length);
                    workCount = 0;
                }

                rebuildOffset = gap.termOffset;
            }
        }
        else
        {
            if (timer.isActive())
            {
                timer.cancel();
            }

            rebuildOffset = rebuildTermOffset;
            workCount = 0;
        }

        return workCount;
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

    private void onGap(final int termId, final UnsafeBuffer buffer, final int offset, final int length)
    {
        scannedGap.reset(termId, offset, length);
    }

    private void activateGap(final int termId, final int termOffset, final int length)
    {
        activeGap.reset(termId, termOffset, length);
        if (determineNakDelay() == -1)
        {
            return;
        }
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
        nakMessageSender.onLossDetected(activeGap.termId, activeGap.termOffset, activeGap.length);
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
