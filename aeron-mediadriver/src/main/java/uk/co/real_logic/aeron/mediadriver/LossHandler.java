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
package uk.co.real_logic.aeron.mediadriver;

import uk.co.real_logic.aeron.util.FeedbackDelayGenerator;
import uk.co.real_logic.aeron.util.TimerWheel;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.GapScanner;

import java.util.concurrent.TimeUnit;

/**
 * Tracking and handling of gaps in a channel
 * <p>
 * This handler only sends a single NAK at a time.
 */
public class LossHandler
{
    /**
     * Handler for sending a NAK
     */
    @FunctionalInterface
    public interface SendNakHandler
    {
        /**
         * Called when a NAK should be sent
         *
         * @param termId     for the NAK
         * @param termOffset for the NAK
         * @param length     for the NAK
         */
        void onSendNak(final long termId, final int termOffset, final int length);
    }

    private final GapScanner[] scanners;
    private final TimerWheel wheel;
    private final Gap[] gaps = new Gap[2];
    private final Gap activeGap = new Gap();
    private final FeedbackDelayGenerator delayGenerator;

    private SendNakHandler sendNakHandler;
    private TimerWheel.Timer timer;

    private int activeIndex = 0;
    private int scanCursor = 0;
    private long activeTermId;

    private long nakSentTimestamp;

    /**
     * Create a loss handler for a channel with no send NAK handler.
     *
     * @param scanners       for the gaps attached to LogBuffers
     * @param wheel          for timer management
     * @param delayGenerator to use for delay determination
     */
    public LossHandler(final GapScanner[] scanners,
                       final TimerWheel wheel,
                       final FeedbackDelayGenerator delayGenerator)
    {
        this(scanners, wheel, delayGenerator, null);
    }

    /**
     * Create a loss handler for a channel.
     *
     * @param scanners       for the gaps attached to LogBuffers
     * @param wheel          for timer management
     * @param delayGenerator to use for delay determination
     * @param sendNakHandler to call when sending a NAK is indicated
     */
    public LossHandler(final GapScanner[] scanners,
                       final TimerWheel wheel,
                       final FeedbackDelayGenerator delayGenerator,
                       final SendNakHandler sendNakHandler)
    {
        this.scanners = scanners;
        this.wheel = wheel;
        this.delayGenerator = delayGenerator;
        this.sendNakHandler = sendNakHandler;
        this.nakSentTimestamp = wheel.now();

        for (int i = 0, max = gaps.length; i < max; i++)
        {
            this.gaps[i] = new Gap();
        }

        this.activeIndex = 0;
    }

    /**
     * Set send NAK handler.
     *
     * @param handler to call when NAK is to be sent
     */
    public void sendNakHandler(final SendNakHandler handler)
    {
        this.sendNakHandler = handler;
    }

    /**
     * Set active Term Id for the active buffer
     *
     * @param termId for the active buffer
     */
    public void activeTermId(final long termId)
    {
        this.activeTermId = termId;
    }

    /**
     * Scan for gaps and handle received data.
     * <p>
     * The handler keeps track from scan to scan what is a gap and what must have been repaired.
     */
    public void scan()
    {
        scanCursor = 0;

        scanners[activeIndex].scan(this::onGap);
        onScanComplete();

        // TODO: determine if the buffer is complete and we need to rotate activeIndex for next scanner
        // if (0 == gaps && ... )
    }

    /**
     * Called on reception of a NAK
     *
     * @param termId     in the NAK
     * @param termOffset in the NAK
     */
    public void onNak(final long termId, final int termOffset)
    {
        if (null != timer && timer.isActive() && activeGap.matches(termId, termOffset))
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
    public long activeTermId()
    {
        return activeTermId;
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
        if (null == timer || !timer.isActive())
        {
            if (scanCursor > 0)
            {
                activeGap.reset(firstGap.termId, firstGap.termOffset, firstGap.length);
                scheduleTimer();
                nakSentTimestamp = wheel.now();

                if (delayGenerator.immediateFeedback())
                {
                    sendNakHandler.onSendNak(activeGap.termId, activeGap.termOffset, activeGap.length);
                }
            }
        }
        else if (scanCursor == 0)
        {
            timer.cancel();
        }
        else if (!firstGap.matches(activeGap.termId, activeGap.termOffset))
        {
            activeGap.reset(firstGap.termId, firstGap.termOffset, firstGap.length);
            scheduleTimer();
            nakSentTimestamp = wheel.now();

            if (delayGenerator.immediateFeedback())
            {
                sendNakHandler.onSendNak(activeGap.termId, activeGap.termOffset, activeGap.length);
            }
        }
    }

    private void onTimerExpire()
    {
        sendNakHandler.onSendNak(activeGap.termId, activeGap.termOffset, activeGap.length);
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

        if (null == timer)
        {
            timer = wheel.newTimeout(delay, TimeUnit.NANOSECONDS, this::onTimerExpire);
        }
        else
        {
            if (timer.isActive())
            {
                timer.cancel();
            }

            wheel.rescheduleTimeout(delay, TimeUnit.NANOSECONDS, timer);
        }
    }

    private static class Gap
    {
        private long termId;
        private int termOffset;
        private int length;

        public void reset(final long termId, final int termOffset, final int length)
        {
            this.termId = termId;
            this.termOffset = termOffset;
            this.length = length;
        }

        public boolean matches(final long termId, final int termOffset)
        {
            return termId == this.termId && termOffset == this.termOffset;
        }
    }
}
