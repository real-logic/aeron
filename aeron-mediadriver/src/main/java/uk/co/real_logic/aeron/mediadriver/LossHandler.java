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
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;

import java.util.concurrent.TimeUnit;

/**
 * Tracking and handling of gaps in a channel
 *
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
         * @param termId for the NAK
         * @param termOffset for the NAK
         */
        void onSendNak(final int termId, final int termOffset);
    }

    private final GapScanner[] scanners;
    private final TimerWheel wheel;
    private final GapState[] scanGaps = new GapState[2];
    private final GapState activeGap= new GapState();
    private final FeedbackDelayGenerator delayGenerator;
    private final SendNakHandler sendNakHandler;

    private TimerWheel.Timer timer;

    private int currentIndex = 0;
    private int scanCursor = 0;

    private long nakSentTimestamp;

    private DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();

    /**
     * Create a loss handler for a channel.
     *
     * @param scanners for the gaps attached to the {@link uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader}
     * @param wheel for timer management
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

        for (int i = 0, max = scanGaps.length; i < max; i++)
        {
            this.scanGaps[i] = new GapState();
        }
    }

    /**
     * Scan for gaps and handle received data.
     *
     * The handler keeps track from scan to scan what is a gap and what must have been repaired.
     */
    public void scan()
    {
        scanCursor = 0;

        scanners[currentIndex].scan(this::onGap);
        onScanComplete();

        // TODO: determine if the buffer is complete and we need to rotate currentIndex for next scanner
        // if (0 == gaps && ... )
    }

    /**
     * Called on reception of a NAK
     *
     * @param termId in the NAK
     * @param termOffset in the NAK
     */
    public void onNak(final int termId, final int termOffset)
    {
        if (null != timer && timer.isActive() && activeGap.isFor(termId, termOffset))
        {
            // suppress sending NAK if it matches what we are waiting on
            nakSentTimestamp = wheel.now();
            scheduleTimer();
        }
    }

    private boolean onGap(final AtomicBuffer buffer, final int offset, final int length)
    {
        // grab termId from the actual buffer
        dataHeader.wrap(buffer, offset);

        if (scanCursor < scanGaps.length)
        {
            scanGaps[scanCursor].reset((int) dataHeader.termId(), offset);

            scanCursor++;

            return (scanCursor == scanGaps.length);
        }

        return false;
    }

    private void onScanComplete()
    {
        // if no active gap
        if (null == timer || !timer.isActive())
        {
            activeGap.reset(scanGaps[0].termId, scanGaps[0].termOffset);
            scheduleTimer();
            nakSentTimestamp = wheel.now();
        }
        else if (scanCursor == 0)
        {
            timer.cancel();
        }
        else
        {
            // replace old gap with new gap and reschedule
            activeGap.reset(scanGaps[0].termId, scanGaps[0].termOffset);
            scheduleTimer();
            nakSentTimestamp = wheel.now();
        }
    }

    private void onTimerExpire()
    {
        sendNakHandler.onSendNak(activeGap.termId, activeGap.termOffset);
        scheduleTimer();
        nakSentTimestamp = wheel.now();
    }

    private long determineNakDelay()
    {
        // this should be 0 for unicast and use OptimalMcastDelayGenerator for multicast situations.
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

    public class GapState
    {
        private int termId;
        private int termOffset;

        public void reset(final int termId, final int termOffset)
        {
            this.termId = termId;
            this.termOffset = termOffset;
        }

        public boolean isFor(final int termId, final int termOffset)
        {
            if (termId == this.termId && termOffset == this.termOffset)
            {
                return true;
            }

            return false;
        }
    }
}
