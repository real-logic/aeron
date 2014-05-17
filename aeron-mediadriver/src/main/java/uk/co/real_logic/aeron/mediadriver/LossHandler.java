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
 */
public class LossHandler
{
    private static final FeedbackDelayGenerator feedbackDelay;

    private final GapScanner[] scanners;
    private final TimerWheel wheel;
    private final GapState currentGap;
    private final SendNakHandler sendNakHandler;

    private TimerWheel.Timer timer;

    private int currentIndex = 0;
    private int numOutstanding = 0;

    private long nakSentTimestamp;

    private DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();

    /**
     * Handler for sending a NAK
     */
    public interface SendNakHandler
    {
        void onSendNak(final int termId, final int termOffset);
    }

    static
    {
        feedbackDelay = new FeedbackDelayGenerator(MediaDriver.NAK_MAX_BACKOFF_DEFAULT,
                                                   MediaDriver.NAK_GROUPSIZE_DEFAULT,
                                                   MediaDriver.NAK_GRTT_DEFAULT);
    }

    public LossHandler(final GapScanner[] scanners, final TimerWheel wheel, final SendNakHandler sendNakHandler)
    {
        this.scanners = scanners;
        this.wheel = wheel;
        this.sendNakHandler = sendNakHandler;
        this.nakSentTimestamp = wheel.now();
        this.currentGap = new GapState();
    }

    public void scan()
    {
        final int gaps = scanners[currentIndex].scan(this::onGap);
        onScanComplete(gaps);

        // TODO: determine if the buffer is complete and we need to rotate currentIndex
        // if (0 == gaps && ... )
    }

    public void onNak(final int termId, final int termOffset)
    {
        if (numOutstanding > 0 && currentGap.isFor(termId, termOffset))
        {
            // suppress sending NAK if it matches what we are waiting on
            nakSentTimestamp = wheel.now();
            if (timer.isActive())
            {
                timer.cancel();
                scheduleTimer(determineNakDelay());
            }
        }
    }

    private void onGap(final AtomicBuffer buffer, final int offset, final int length)
    {
        dataHeader.wrap(buffer, offset);

        if (numOutstanding > 0)
        {
            // re-verify gap is still present or we've passed over it (i.e. it was filled)
        }
        else
        {
            // no existing Gaps
            // grab termId from the actual buffer
            currentGap.reset((int)dataHeader.termId(), offset);
            numOutstanding++;
            scheduleTimer(determineNakDelay());
            nakSentTimestamp = wheel.now();
        }
    }

    private void onScanComplete(final int gaps)
    {

    }

    private void onTimerExpire()
    {
        sendNakHandler.onSendNak(currentGap.termId, currentGap.termOffset);
        scheduleTimer(determineNakDelay());
    }

    private long determineNakDelay()
    {
        /*
         * TODO: This should be 0 for unicast and use FeedbackDelayGenerator for multicast situations.
         */
        return TimeUnit.MILLISECONDS.toNanos(20);
    }

    private void scheduleTimer(final long delay)
    {
        if (null == timer)
        {
            timer = wheel.newTimeout(delay, TimeUnit.NANOSECONDS, this::onTimerExpire);
        }
        else
        {
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
