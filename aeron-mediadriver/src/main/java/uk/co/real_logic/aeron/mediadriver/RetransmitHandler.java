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
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;

import java.util.concurrent.TimeUnit;

/**
 * Tracking and handling of retransmit request, NAKs, for senders and receivers
 */
public class RetransmitHandler
{
    private static final FeedbackDelayGenerator feedbackDelay;

    private final LogReader reader;
    private final TimerWheel wheel;
    private final LogReader.FrameHandler sendRetransmitHandler;

    static
    {
        feedbackDelay = new FeedbackDelayGenerator(MediaDriver.RETRANS_MAX_BACKOFF_DEFAULT,
                                                   MediaDriver.RETRANS_GROUPSIZE_DEFAULT,
                                                   MediaDriver.RETRANS_GRTT_DEFAULT);
    }

    public RetransmitHandler(final LogReader reader,
                             final TimerWheel wheel,
                             final LogReader.FrameHandler retransmitHandler)
    {
        this.reader = reader;
        this.wheel = wheel;
        this.sendRetransmitHandler = retransmitHandler;
    }

    public void onNak(final int termOffset)
    {

    }

    public void onRetransmitReceived(final int termOffset)
    {

    }

    private long determineRetransmitDelay()
    {
        // TODO: will be 0 if this is the only retransmitter. Or will delay if not.
        return TimeUnit.MILLISECONDS.toNanos(20);
    }

    private long determineLingerTimeout()
    {
        return TimeUnit.MILLISECONDS.toNanos(10);
    }

    /*
     * Fixed number of retransmits being handled. Including ones in Linger.
     */

    public enum State
    {
        DELAY,
        LINGER,
        INACTIVE
    }

    public class RetransmitState
    {
        private int termOffset;

        public void reset(final int termOffset)
        {
            this.termOffset = termOffset;
        }
    }
}
