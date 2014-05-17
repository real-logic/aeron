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

/**
 */
public class RetransmitController
{
    private static final FeedbackDelayGenerator feedbackDelay;

    private final LogReader reader;
    private final TimerWheel wheel;

    static
    {
        feedbackDelay = new FeedbackDelayGenerator(MediaDriver.RETRANS_MAX_BACKOFF_DEFAULT,
                                                   MediaDriver.RETRANS_GROUPSIZE_DEFAULT,
                                                   MediaDriver.RETRANS_GRTT_DEFAULT);
    }

    public RetransmitController(final LogReader reader, final TimerWheel wheel)
    {
        this.reader = reader;
        this.wheel = wheel;
    }

    public void onNak(final int termOffset)
    {

    }
}
