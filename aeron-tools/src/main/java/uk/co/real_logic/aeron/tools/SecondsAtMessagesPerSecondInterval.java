/*
 * Copyright 2015 Kaazing Corporation
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
package uk.co.real_logic.aeron.tools;

import uk.co.real_logic.aeron.tools.RateController.IntervalInternal;

public class SecondsAtMessagesPerSecondInterval extends RateControllerInterval
{
    /* The rate we _want_ to achieve, if possible.  Might not be able
     * to hit it exactly due to receiver pacing, etc.  But it's what
     * we're aiming for. */
    private final double goalMessagesPerSecond;
    /* Number of seconds (can be fractional) to run for, in total. */
    private final double seconds;

    public SecondsAtMessagesPerSecondInterval(final double seconds, final double messagesPerSecond)
    {
        this.goalMessagesPerSecond = messagesPerSecond;
        this.seconds = seconds;
    }

    public double messagesPerSecond()
    {
        return goalMessagesPerSecond;
    }

    public double seconds()
    {
        return seconds;
    }

    IntervalInternal makeInternal(final RateController rateController) throws Exception
    {
        return rateController.new SecondsAtMessagesPerSecondInternal(rateController, seconds, goalMessagesPerSecond);
    }
}
