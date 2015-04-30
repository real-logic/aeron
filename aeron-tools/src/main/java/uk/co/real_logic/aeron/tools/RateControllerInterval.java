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

public abstract class RateControllerInterval
{
    /* Is this interval currently active/running? */
    protected boolean active;

    /* Number of bits sent so far during this interval. */
    protected long bitsSent;
    /* Number of messages sent so far during this interval. */
    protected long messagesSent;

    /* Begin and end timestamps for last time this interval was active. */
    protected long beginTimeNanos;
    protected long endTimeNanos;

    public long messagesSent()
    {
        return messagesSent;
    }

    public long bytesSent()
    {
        return (bitsSent / 8);
    }

    public long startTimeNanos()
    {
        return beginTimeNanos;
    }

    public long stopTimeNanos()
    {
        return endTimeNanos;
    }

    abstract RateController.IntervalInternal makeInternal(RateController rateController) throws Exception;
}
