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
package uk.co.real_logic.aeron.common;

/**
 * Delay generator that simply returns a constant value (such as 0)
 */
public class StaticDelayGenerator implements FeedbackDelayGenerator
{
    private final long delay;
    private final boolean immediateFeedback;

    /**
     * Create a delay generator that uses the specified delay.
     *
     * @param delay to return
     */
    public StaticDelayGenerator(final long delay, final boolean immediateFeedback)
    {
        this.delay = delay;
        this.immediateFeedback = immediateFeedback;
    }

    /** {@inheritDoc} */
    public long generateDelay()
    {
        return delay;
    }

    /** {@inheritDoc} */
    public boolean immediateFeedback()
    {
        return immediateFeedback;
    }
}
