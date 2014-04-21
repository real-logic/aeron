/*
 * Copyright 2013 Real Logic Ltd.
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
package uk.co.real_logic.aeron.util.concurrent.logbuffer;

import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import static uk.co.real_logic.aeron.util.concurrent.logbuffer.BufferDescriptor.*;

/**
 * View over the state for a log buffer that can be read in a thread safe manner.
 */
public class StateViewer
{
    private final AtomicBuffer buffer;

    /**
     * Construct a viewer based on an underlying buffer.
     *
     * @param buffer containing the state variables.
     */
    public StateViewer(final AtomicBuffer buffer)
    {
        checkStateBuffer(buffer);

        this.buffer = buffer;
    }

    /**
     * Get the current tail value in a volatile memory ordering fashion.
     *
     * @return the current tail value.
     */
    public int tailVolatile()
    {
        return buffer.getIntVolatile(TAIL_COUNTER_OFFSET);
    }

    /**
     * Get the current high-water-mark value in a volatile memory ordering fashion.
     *
     * @return the current high-water-mark value.
     */
    public int highWaterMarkVolatile()
    {
        return buffer.getIntVolatile(HIGH_WATER_MARK_OFFSET);
    }

    /**
     * Get the current tail value.
     *
     * @return the current tail value.
     */
    public int tail()
    {
        return buffer.getInt(TAIL_COUNTER_OFFSET);
    }

    /**
     * Get the current high-water-mark value.
     *
     * @return the current high-water-mark value.
     */
    public int highWaterMark()
    {
        return buffer.getInt(HIGH_WATER_MARK_OFFSET);
    }

}
