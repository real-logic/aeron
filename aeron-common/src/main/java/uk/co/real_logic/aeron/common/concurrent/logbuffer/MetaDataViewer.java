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
package uk.co.real_logic.aeron.common.concurrent.logbuffer;

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;

/**
 * View over the meta data for a term log buffer that can be read in a thread safe manner.
 */
public class MetaDataViewer
{
    private final UnsafeBuffer buffer;

    /**
     * Construct a viewer based on an underlying buffer.
     *
     * @param buffer containing the state variables.
     */
    public MetaDataViewer(final UnsafeBuffer buffer)
    {
        checkMetaDataBuffer(buffer);

        this.buffer = buffer;
    }

    /**
     * Get the current tail value in a volatile memory ordering fashion.
     *
     * @return the current tail value.
     */
    public int tailVolatile()
    {
        return buffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET);
    }

    /**
     * Get the current tail value.
     *
     * @return the current tail value.
     */
    public int tail()
    {
        return buffer.getInt(TERM_TAIL_COUNTER_OFFSET);
    }
}
