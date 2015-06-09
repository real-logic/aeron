/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.logbuffer;

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET;


/**
 * Base log buffer implementation containing common functionality for dealing with fragment terms.
 */
public class LogBufferPartition
{
    private final UnsafeBuffer termBuffer;
    private final UnsafeBuffer metaDataBuffer;

    public LogBufferPartition(final UnsafeBuffer termBuffer, final UnsafeBuffer metaDataBuffer)
    {
        checkTermBuffer(termBuffer);
        checkMetaDataBuffer(metaDataBuffer);
        termBuffer.verifyAlignment();
        metaDataBuffer.verifyAlignment();

        this.termBuffer = termBuffer;
        this.metaDataBuffer = metaDataBuffer;
    }

    /**
     * The log of messages for a term.
     *
     * @return the log of messages for a term.
     */
    public UnsafeBuffer termBuffer()
    {
        return termBuffer;
    }

    /**
     * The meta data describing the term.
     *
     * @return the meta data describing the term.
     */
    public UnsafeBuffer metaDataBuffer()
    {
        return metaDataBuffer;
    }


    /**
     * Clean down the buffers for reuse by zeroing them out.
     */
    public void clean()
    {
        termBuffer.setMemory(0, termBuffer.capacity(), (byte)0);

        metaDataBuffer.putInt(TERM_TAIL_COUNTER_OFFSET, 0);
        metaDataBuffer.putIntOrdered(TERM_STATUS_OFFSET, CLEAN);
    }

    /**
     * What is the current status of the buffer.
     *
     * @return the status of buffer as described in {@link LogBufferDescriptor}
     */
    public int status()
    {
        return metaDataBuffer.getIntVolatile(TERM_STATUS_OFFSET);
    }

    /**
     * Set the status of the log buffer with StoreStore memory ordering semantics.
     *
     * @param status to be set for the log buffer.
     */
    public void statusOrdered(final int status)
    {
        metaDataBuffer.putIntOrdered(TERM_STATUS_OFFSET, status);
    }

    /**
     * Get the current tail value in a volatile memory ordering fashion. If raw tail is greater than
     * {@link #termBuffer()}.{@link uk.co.real_logic.agrona.DirectBuffer#capacity()} then capacity will be returned.
     *
     * @return the current tail value.
     */
    public int tailVolatile()
    {
        return Math.min(metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET), termBuffer.capacity());
    }

    /**
     * Get the raw value current tail value in a volatile memory ordering fashion.
     *
     * @return the current tail value.
     */
    public int rawTailVolatile()
    {
        return metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET);
    }

    /**
     * Get the current tail value.
     *
     * @return the current tail value.
     */
    public int tail()
    {
        return Math.min(metaDataBuffer.getInt(TERM_TAIL_COUNTER_OFFSET), termBuffer.capacity());
    }
}
