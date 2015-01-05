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
 * Base log buffer implementation containing common functionality.
 */
public class LogBuffer
{
    private final UnsafeBuffer termBuffer;
    private final UnsafeBuffer termStateBuffer;
    private final int capacity;

    protected LogBuffer(final UnsafeBuffer termBuffer, final UnsafeBuffer termStateBuffer)
    {
        checkTermBuffer(termBuffer);
        checkTermStateBuffer(termStateBuffer);

        this.termBuffer = termBuffer;
        this.termStateBuffer = termStateBuffer;
        this.capacity = termBuffer.capacity();
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
     * The state describing the term.
     *
     * @return the state describing the term.
     */
    public UnsafeBuffer termStateBuffer()
    {
        return termStateBuffer;
    }

    /**
     * The capacity of the underlying log buffer.
     *
     * @return the capacity of the underlying log buffer.
     */
    public int capacity()
    {
        return capacity;
    }

    /**
     * Clean down the buffers for reuse by zeroing them out.
     */
    public void clean()
    {
        termBuffer.setMemory(0, termBuffer.capacity(), (byte)0);
        termStateBuffer.setMemory(0, termStateBuffer.capacity(), (byte)0);
        statusOrdered(CLEAN);
    }

    /**
     * What is the current status of the buffer.
     *
     * @return the status of buffer as described in {@link LogBufferDescriptor}
     */
    public int status()
    {
        return termStateBuffer.getIntVolatile(STATUS_OFFSET);
    }

    /**
     * Set the status of the log buffer with StoreStore memory ordering semantics.
     *
     * @param status to be set for the log buffer.
     */
    public void statusOrdered(final int status)
    {
        termStateBuffer.putIntOrdered(STATUS_OFFSET, status);
    }

    /**
     * Get the current tail value in a volatile memory ordering fashion.
     *
     * @return the current tail value.
     */
    public int tailVolatile()
    {
        return Math.min(termStateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET), capacity);
    }

    /**
     * Get the current high-water-mark value in a volatile memory ordering fashion.
     *
     * @return the current high-water-mark value.
     */
    public int highWaterMarkVolatile()
    {
        return termStateBuffer.getIntVolatile(HIGH_WATER_MARK_OFFSET);
    }

    /**
     * Get the current tail value.
     *
     * @return the current tail value.
     */
    public int tail()
    {
        return Math.min(termStateBuffer.getInt(TAIL_COUNTER_OFFSET), capacity);
    }

    /**
     * Get the current high-water-mark value.
     *
     * @return the current high-water-mark value.
     */
    public int highWaterMark()
    {
        return termStateBuffer.getInt(HIGH_WATER_MARK_OFFSET);
    }
}
