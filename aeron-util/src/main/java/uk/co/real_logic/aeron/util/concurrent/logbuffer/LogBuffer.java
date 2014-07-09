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
package uk.co.real_logic.aeron.util.concurrent.logbuffer;

import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.STATUS_OFFSET;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.checkLogBuffer;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.checkStateBuffer;

/**
 * Base log buffer implementation containing common functionality.
 */
public class LogBuffer
{
    private final AtomicBuffer logBuffer;
    private final AtomicBuffer stateBuffer;
    private final int capacity;

    protected LogBuffer(final AtomicBuffer logBuffer, final AtomicBuffer stateBuffer)
    {
        checkLogBuffer(logBuffer);
        checkStateBuffer(stateBuffer);

        this.logBuffer = logBuffer;
        this.stateBuffer = stateBuffer;
        this.capacity = logBuffer.capacity();
    }

    /**
     * The log of messages.
     *
     * @return the log of messages.
     */
    public AtomicBuffer logBuffer()
    {
        return logBuffer;
    }

    /**
     * The state describing the log.
     *
     * @return the state describing the log.
     */
    public AtomicBuffer stateBuffer()
    {
        return stateBuffer;
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
        logBuffer.setMemory(0, logBuffer.capacity(), (byte)0);
        stateBuffer.setMemory(0, stateBuffer.capacity(), (byte)0);
    }

    /**
     * What is the current status of the buffer.
     *
     * @return the status of buffer as described in {@link LogBufferDescriptor}
     */
    public int status()
    {
        return stateBuffer.getIntVolatile(STATUS_OFFSET);
    }

    /**
     * Atomically compare and set the status to updateStatus if it is in expectedStatus.
     *
     * @param expectedStatus as a conditional guard.
     * @param updateStatus to be applied if conditional guard is meet.
     * @return true if successful otherwise false.
     */
    public boolean compareAndSetStatus(final int expectedStatus, final int updateStatus)
    {
        return stateBuffer.compareAndSetInt(STATUS_OFFSET, expectedStatus, updateStatus);
    }

    /**
     * Set the status of the log buffer with StoreStore memory ordering semantics.
     *
     * @param status to be set for the log buffer.
     */
    public void statusOrdered(final int status)
    {
        stateBuffer.putIntOrdered(STATUS_OFFSET, status);
    }
}
