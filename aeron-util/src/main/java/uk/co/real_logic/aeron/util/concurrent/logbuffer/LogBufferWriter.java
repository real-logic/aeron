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

import static java.lang.Integer.valueOf;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.RecordDescriptor.ALIGNMENT;

/**
 * Log buffer writer which supports many producer concurrently writing an append only log.
 */
public class LogBufferWriter
{
    /**
     * Buffer has insufficient capacity to record a message.
     */
    private static final int INSUFFICIENT_CAPACITY = -1;

    private final AtomicBuffer logBuffer;
    private final AtomicBuffer stateBuffer;
    private final int capacity;
    private final int maxMessageLength;

    /**
     * Construct a view over a log buffer and state buffer for its usage.
     *
     * @param logAtomicBuffer for where events are stored.
     * @param stateAtomicBuffer for where the state of writers is stored manage concurrency.
     */
    public LogBufferWriter(final AtomicBuffer logAtomicBuffer, final AtomicBuffer stateAtomicBuffer)
    {
        checkLogBuffer(logAtomicBuffer);
        checkStateBuffer(stateAtomicBuffer);

        this.logBuffer = logAtomicBuffer;
        this.stateBuffer = stateAtomicBuffer;
        capacity = logAtomicBuffer.capacity();
        maxMessageLength = capacity / 4;
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
     * The maximum length of a message that can be recorded in the log.
     *
     * @return the maximum length of a message that can be recorded in the log.
     */
    public int maxMessageLength()
    {
        return maxMessageLength;
    }

    /**
     * Get the current value of the tail counter for the log.
     *
     * @return the current value of the tail counter for the log.
     */
    public int tail()
    {
        final int tail = stateBuffer.getIntVolatile(STATE_TAIL_COUNTER_OFFSET);
        return Math.min(tail, capacity);
    }

    /**
     * Write a message into the log if sufficient capacity exists.
     *
     * @param srcBuffer containing the encoded message.
     * @param srcOffset at which the encoded message begins.
     * @param length of the message in bytes.
     * @return true if recorded in the log otherwise false if the capacity is exhausted.
     * @throws IllegalArgumentException if the
     */
    public boolean write(final AtomicBuffer srcBuffer, final int srcOffset, final int length)
    {
        checkMessageLength(length);

        return true;
    }

    private static void checkLogBuffer(final AtomicBuffer buffer)
    {
        final int capacity = buffer.capacity();
        if (capacity < LOG_MIN_SIZE)
        {
            final String s = String.format("Log buffer capacity less than min size of %d, capacity=%d",
                                           valueOf(LOG_MIN_SIZE), valueOf(capacity));
            throw new IllegalStateException(s);
        }

        if (capacity % ALIGNMENT != 0)
        {
            final String s = String.format("Log buffer capacity not a multiple of %d, capacity=%d",
                                           valueOf(ALIGNMENT), valueOf(capacity));
            throw new IllegalStateException(s);
        }
    }

    private static void checkStateBuffer(final AtomicBuffer buffer)
    {
        final int capacity = buffer.capacity();
        if (capacity < STATE_SIZE)
        {
            final String s = String.format("State buffer capacity less than min size of %d, capacity=%d",
                                           valueOf(STATE_SIZE), valueOf(capacity));
            throw new IllegalStateException(s);
        }
    }

    private void checkMessageLength(final int length)
    {
        if (length > maxMessageLength)
        {
            final String msg = String.format("encoded event exceeds maxMessageLength of %d, length=%d",
                                             Integer.valueOf(maxMessageLength), Integer.valueOf(length));

            throw new IllegalArgumentException(msg);
        }
    }
}
