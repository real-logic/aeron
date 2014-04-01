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

import static uk.co.real_logic.aeron.util.BitUtil.align;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.RecordDescriptor.*;

/**
 * Log buffer appender which supports many producers concurrently writing an append-only log.
 */
public class LogBufferAppender
{
    private final AtomicBuffer logBuffer;
    private final AtomicBuffer stateBuffer;
    private final int capacity;
    private final int recordHeaderLength;
    private final int recordLengthFieldOffset;
    private final int maxMessageLength;

    /**
     * Construct a view over a log buffer and state buffer for appending records.
     *
     * @param logAtomicBuffer for where events are stored.
     * @param stateAtomicBuffer for where the state of writers is stored manage concurrency.
     * @param recordHeaderLength length of the header field to be used on each record.
     * @param recordLengthFieldOffset the offset in the header at which the record length field begins.
     */
    public LogBufferAppender(final AtomicBuffer logAtomicBuffer,
                             final AtomicBuffer stateAtomicBuffer,
                             final int recordHeaderLength,
                             final int recordLengthFieldOffset)
    {
        checkLogBuffer(logAtomicBuffer);
        checkStateBuffer(stateAtomicBuffer);
        checkHeaderLength(recordHeaderLength);
        checkLengthFieldOffset(recordHeaderLength, recordLengthFieldOffset);

        this.logBuffer = logAtomicBuffer;
        this.stateBuffer = stateAtomicBuffer;
        this.capacity = logAtomicBuffer.capacity();
        this.recordHeaderLength = recordHeaderLength;
        this.recordLengthFieldOffset = recordLengthFieldOffset;
        this.maxMessageLength = RecordDescriptor.calculateMaxMessageLength(capacity);
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
        final int tail = stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET);
        return Math.min(tail, capacity);
    }

    /**
     * Append a message to the log if sufficient capacity exists.
     *
     * @param srcBuffer containing the encoded message.
     * @param srcOffset at which the encoded message begins.
     * @param length of the message in bytes.
     * @return true if appended in the log otherwise false if the capacity is exhausted.
     * @throws IllegalArgumentException if the length is greater than {@link #maxMessageLength()}
     */
    public boolean append(final AtomicBuffer srcBuffer, final int srcOffset, final int length)
    {
        checkMessageLength(length);

        final int requiredCapacity = align(length + recordHeaderLength, RECORD_ALIGNMENT);
        final int recordOffset = getTailAndAdd(requiredCapacity);

        if (recordOffset >= capacity)
        {
            return false;
        }

        if (recordOffset + requiredCapacity > capacity)
        {
            appendPaddingRecord(recordOffset);

            return false;
        }

        appendRecord(recordOffset, requiredCapacity, srcBuffer, srcOffset, length);

        return true;
    }

    private void appendRecord(final int recordOffset,
                              final int requiredCapacity,
                              final AtomicBuffer srcBuffer,
                              final int srcOffset,
                              final int length)
    {
        logBuffer.putBytes(recordOffset + recordHeaderLength, srcBuffer, srcOffset, length);
        putRecordLengthOrdered(recordOffset, requiredCapacity);
    }

    private void appendPaddingRecord(final int recordOffset)
    {
        putRecordLengthOrdered(recordOffset, capacity - recordOffset);
    }

    private void putRecordLengthOrdered(final int recordOffset, final int length)
    {
        logBuffer.putIntOrdered(recordOffset + recordLengthFieldOffset, length);
    }

    private int getTailAndAdd(final int delta)
    {
        return stateBuffer.getAndAddInt(TAIL_COUNTER_OFFSET, delta);
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
