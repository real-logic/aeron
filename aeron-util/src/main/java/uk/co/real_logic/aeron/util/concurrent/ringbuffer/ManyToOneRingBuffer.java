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
package uk.co.real_logic.aeron.util.concurrent.ringbuffer;

import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.EventHandler;

import static uk.co.real_logic.aeron.util.BitUtil.align;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RecordDescriptor.*;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor.*;

/**
 * A ring-buffer that supports the exchange of events from many producers to a single consumer.
 */
public class ManyToOneRingBuffer implements RingBuffer
{
    /**
     * Event type is padding to prevent fragmentation in the buffer.
     */
    public static final int PADDING_EVENT_TYPE_ID = -1;

    /**
     * Buffer has insufficient capacity to record an event.
     */
    public static final int INSUFFICIENT_CAPACITY = -1;

    private final AtomicBuffer buffer;
    private final int capacity;
    private final int mask;
    private final int maxEventSize;
    private final int tailCounterIndex;
    private final int headCounterIndex;
    private final int correlationIdCounterIndex;

    /**
     * Construct a new {@link RingBuffer} based on an underlying {@link AtomicBuffer}.
     * The underlying buffer must a power of 2 in size plus sufficient space
     * for the {@link RingBufferDescriptor#TRAILER_SIZE}.
     *
     * @param buffer via which events will be exchanged.
     * @throws IllegalStateException if the buffer capacity is not a power of 2
     *                               plus {@link RingBufferDescriptor#TRAILER_SIZE} in capacity.
     */
    public ManyToOneRingBuffer(final AtomicBuffer buffer)
    {
        this.buffer = buffer;
        capacity = buffer.capacity() - RingBufferDescriptor.TRAILER_SIZE;

        if (capacity < 1 || 1 != Integer.bitCount(capacity))
        {
            final String msg =
                "Capacity must be a positive power of 2 + TRAILER_SIZE: AtomicBuffer.capacity=" + capacity;
            throw new IllegalStateException(msg);
        }

        mask = capacity - 1;
        maxEventSize = capacity / 4;
        tailCounterIndex = capacity + TAIL_COUNTER_OFFSET;
        headCounterIndex = capacity + HEAD_COUNTER_OFFSET;
        correlationIdCounterIndex = capacity + CORRELATION_COUNTER_OFFSET;
    }

    /**
     * {@inheritDoc}
     */
    public int capacity()
    {
        return capacity;
    }

    /**
     * {@inheritDoc}
     */
    public boolean write(final int eventTypeId, final AtomicBuffer srcBuffer, final int srcIndex, final int length)
    {
        checkEventTypeId(eventTypeId);
        checkEventSize(length);

        final int requiredCapacity = align(length + RECORD_HEADER_SIZE, ALIGNMENT);
        final int ringBufferIndex = claimCapacity(requiredCapacity);
        if (INSUFFICIENT_CAPACITY == ringBufferIndex)
        {
            return false;
        }

        writeEventLength(ringBufferIndex, length);
        writeEventType(ringBufferIndex, eventTypeId);
        writeEvent(ringBufferIndex, srcBuffer, srcIndex, length);

        writeRecordLengthOrdered(ringBufferIndex, requiredCapacity);

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public int read(final EventHandler handler)
    {
        return read(handler, Integer.MAX_VALUE);
    }

    /**
     * {@inheritDoc}
     */
    public int read(final EventHandler handler, final int maxEvents)
    {
        final long tail = getTailVolatile();
        final long head = getHeadVolatile();
        final int available = (int)(tail - head);
        int recordsRead = 0;

        if (available > 0)
        {
            final int headIndex = (int)head & mask;
            final int contiguousBlockSize = Math.min(available, capacity - headIndex);
            int bytesRead = 0;

            try
            {
                while ((bytesRead < contiguousBlockSize) && (recordsRead <= maxEvents))
                {
                    final int recordIndex = headIndex + bytesRead;
                    final int recordLength = waitForRecordLengthOrdered(recordIndex);

                    final int eventLength = getEventLength(recordIndex);
                    final int eventTypeId = getEventType(recordIndex);

                    bytesRead += recordLength;

                    if (eventTypeId != PADDING_EVENT_TYPE_ID)
                    {
                        ++recordsRead;
                        handler.onEvent(eventTypeId, buffer, encodedEventOffset(recordIndex), eventLength);
                    }
                }
            }
            finally
            {
                zeroBuffer(headIndex, bytesRead);
                putHeadOrdered(head + bytesRead);
            }
        }

        return recordsRead;
    }

    /**
     * {@inheritDoc}
     */
    public int maxEventSize()
    {
        return maxEventSize;
    }

    /**
     * {@inheritDoc}
     */
    public long nextCorrelationId()
    {
        return buffer.getAndAddLong(correlationIdCounterIndex, 1);
    }

    private void checkEventTypeId(final int eventTypeId)
    {
        if (eventTypeId < 1)
        {
            final String msg = String.format("event type id must be greater than zero, eventTypeId=%d",
                                             Integer.valueOf(eventTypeId));

            throw new IllegalArgumentException(msg);
        }
    }

    private void checkEventSize(final int length)
    {
        if (length > maxEventSize)
        {
            final String msg = String.format("encoded event exceeds maxEventSize of %d, length=%d",
                                             Integer.valueOf(maxEventSize), Integer.valueOf(length));

            throw new IllegalArgumentException(msg);
        }
    }

    private int claimCapacity(final int requiredCapacity)
    {
        final long head = getHeadVolatile();
        final int headIndex = (int)head & mask;

        long tail;
        int tailIndex;
        int padding;
        do
        {
            tail = getTailVolatile();

            final int availableCapacity = capacity - (int)(tail - head);
            if (requiredCapacity > availableCapacity)
            {
                return INSUFFICIENT_CAPACITY;
            }

            padding = 0;
            tailIndex = (int)tail & mask;

            if (tailIndex > headIndex)
            {
                final int bufferEndSize = capacity - tailIndex;
                if (requiredCapacity > bufferEndSize)
                {
                    if (requiredCapacity > headIndex)
                    {
                        return INSUFFICIENT_CAPACITY;
                    }

                    padding = bufferEndSize;
                }
            }
        }
        while (!buffer.compareAndSetLong(tailCounterIndex, tail, tail + requiredCapacity + padding));

        if (0 != padding)
        {
            writePaddingRecord(tailIndex, padding);
            tailIndex = 0;
        }

        return tailIndex;
    }

    private long getTailVolatile()
    {
        return buffer.getLongVolatile(tailCounterIndex);
    }

    private long getHeadVolatile()
    {
        return buffer.getLongVolatile(headCounterIndex);
    }

    private void putHeadOrdered(final long value)
    {
        buffer.putLongOrdered(headCounterIndex, value);
    }

    private void writePaddingRecord(final int recordIndex, final int padding)
    {
        writeEventType(recordIndex, PADDING_EVENT_TYPE_ID);
        writeRecordLengthOrdered(recordIndex, padding);
    }

    private void writeRecordLengthOrdered(final int recordIndex, final int length)
    {
        buffer.putIntOrdered(lengthOffset(recordIndex), length);
    }

    private void writeEventLength(final int recordIndex, final int length)
    {
        buffer.putInt(eventLengthOffset(recordIndex), length);
    }

    private void writeEventType(final int recordIndex, final int eventTypeId)
    {
        buffer.putInt(eventTypeOffset(recordIndex), eventTypeId);
    }

    private void writeEvent(final int recordIndex, final AtomicBuffer srcBuffer, final int srcIndex, final int length)
    {
        buffer.putBytes(encodedEventOffset(recordIndex), srcBuffer, srcIndex, length);
    }

    private int getEventType(final int recordIndex)
    {
        return buffer.getInt(eventTypeOffset(recordIndex));
    }

    private int getEventLength(final int recordIndex)
    {
        return buffer.getInt(eventLengthOffset(recordIndex));
    }

    private int waitForRecordLengthOrdered(final int recordIndex)
    {
        int recordLength;

        do
        {
            recordLength = buffer.getIntVolatile(lengthOffset(recordIndex));
        }
        while (0 == recordLength);

        return recordLength;
    }

    private void zeroBuffer(final int position, int length)
    {
        buffer.setMemory(position, length, (byte)0);
    }
}
