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
package uk.co.real_logic.agrona.concurrent.ringbuffer;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.MessageHandler;

import static uk.co.real_logic.agrona.BitUtil.align;

/**
 * A ring-buffer that supports the exchange of messages from many producers to a single consumer.
 */
public class ManyToOneRingBuffer implements RingBuffer
{
    /**
     * Record type is padding to prevent fragmentation in the buffer.
     */
    public static final int PADDING_MSG_TYPE_ID = -1;

    /**
     * Buffer has insufficient capacity to record a message.
     */
    public static final int INSUFFICIENT_CAPACITY = -1;

    private final AtomicBuffer buffer;
    private final int capacity;
    private final int mask;
    private final int maxMsgLength;
    private final int tailCounterIndex;
    private final int headCounterIndex;
    private final int correlationIdCounterIndex;
    private final int consumerHeartbeatIndex;

    /**
     * Construct a new {@link RingBuffer} based on an underlying {@link AtomicBuffer}.
     * The underlying buffer must a power of 2 in size plus sufficient space
     * for the {@link RingBufferDescriptor#TRAILER_LENGTH}.
     *
     * @param buffer via which events will be exchanged.
     * @throws IllegalStateException if the buffer capacity is not a power of 2
     *                               plus {@link RingBufferDescriptor#TRAILER_LENGTH} in capacity.
     */
    public ManyToOneRingBuffer(final AtomicBuffer buffer)
    {
        this.buffer = buffer;
        capacity = buffer.capacity() - RingBufferDescriptor.TRAILER_LENGTH;

        RingBufferDescriptor.checkCapacity(capacity);

        mask = capacity - 1;
        maxMsgLength = capacity / 8;
        tailCounterIndex = capacity + RingBufferDescriptor.TAIL_COUNTER_OFFSET;
        headCounterIndex = capacity + RingBufferDescriptor.HEAD_COUNTER_OFFSET;
        correlationIdCounterIndex = capacity + RingBufferDescriptor.CORRELATION_COUNTER_OFFSET;
        consumerHeartbeatIndex = capacity + RingBufferDescriptor.CONSUMER_HEARTBEAT_OFFSET;
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
    public boolean write(final int msgTypeId, final DirectBuffer srcBuffer, final int srcIndex, final int length)
    {
        RecordDescriptor.checkMsgTypeId(msgTypeId);
        checkMsgLength(length);

        final AtomicBuffer buffer = this.buffer;
        final int requiredCapacity = align(length + RecordDescriptor.HEADER_LENGTH, RecordDescriptor.ALIGNMENT);
        final int recordIndex = claimCapacity(buffer, requiredCapacity);
        if (INSUFFICIENT_CAPACITY == recordIndex)
        {
            return false;
        }

        msgLength(buffer, recordIndex, length);
        msgType(buffer, recordIndex, msgTypeId);
        writeMsg(buffer, recordIndex, srcBuffer, srcIndex, length);

        recordLengthOrdered(buffer, recordIndex, requiredCapacity);

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public int read(final MessageHandler handler)
    {
        return read(handler, Integer.MAX_VALUE);
    }

    /**
     * {@inheritDoc}
     */
    public int read(final MessageHandler handler, final int messageCountLimit)
    {
        final AtomicBuffer buffer = this.buffer;
        final long tail = tailVolatile(buffer);
        final long head = headVolatile(buffer);
        final int available = (int)(tail - head);
        int messagesRead = 0;

        if (available > 0)
        {
            final int headIndex = (int)head & mask;
            final int contiguousBlockSize = Math.min(available, capacity - headIndex);
            int bytesRead = 0;

            try
            {
                while ((bytesRead < contiguousBlockSize) && (messagesRead < messageCountLimit))
                {
                    final int recordIndex = headIndex + bytesRead;
                    final int recordLength = waitForRecordLengthVolatile(buffer, recordIndex);

                    final int msgLength = msgLength(buffer, recordIndex);
                    final int msgTypeId = msgType(buffer, recordIndex);

                    bytesRead += recordLength;

                    if (msgTypeId != PADDING_MSG_TYPE_ID)
                    {
                        ++messagesRead;
                        handler.onMessage(msgTypeId, buffer, RecordDescriptor.encodedMsgOffset(recordIndex), msgLength);
                    }
                }
            }
            finally
            {
                zeroBuffer(buffer, headIndex, bytesRead);
                headOrdered(buffer, head + bytesRead);
            }
        }

        return messagesRead;
    }

    /**
     * {@inheritDoc}
     */
    public int maxMsgLength()
    {
        return maxMsgLength;
    }

    /**
     * {@inheritDoc}
     */
    public long nextCorrelationId()
    {
        return buffer.getAndAddLong(correlationIdCounterIndex, 1);
    }

    /**
     * {@inheritDoc}
     */
    public AtomicBuffer buffer()
    {
        return buffer;
    }

    /**
     * {@inheritDoc}
     */
    public void consumerHeartbeatTimeNs(final long time)
    {
        consumerHeartbeatOrdered(buffer, time);
    }

    /**
     * {@inheritDoc}
     */
    public long consumerHeartbeatTimeNs()
    {
        return consumerHeartbeatVolatile(buffer);
    }

    private void checkMsgLength(final int length)
    {
        if (length > maxMsgLength)
        {
            final String msg = String.format("encoded message exceeds maxMsgLength of %d, length=%d", maxMsgLength, length);

            throw new IllegalArgumentException(msg);
        }
    }

    private int claimCapacity(final AtomicBuffer buffer, final int requiredCapacity)
    {
        final long head = headVolatile(buffer);
        final int headIndex = (int)head & mask;

        long tail;
        int tailIndex;
        int padding;
        do
        {
            tail = tailVolatile(buffer);
            final int availableCapacity = capacity - (int)(tail - head);

            if (requiredCapacity > availableCapacity)
            {
                return INSUFFICIENT_CAPACITY;
            }

            padding = 0;
            tailIndex = (int)tail & mask;
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
        while (!buffer.compareAndSetLong(tailCounterIndex, tail, tail + requiredCapacity + padding));

        if (0 != padding)
        {
            writePaddingRecord(buffer, tailIndex, padding);
            tailIndex = 0;
        }

        return tailIndex;
    }

    private long tailVolatile(final AtomicBuffer buffer)
    {
        return buffer.getLongVolatile(tailCounterIndex);
    }

    private long headVolatile(final AtomicBuffer buffer)
    {
        return buffer.getLongVolatile(headCounterIndex);
    }

    private long consumerHeartbeatVolatile(final AtomicBuffer buffer)
    {
        return buffer.getLongVolatile(consumerHeartbeatIndex);
    }

    private void headOrdered(final AtomicBuffer buffer, final long value)
    {
        buffer.putLongOrdered(headCounterIndex, value);
    }

    private void consumerHeartbeatOrdered(final AtomicBuffer buffer, final long value)
    {
        buffer.putLongOrdered(consumerHeartbeatIndex, value);
    }

    private static void writePaddingRecord(final AtomicBuffer buffer, final int recordIndex, final int padding)
    {
        msgType(buffer, recordIndex, PADDING_MSG_TYPE_ID);
        recordLengthOrdered(buffer, recordIndex, padding);
    }

    private static void recordLengthOrdered(final AtomicBuffer buffer, final int recordIndex, final int length)
    {
        buffer.putIntOrdered(RecordDescriptor.lengthOffset(recordIndex), length);
    }

    private static void msgLength(final AtomicBuffer buffer, final int recordIndex, final int length)
    {
        buffer.putInt(RecordDescriptor.msgLengthOffset(recordIndex), length);
    }

    private static void msgType(final AtomicBuffer buffer, final int recordIndex, final int msgTypeId)
    {
        buffer.putInt(RecordDescriptor.msgTypeOffset(recordIndex), msgTypeId);
    }

    private static void writeMsg(
        final AtomicBuffer buffer, final int recordIndex, final DirectBuffer srcBuffer, final int srcIndex, final int length)
    {
        buffer.putBytes(RecordDescriptor.encodedMsgOffset(recordIndex), srcBuffer, srcIndex, length);
    }

    private static int msgType(final AtomicBuffer buffer, final int recordIndex)
    {
        return buffer.getInt(RecordDescriptor.msgTypeOffset(recordIndex));
    }

    private static int msgLength(final AtomicBuffer buffer, final int recordIndex)
    {
        return buffer.getInt(RecordDescriptor.msgLengthOffset(recordIndex));
    }

    private static int waitForRecordLengthVolatile(final AtomicBuffer buffer, final int recordIndex)
    {
        int recordLength;
        do
        {
            recordLength = buffer.getIntVolatile(RecordDescriptor.lengthOffset(recordIndex));
        }
        while (0 == recordLength);

        return recordLength;
    }

    private static void zeroBuffer(final AtomicBuffer buffer, final int position, int length)
    {
        buffer.setMemory(position, length, (byte)0);
    }
}
