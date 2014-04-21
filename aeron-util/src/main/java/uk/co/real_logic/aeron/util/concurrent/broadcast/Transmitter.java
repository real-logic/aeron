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
package uk.co.real_logic.aeron.util.concurrent.broadcast;

import uk.co.real_logic.aeron.util.BitUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import static uk.co.real_logic.aeron.util.concurrent.broadcast.BufferDescriptor.*;
import static uk.co.real_logic.aeron.util.concurrent.broadcast.RecordDescriptor.*;

/**
 * Transmit messages via an underlying broadcast buffer to zero or more {@link Receiver}s.
 *
 * <b>Note:</b> This class is not threadsafe. Only one transmitter is allow per broadcast buffer.
 */
public class Transmitter
{
    private final AtomicBuffer buffer;
    private final int capacity;
    private final int mask;
    private final int maxMsgLength;
    private final int tailCounterIndex;

    /**
     * Construct a new broadcast transmitter based on an underlying {@link AtomicBuffer}.
     * The underlying buffer must a power of 2 in size plus sufficient space
     * for the {@link BufferDescriptor#TRAILER_SIZE}.
     *
     * @param buffer via which events will be exchanged.
     * @throws IllegalStateException if the buffer capacity is not a power of 2
     *                               plus {@link BufferDescriptor#TRAILER_SIZE} in capacity.
     */
    public Transmitter(final AtomicBuffer buffer)
    {
        this.buffer = buffer;
        capacity = buffer.capacity() - TRAILER_SIZE;

        checkCapacity(capacity);

        this.mask = capacity - 1;
        this.maxMsgLength = calculateMaxMessageLength(capacity);
        this.tailCounterIndex = capacity + TAIL_COUNTER_OFFSET;
    }

    /**
     * Get the capacity of the underlying broadcast buffer.
     *
     * @return the capacity of the underlying broadcast buffer.
     */
    public int capacity()
    {
        return capacity;
    }

    /**
     * Get the maximum message length that can be transmitted for a buffer.
     *
     * @return the maximum message length that can be transmitted for a buffer.
     */
    public int maxMsgLength()
    {
        return maxMsgLength;
    }

    /**
     * Transmit a message to {@link Receiver}s via the broadcast buffer.
     *
     * @param msgTypeId type of the message to be transmitted.
     * @param srcBuffer containing the encoded message to be transmitted.
     * @param index index in the source buffer at which the encoded message begins.
     * @param length in bytes of the encoded message.
     * @throws IllegalArgumentException of the msgTypeId is not valid,
     *                                  or if the message is greater than {@link #maxMsgLength()}.
     */
    public void transmit(final int msgTypeId, final AtomicBuffer srcBuffer, final int index, final int length)
    {
        checkMsgTypeId(msgTypeId);
        checkMessageLength(length);

        final long tail = buffer.getLong(tailCounterIndex);
        final int recordOffset = (int)tail & mask;
        final int recordLength = BitUtil.align(length, RECORD_ALIGNMENT);

        buffer.putLongOrdered(tailSequenceOffset(recordOffset), tail);
        buffer.putInt(recLengthOffset(recordOffset), recordLength);
        buffer.putInt(msgLengthOffset(recordOffset), length);
        buffer.putInt(msgTypeOffset(recordOffset), msgTypeId);

        buffer.putBytes(msgOffset(recordOffset), srcBuffer, index, length);

        buffer.putLongOrdered(tailCounterIndex, tail + recordLength);
    }

    private void checkMessageLength(final int length)
    {
        if (length > maxMsgLength)
        {
            final String msg = String.format("encoded message exceeds maxMsgLength of %d, length=%d",
                                             Integer.valueOf(maxMsgLength), Integer.valueOf(length));

            throw new IllegalArgumentException(msg);
        }
    }
}
