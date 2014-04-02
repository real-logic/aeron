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
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.*;

/**
 * Log buffer appender which supports many producers concurrently writing an append-only log.
 *
 * Messages are appending to a log using a framing protocol as described in {@link FrameDescriptor}.
 * If a message is larger than a single frame it will be fragmented up to {@link #maxMessageLength()}.
 */
public class LogBufferAppender
{
    private final AtomicBuffer logBuffer;
    private final AtomicBuffer stateBuffer;
    private final int capacity;
    private final int headerReserveLength;
    private final int maxMessageLength;

    /**
     * Construct a view over a log buffer and state buffer for appending frames.
     *
     * @param logAtomicBuffer for where events are stored.
     * @param stateAtomicBuffer for where the state of writers is stored manage concurrency.
     * @param headerReserveLength length of the header field to be used on each frame.
     */
    public LogBufferAppender(final AtomicBuffer logAtomicBuffer,
                             final AtomicBuffer stateAtomicBuffer,
                             final int headerReserveLength)
    {
        checkLogBuffer(logAtomicBuffer);
        checkStateBuffer(stateAtomicBuffer);
        checkHeaderReserve(headerReserveLength);

        this.logBuffer = logAtomicBuffer;
        this.stateBuffer = stateAtomicBuffer;
        this.capacity = logAtomicBuffer.capacity();
        this.headerReserveLength = headerReserveLength;
        this.maxMessageLength = FrameDescriptor.calculateMaxMessageLength(capacity);
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
     * The header reserve length in bytes.
     *
     * @return the header reserve length in bytes.
     */
    public int headerReserveLength()
    {
        return headerReserveLength;
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

        final int requiredCapacity = align(length + headerReserveLength, FRAME_ALIGNMENT);
        final int frameOffset = getTailAndAdd(requiredCapacity);

        if (frameOffset >= capacity)
        {
            return false;
        }

        if (frameOffset + requiredCapacity > capacity)
        {
            appendPaddingFrame(frameOffset);

            return false;
        }

        appendFrame(frameOffset, requiredCapacity, srcBuffer, srcOffset, length, UNFRAGMENTED);

        return true;
    }

    private void appendFrame(final int frameOffset,
                             final int requiredCapacity,
                             final AtomicBuffer srcBuffer,
                             final int srcOffset,
                             final int length,
                             final byte fragmentFlags)
    {
        logBuffer.putBytes(messageOffset(frameOffset, headerReserveLength), srcBuffer, srcOffset, length);

        putFragmentFlags(frameOffset, fragmentFlags);
        putFrameLengthOrdered(frameOffset, requiredCapacity);
    }

    private void appendPaddingFrame(final int frameOffset)
    {
        putFragmentFlags(frameOffset, UNFRAGMENTED);
        putFrameLengthOrdered(frameOffset, capacity - frameOffset);
    }

    private void putFragmentFlags(final int frameOffset, final byte flags)
    {
        logBuffer.putByte(fragmentFlagsOffset(frameOffset), flags);
    }

    private void putFrameLengthOrdered(final int frameOffset, final int length)
    {
        logBuffer.putIntOrdered(lengthOffset(frameOffset), length);
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
