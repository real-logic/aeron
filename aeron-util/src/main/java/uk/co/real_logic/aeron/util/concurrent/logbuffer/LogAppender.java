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

import java.nio.ByteOrder;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.aeron.util.BitUtil.align;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.BufferDescriptor.*;

/**
 * Log buffer appender which supports many producers concurrently writing an append-only log.
 *
 * <b>Note:</b> This class is threadsafe.
 *
 * Messages are appending to a log using a framing protocol as described in {@link FrameDescriptor}.
 * If a message is larger than what will fit in a single frame will be fragmented up to {@link #maxMessageLength()}.
 *
 * A default message header is applied to each message with the fields filled in for fragment flags, sequence number,
 * and frame length as appropriate.
 */
public class LogAppender
{
    private final AtomicBuffer logBuffer;
    private final AtomicBuffer stateBuffer;
    private final byte[] defaultHeader;
    private final int capacity;
    private final int maxMessageLength;
    private final int maxFrameLength;
    private final int headerLength;
    private final int maxPayload;

    /**
     * Construct a view over a log buffer and state buffer for appending frames.
     *
     * @param logBuffer for where events are stored.
     * @param stateBuffer for where the state of writers is stored manage concurrency.
     * @param defaultHeader to be applied for each frame logged.
     * @param maxFrameLength maximum frame length supported by the underlying transport.
     */
    public LogAppender(final AtomicBuffer logBuffer,
                       final AtomicBuffer stateBuffer,
                       final byte[] defaultHeader,
                       final int maxFrameLength)
    {
        checkLogBuffer(logBuffer);
        checkStateBuffer(stateBuffer);
        checkHeaderLength(defaultHeader.length);
        checkMaxFrameLength(maxFrameLength);

        this.logBuffer = logBuffer;
        this.stateBuffer = stateBuffer;
        this.capacity = logBuffer.capacity();
        this.defaultHeader = defaultHeader;
        this.maxFrameLength = maxFrameLength;
        this.maxMessageLength = FrameDescriptor.calculateMaxMessageLength(capacity);
        this.headerLength = defaultHeader.length;
        this.maxPayload = maxFrameLength - headerLength;
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
     * The maximum length of a message payload within a frame before fragmentation takes place.
     *
     * @return the maximum length of a message that can be recorded in the log.
     */
    public int maxPayloadLength()
    {
        return maxPayload;
    }

    /**
     * The maximum length of a frame, including header, that can be recorded in the log.
     *
     * @return the maximum length of a frame, including header, that can be recorded in the log.
     */
    public int maxFrameLength()
    {
        return maxFrameLength;
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

        if (length <= maxPayload)
        {
            return appendUnfragmentedMessage(srcBuffer, srcOffset, length);
        }

        return appendFragmentedMessage(srcBuffer, srcOffset, length);
    }

    private boolean appendUnfragmentedMessage(final AtomicBuffer srcBuffer, final int srcOffset, final int length)
    {
        final int frameLength = align(length + headerLength, FRAME_ALIGNMENT);
        final int frameOffset = getTailAndAdd(frameLength);

        if (frameOffset + frameLength > capacity)
        {
            if (frameOffset < capacity)
            {
                appendPaddingFrame(frameOffset);
            }

            return false;
        }

        logBuffer.putBytes(frameOffset, defaultHeader, 0, headerLength);
        logBuffer.putBytes(frameOffset + headerLength, srcBuffer, srcOffset, length);

        putFlags(frameOffset, UNFRAGMENTED);
        putTermOffset(frameOffset, frameOffset);
        putLengthOrdered(frameOffset, frameLength);

        return true;
    }

    private boolean appendFragmentedMessage(final AtomicBuffer srcBuffer, final int srcOffset, final int length)
    {
        final int numMaxPayloads = length / maxPayload;
        final int remainingPayload = length % maxPayload;
        final int requiredCapacity =
            align(remainingPayload + headerLength, FRAME_ALIGNMENT) + (numMaxPayloads * maxFrameLength);
        int frameOffset = getTailAndAdd(requiredCapacity);

        if (frameOffset + requiredCapacity > capacity)
        {
            if (frameOffset < capacity)
            {
                appendPaddingFrame(frameOffset);
            }

            return false;
        }

        byte flags = BEGIN_FRAG;
        int remaining = length;
        do
        {
            final int bytesToWrite = Math.min(remaining, maxPayload);
            final int frameLength = align(bytesToWrite + headerLength, FRAME_ALIGNMENT);

            logBuffer.putBytes(frameOffset, defaultHeader, 0, headerLength);
            logBuffer.putBytes(frameOffset + headerLength,
                               srcBuffer,
                               srcOffset + (length - remaining),
                               bytesToWrite);

            if (remaining <= maxPayload)
            {
                flags |= END_FRAG;
            }

            putFlags(frameOffset, flags);
            putTermOffset(frameOffset, frameOffset);
            putLengthOrdered(frameOffset, frameLength);

            flags = 0;
            frameOffset += frameLength;
            remaining -= bytesToWrite;
        }
        while (remaining > 0);

        return true;
    }

    private void appendPaddingFrame(final int frameOffset)
    {
        logBuffer.putBytes(frameOffset, defaultHeader, 0, headerLength);

        putType(frameOffset, PADDING_MSG_TYPE);
        putFlags(frameOffset, UNFRAGMENTED);
        putTermOffset(frameOffset, frameOffset);
        putLengthOrdered(frameOffset, capacity - frameOffset);
    }

    private void putType(final int frameOffset, final short type)
    {
        logBuffer.putShort(typeOffset(frameOffset), type, LITTLE_ENDIAN);
    }

    private void putFlags(final int frameOffset, final byte flags)
    {
        logBuffer.putByte(flagsOffset(frameOffset), flags);
    }

    private void putTermOffset(final int frameOffset, final int termOffset)
    {
        logBuffer.putInt(termOffsetOffset(frameOffset), termOffset, LITTLE_ENDIAN);
    }

    private void putLengthOrdered(final int frameOffset, int frameLength)
    {
        if (LITTLE_ENDIAN != ByteOrder.nativeOrder())
        {
            frameLength = Integer.reverseBytes(frameLength);
        }

        logBuffer.putIntOrdered(lengthOffset(frameOffset), frameLength);
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
