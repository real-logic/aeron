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

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static uk.co.real_logic.agrona.BitUtil.align;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.PADDING_FRAME_TYPE;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.TAIL_COUNTER_OFFSET;

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
 *
 * A message of type {@link FrameDescriptor#PADDING_FRAME_TYPE} is appended at the end of the buffer if claimed
 * space is not sufficiently large to accommodate the message about to be written.
 */
public class LogAppender extends LogBuffer
{
    public enum AppendStatus
    {
        SUCCESS,
        TRIPPED,
        FAILURE,
    }

    private final byte[] defaultHeader;
    private final int headerLength;
    private final int maxMessageLength;
    private final int maxFrameLength;
    private final int maxPayload;

    /**
     * Construct a view over a log buffer and state buffer for appending frames.
     *
     * @param logBuffer      for where messages are stored.
     * @param stateBuffer    for where the state of writers is stored manage concurrency.
     * @param defaultHeader  to be applied for each frame logged.
     * @param maxFrameLength maximum frame length supported by the underlying transport.
     */
    public LogAppender(
        final UnsafeBuffer logBuffer, final UnsafeBuffer stateBuffer, final byte[] defaultHeader, final int maxFrameLength)
    {
        super(logBuffer, stateBuffer);

        checkHeaderLength(defaultHeader.length);
        checkMaxFrameLength(maxFrameLength);

        this.defaultHeader = defaultHeader;
        this.headerLength = defaultHeader.length;
        this.maxFrameLength = maxFrameLength;
        this.maxMessageLength = FrameDescriptor.calculateMaxMessageLength(capacity());
        this.maxPayload = maxFrameLength - headerLength;
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
     * The default header applied to each record.
     *
     * @return the default header applied to each record.
     */
    public byte[] defaultHeader()
    {
        return defaultHeader;
    }

    /**
     * Append a message to the log if sufficient capacity exists.
     *
     * @param srcBuffer containing the encoded message.
     * @param srcOffset at which the encoded message begins.
     * @param length    of the message in bytes.
     * @return SUCCESS if appended in the log, FAILURE if not appended in the log, TRIPPED if first failure.
     * @throws IllegalArgumentException if the length is greater than {@link #maxMessageLength()}
     */
    public AppendStatus append(final DirectBuffer srcBuffer, final int srcOffset, final int length)
    {
        checkMessageLength(length);

        if (length <= maxPayload)
        {
            return appendUnfragmentedMessage(srcBuffer, srcOffset, length);
        }

        return appendFragmentedMessage(srcBuffer, srcOffset, length);
    }

    private AppendStatus appendUnfragmentedMessage(final DirectBuffer srcBuffer, final int srcOffset, final int length)
    {
        final int frameLength = length + headerLength;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final int frameOffset = getTailAndAdd(alignedLength);

        final UnsafeBuffer logBuffer = logBuffer();
        final int capacity = capacity();
        if (isBeyondLogBufferCapacity(frameOffset, alignedLength, capacity))
        {
            if (frameOffset < capacity)
            {
                appendPaddingFrame(logBuffer, frameOffset);
                return AppendStatus.TRIPPED;
            }
            else if (frameOffset == capacity)
            {
                return AppendStatus.TRIPPED;
            }

            return AppendStatus.FAILURE;
        }

        logBuffer.putBytes(frameOffset, defaultHeader, 0, headerLength);
        logBuffer.putBytes(frameOffset + headerLength, srcBuffer, srcOffset, length);

        frameFlags(logBuffer, frameOffset, UNFRAGMENTED);
        frameTermOffset(logBuffer, frameOffset, frameOffset);
        frameLengthOrdered(logBuffer, frameOffset, frameLength);

        return AppendStatus.SUCCESS;
    }

    private AppendStatus appendFragmentedMessage(final DirectBuffer srcBuffer, final int srcOffset, final int length)
    {
        final int numMaxPayloads = length / maxPayload;
        final int remainingPayload = length % maxPayload;
        final int requiredCapacity =
            align(remainingPayload + headerLength, FRAME_ALIGNMENT) + (numMaxPayloads * maxFrameLength);
        int frameOffset = getTailAndAdd(requiredCapacity);

        final UnsafeBuffer logBuffer = logBuffer();
        final int capacity = capacity();
        if (isBeyondLogBufferCapacity(frameOffset, requiredCapacity, capacity))
        {
            if (frameOffset < capacity)
            {
                appendPaddingFrame(logBuffer, frameOffset);
                return AppendStatus.TRIPPED;
            }
            else if (frameOffset == capacity)
            {
                return AppendStatus.TRIPPED;
            }

            return AppendStatus.FAILURE;
        }

        byte flags = BEGIN_FRAG;
        int remaining = length;
        do
        {
            final int bytesToWrite = Math.min(remaining, maxPayload);
            final int frameLength = bytesToWrite + headerLength;
            final int alignedLength = align(frameLength, FRAME_ALIGNMENT);

            logBuffer.putBytes(frameOffset, defaultHeader, 0, headerLength);
            logBuffer.putBytes(
                frameOffset + headerLength,
                srcBuffer,
                srcOffset + (length - remaining),
                bytesToWrite);

            if (remaining <= maxPayload)
            {
                flags |= END_FRAG;
            }

            frameFlags(logBuffer, frameOffset, flags);
            frameTermOffset(logBuffer, frameOffset, frameOffset);
            frameLengthOrdered(logBuffer, frameOffset, frameLength);

            flags = 0;
            frameOffset += alignedLength;
            remaining -= bytesToWrite;
        }
        while (remaining > 0);

        return AppendStatus.SUCCESS;
    }

    private boolean isBeyondLogBufferCapacity(final int frameOffset, final int alignedFrameLength, final int capacity)
    {
        return (frameOffset + alignedFrameLength + headerLength) > capacity;
    }

    private void appendPaddingFrame(final UnsafeBuffer logBuffer, final int frameOffset)
    {
        logBuffer.putBytes(frameOffset, defaultHeader, 0, headerLength);

        frameType(logBuffer, frameOffset, PADDING_FRAME_TYPE);
        frameFlags(logBuffer, frameOffset, UNFRAGMENTED);
        frameTermOffset(logBuffer, frameOffset, frameOffset);
        frameLengthOrdered(logBuffer, frameOffset, capacity() - frameOffset);
    }

    private int getTailAndAdd(final int delta)
    {
        return stateBuffer().getAndAddInt(TAIL_COUNTER_OFFSET, delta);
    }

    private void checkMessageLength(final int length)
    {
        if (length > maxMessageLength)
        {
            final String s = String.format(
                "encoded message exceeds maxMessageLength of %d, length=%d",
                maxMessageLength,
                length);

            throw new IllegalArgumentException(s);
        }
    }
}
