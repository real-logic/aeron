/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static uk.co.real_logic.agrona.BitUtil.align;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.PADDING_FRAME_TYPE;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET;

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
public class LogAppender extends LogBufferPartition
{
    public enum ActionStatus
    {
        SUCCEEDED,
        TRIPPED,
        FAILED,
    }

    private final MutableDirectBuffer defaultHeader;
    private final int headerLength;
    private final int maxMessageLength;
    private final int maxFrameLength;
    private final int maxPayloadLength;

    /**
     * Construct a view over a log buffer and state buffer for appending frames.
     *
     * @param termBuffer     for where messages are stored.
     * @param metaDataBuffer for where the state of writers is stored manage concurrency.
     * @param defaultHeader  to be applied for each frame logged.
     * @param maxFrameLength maximum frame length supported by the underlying transport.
     */
    public LogAppender(
        final UnsafeBuffer termBuffer,
        final UnsafeBuffer metaDataBuffer,
        final MutableDirectBuffer defaultHeader,
        final int maxFrameLength)
    {
        super(termBuffer, metaDataBuffer);

        checkHeaderLength(defaultHeader.capacity());
        checkMaxFrameLength(maxFrameLength);

        this.defaultHeader = defaultHeader;
        this.headerLength = defaultHeader.capacity();
        this.maxFrameLength = maxFrameLength;
        this.maxMessageLength = FrameDescriptor.computeMaxMessageLength(termBuffer.capacity());
        this.maxPayloadLength = maxFrameLength - headerLength;
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
        return maxPayloadLength;
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
    public MutableDirectBuffer defaultHeader()
    {
        return defaultHeader;
    }

    /**
     * Append a message to the log if sufficient capacity exists.
     *
     * @param srcBuffer containing the encoded message.
     * @param srcOffset at which the encoded message begins.
     * @param length    of the message in bytes.
     * @return SUCCEEDED if append was successful, FAILED if beyond end of the log in the log, TRIPPED if first failure.
     * @throws IllegalArgumentException if the length is greater than {@link #maxMessageLength()}
     */
    public ActionStatus append(final DirectBuffer srcBuffer, final int srcOffset, final int length)
    {
        checkMessageLength(length);

        if (length <= maxPayloadLength)
        {
            return appendUnfragmentedMessage(srcBuffer, srcOffset, length);
        }

        return appendFragmentedMessage(srcBuffer, srcOffset, length);
    }

    /**
     * Claim a range within the buffer for recording a message payload.
     *
     * @param length      of the message payload
     * @param bufferClaim to be completed for the claim if successful.
     * @return SUCCEEDED if claim was successful, FAILED if beyond end of the log in the log, TRIPPED if first failure.
     */
    public ActionStatus claim(final int length, final BufferClaim bufferClaim)
    {
        if (length > maxPayloadLength)
        {
            final String s = String.format("claim exceeds maxPayloadLength of %d, length=%d", maxPayloadLength, length);
            throw new IllegalArgumentException(s);
        }

        final int headerLength = this.headerLength;
        final int frameLength = length + headerLength;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final int frameOffset = getTailAndAdd(alignedLength);

        final UnsafeBuffer termBuffer = termBuffer();
        final int capacity = termBuffer.capacity();
        if (isBeyondLogBufferCapacity(frameOffset, alignedLength, capacity))
        {
            if (frameOffset < capacity)
            {
                appendPaddingFrame(termBuffer, frameOffset, capacity);
                return ActionStatus.TRIPPED;
            }
            else if (frameOffset == capacity)
            {
                return ActionStatus.TRIPPED;
            }

            return ActionStatus.FAILED;
        }

        termBuffer.putBytes(frameOffset, defaultHeader, 0, headerLength);
        frameFlags(termBuffer, frameOffset, UNFRAGMENTED);
        frameTermOffset(termBuffer, frameOffset, frameOffset);

        bufferClaim.buffer(termBuffer)
                   .offset(frameOffset + headerLength)
                   .length(length)
                   .frameLengthOffset(lengthOffset(frameOffset))
                   .frameLength(frameLength);

        return ActionStatus.SUCCEEDED;
    }

    private ActionStatus appendUnfragmentedMessage(final DirectBuffer srcBuffer, final int srcOffset, final int length)
    {
        final int headerLength = this.headerLength;
        final int frameLength = length + headerLength;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final int frameOffset = getTailAndAdd(alignedLength);

        final UnsafeBuffer termBuffer = termBuffer();
        final int capacity = termBuffer.capacity();
        if (isBeyondLogBufferCapacity(frameOffset, alignedLength, capacity))
        {
            if (frameOffset < capacity)
            {
                appendPaddingFrame(termBuffer, frameOffset, capacity);
                return ActionStatus.TRIPPED;
            }
            else if (frameOffset == capacity)
            {
                return ActionStatus.TRIPPED;
            }

            return ActionStatus.FAILED;
        }

        termBuffer.putBytes(frameOffset, defaultHeader, 0, headerLength);
        termBuffer.putBytes(frameOffset + headerLength, srcBuffer, srcOffset, length);

        frameFlags(termBuffer, frameOffset, UNFRAGMENTED);
        frameTermOffset(termBuffer, frameOffset, frameOffset);
        frameLengthOrdered(termBuffer, frameOffset, frameLength);

        return ActionStatus.SUCCEEDED;
    }

    private ActionStatus appendFragmentedMessage(final DirectBuffer srcBuffer, final int srcOffset, final int length)
    {
        final int numMaxPayloads = length / maxPayloadLength;
        final int remainingPayload = length % maxPayloadLength;
        final int headerLength = this.headerLength;
        final int lastFrameLength = (remainingPayload > 0) ? align(remainingPayload + headerLength, FRAME_ALIGNMENT) : 0;
        final int requiredCapacity = (numMaxPayloads * maxFrameLength) + lastFrameLength;
        int frameOffset = getTailAndAdd(requiredCapacity);

        final UnsafeBuffer termBuffer = termBuffer();
        final int capacity = termBuffer.capacity();
        if (isBeyondLogBufferCapacity(frameOffset, requiredCapacity, capacity))
        {
            if (frameOffset < capacity)
            {
                appendPaddingFrame(termBuffer, frameOffset, capacity);
                return ActionStatus.TRIPPED;
            }
            else if (frameOffset == capacity)
            {
                return ActionStatus.TRIPPED;
            }

            return ActionStatus.FAILED;
        }

        byte flags = BEGIN_FRAG;
        int remaining = length;
        do
        {
            final int bytesToWrite = Math.min(remaining, maxPayloadLength);
            final int frameLength = bytesToWrite + headerLength;
            final int alignedLength = align(frameLength, FRAME_ALIGNMENT);

            termBuffer.putBytes(frameOffset, defaultHeader, 0, headerLength);
            termBuffer.putBytes(
                frameOffset + headerLength,
                srcBuffer,
                srcOffset + (length - remaining),
                bytesToWrite);

            if (remaining <= maxPayloadLength)
            {
                flags |= END_FRAG;
            }

            frameFlags(termBuffer, frameOffset, flags);
            frameTermOffset(termBuffer, frameOffset, frameOffset);
            frameLengthOrdered(termBuffer, frameOffset, frameLength);

            flags = 0;
            frameOffset += alignedLength;
            remaining -= bytesToWrite;
        }
        while (remaining > 0);

        return ActionStatus.SUCCEEDED;
    }

    private boolean isBeyondLogBufferCapacity(final int frameOffset, final int alignedFrameLength, final int capacity)
    {
        return (frameOffset + alignedFrameLength + headerLength) > capacity;
    }

    private void appendPaddingFrame(final UnsafeBuffer termBuffer, final int frameOffset, final int capacity)
    {
        termBuffer.putBytes(frameOffset, defaultHeader, 0, headerLength);

        frameType(termBuffer, frameOffset, PADDING_FRAME_TYPE);
        frameFlags(termBuffer, frameOffset, UNFRAGMENTED);
        frameTermOffset(termBuffer, frameOffset, frameOffset);
        frameLengthOrdered(termBuffer, frameOffset, capacity - frameOffset);
    }

    private int getTailAndAdd(final int delta)
    {
        return metaDataBuffer().getAndAddInt(TERM_TAIL_COUNTER_OFFSET, delta);
    }

    private void checkMessageLength(final int length)
    {
        if (length > maxMessageLength)
        {
            final String s = String.format("encoded message exceeds maxMessageLength of %d, length=%d", maxMessageLength, length);
            throw new IllegalArgumentException(s);
        }
    }
}
