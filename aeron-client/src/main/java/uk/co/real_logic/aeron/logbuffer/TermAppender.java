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
package uk.co.real_logic.aeron.logbuffer;

import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

import static java.lang.Integer.reverseBytes;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static uk.co.real_logic.agrona.BitUtil.*;

/**
 * Term buffer appender which supports many producers concurrently writing an append-only log.
 *
 * <b>Note:</b> This class is threadsafe.
 *
 * Messages are appended to a term using a framing protocol as described in {@link FrameDescriptor}.
 * If a message is larger than what will fit in a single frame will be fragmented up to {@link #maxMessageLength()}.
 *
 * A default message header is applied to each message with the fields filled in for fragment flags, type, term number,
 * as appropriate.
 *
 * A message of type {@link FrameDescriptor#PADDING_FRAME_TYPE} is appended at the end of the buffer if claimed
 * space is not sufficiently large to accommodate the message about to be written.
 */
public class TermAppender
{
    /**
     * The append operation tripped the end of the buffer and needs to rotate.
     */
    public static final int TRIPPED = -1;

    /**
     * The append operation went past the end of the buffer and failed.
     */
    public static final int FAILED = -2;

    private final int maxMessageLength;
    private final int maxFrameLength;
    private final int maxPayloadLength;
    private final int sessionId;
    private final int headerSecondWord;
    private final UnsafeBuffer termBuffer;
    private final UnsafeBuffer metaDataBuffer;
    private final MutableDirectBuffer defaultHeader;

    /**
     * Construct a view over a term buffer and state buffer for appending frames.
     *
     * @param termBuffer     for where messages are stored.
     * @param metaDataBuffer for where the state of writers is stored manage concurrency.
     * @param defaultHeader  to be applied for each frame logged.
     * @param maxFrameLength maximum frame length supported by the underlying transport.
     */
    public TermAppender(
        final UnsafeBuffer termBuffer,
        final UnsafeBuffer metaDataBuffer,
        final MutableDirectBuffer defaultHeader,
        final int maxFrameLength)
    {
        checkTermLength(termBuffer.capacity());
        checkMetaDataBuffer(metaDataBuffer);

        checkHeaderLength(defaultHeader.capacity());
        checkMaxFrameLength(maxFrameLength);
        termBuffer.verifyAlignment();
        metaDataBuffer.verifyAlignment();

        this.termBuffer = termBuffer;
        this.metaDataBuffer = metaDataBuffer;

        this.defaultHeader = defaultHeader;
        this.maxFrameLength = maxFrameLength;
        this.maxMessageLength = FrameDescriptor.computeMaxMessageLength(termBuffer.capacity());
        this.maxPayloadLength = maxFrameLength - HEADER_LENGTH;
        this.headerSecondWord = defaultHeader.getInt(SIZE_OF_INT);
        this.sessionId = defaultHeader.getInt(DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET);
    }

    /**
     * Get the raw value current tail value in a volatile memory ordering fashion.
     *
     * @return the current tail value.
     */
    public int rawTailVolatile()
    {
        return metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET);
    }

    /**
     * Get the current tail value in a volatile memory ordering fashion. If raw tail is greater than
     * term capacity then capacity will be returned.
     *
     * @return the current tail value.
     */
    public int tailVolatile()
    {
        return Math.min(metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET), termBuffer.capacity());
    }

    /**
     * Set the status of the log buffer with StoreStore memory ordering semantics.
     *
     * @param status to be set for the log buffer.
     */
    public void statusOrdered(final int status)
    {
        metaDataBuffer.putIntOrdered(TERM_STATUS_OFFSET, status);
    }

    /**
     * The maximum length of a message that can be recorded in the term.
     *
     * @return the maximum length of a message that can be recorded in the term.
     */
    public int maxMessageLength()
    {
        return maxMessageLength;
    }

    /**
     * The maximum length of a message payload within a frame before fragmentation takes place.
     *
     * @return the maximum length of a message that can be recorded in the term.
     */
    public int maxPayloadLength()
    {
        return maxPayloadLength;
    }

    /**
     * The maximum length of a frame, including header, that can be recorded in the term.
     *
     * @return the maximum length of a frame, including header, that can be recorded in the term.
     */
    public int maxFrameLength()
    {
        return maxFrameLength;
    }

    /**
     * Append a message to the term if sufficient capacity exists.
     *
     * @param srcBuffer containing the encoded message.
     * @param srcOffset at which the encoded message begins.
     * @param length    of the message in bytes.
     * @return the resulting termOffset on success otherwise {@link #FAILED} if beyond end of the term, or
     * {@link #TRIPPED} if first failure.
     * @throws IllegalArgumentException if the length is greater than {@link #maxMessageLength()}
     */
    public int append(final DirectBuffer srcBuffer, final int srcOffset, final int length)
    {
        final int resultingOffset;
        if (length <= maxPayloadLength)
        {
            resultingOffset = appendUnfragmentedMessage(srcBuffer, srcOffset, length);
        }
        else
        {
            if (length > maxMessageLength)
            {
                throw new IllegalArgumentException(String.format(
                    "Encoded message exceeds maxMessageLength of %d, length=%d", maxMessageLength, length));
            }

            resultingOffset = appendFragmentedMessage(srcBuffer, srcOffset, length);
        }

        return resultingOffset;
    }

    /**
     * Claim a range within the buffer for recording a message payload.
     *
     * @param length      of the message payload
     * @param bufferClaim to be completed for the claim if successful.
     * @return the resulting termOffset on success otherwise {@link #FAILED} if beyond end of the term, or
     * {@link #TRIPPED} if first failure.
     */
    public int claim(final int length, final BufferClaim bufferClaim)
    {
        if (length > maxPayloadLength)
        {
            throw new IllegalArgumentException(String.format(
                "Claim exceeds maxPayloadLength of %d, length=%d", maxPayloadLength, length));
        }

        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final int frameOffset = metaDataBuffer.getAndAddInt(TERM_TAIL_COUNTER_OFFSET, alignedLength);
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int capacity = termBuffer.capacity();

        int resultingOffset = frameOffset + alignedLength;
        if (resultingOffset > (capacity - HEADER_LENGTH))
        {
            resultingOffset = handleEndOfLogCondition(termBuffer, frameOffset, capacity);
        }
        else
        {
            applyDefaultHeader(termBuffer, frameOffset, frameLength, defaultHeader);
            bufferClaim.wrap(termBuffer, frameOffset, frameLength);
        }

        return resultingOffset;
    }

    private int appendUnfragmentedMessage(final DirectBuffer srcBuffer, final int srcOffset, final int length)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final int frameOffset = metaDataBuffer.getAndAddInt(TERM_TAIL_COUNTER_OFFSET, alignedLength);
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int capacity = termBuffer.capacity();

        int resultingOffset = frameOffset + alignedLength;
        if (resultingOffset > (capacity - HEADER_LENGTH))
        {
            resultingOffset = handleEndOfLogCondition(termBuffer, frameOffset, capacity);
        }
        else
        {
            applyDefaultHeader(termBuffer, frameOffset, frameLength, defaultHeader);
            termBuffer.putBytes(frameOffset + HEADER_LENGTH, srcBuffer, srcOffset, length);
            frameLengthOrdered(termBuffer, frameOffset, frameLength);
        }

        return resultingOffset;
    }

    private int appendFragmentedMessage(final DirectBuffer srcBuffer, final int srcOffset, final int length)
    {
        final int numMaxPayloads = length / maxPayloadLength;
        final int remainingPayload = length % maxPayloadLength;
        final int lastFrameLength = (remainingPayload > 0) ? align(remainingPayload + HEADER_LENGTH, FRAME_ALIGNMENT) : 0;
        final int requiredLength = (numMaxPayloads * maxFrameLength) + lastFrameLength;
        int frameOffset = metaDataBuffer.getAndAddInt(TERM_TAIL_COUNTER_OFFSET, requiredLength);
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int capacity = termBuffer.capacity();

        int resultingOffset = frameOffset + requiredLength;
        if (resultingOffset > (capacity - HEADER_LENGTH))
        {
            resultingOffset = handleEndOfLogCondition(termBuffer, frameOffset, capacity);
        }
        else
        {
            byte flags = BEGIN_FRAG;
            int remaining = length;
            do
            {
                final int bytesToWrite = Math.min(remaining, maxPayloadLength);
                final int frameLength = bytesToWrite + HEADER_LENGTH;
                final int alignedLength = align(frameLength, FRAME_ALIGNMENT);

                applyDefaultHeader(termBuffer, frameOffset, frameLength, defaultHeader);
                termBuffer.putBytes(
                    frameOffset + HEADER_LENGTH,
                    srcBuffer,
                    srcOffset + (length - remaining),
                    bytesToWrite);

                if (remaining <= maxPayloadLength)
                {
                    flags |= END_FRAG;
                }

                frameFlags(termBuffer, frameOffset, flags);
                frameLengthOrdered(termBuffer, frameOffset, frameLength);

                flags = 0;
                frameOffset += alignedLength;
                remaining -= bytesToWrite;
            }
            while (remaining > 0);
        }

        return resultingOffset;
    }

    private void applyDefaultHeader(
        final UnsafeBuffer buffer,
        final int frameOffset,
        final int frameLength,
        final MutableDirectBuffer defaultHeaderBuffer)
    {
        long firstLongWord;
        long secondLongWord;

        if (ByteOrder.nativeOrder() == LITTLE_ENDIAN)
        {
            firstLongWord = ((headerSecondWord & 0xFFFF_FFFFL) << 32) | ((-frameLength) & 0xFFFF_FFFFL);
            secondLongWord = ((sessionId & 0xFFFF_FFFFL) << 32) | (frameOffset & 0xFFFF_FFFFL);
        }
        else
        {
            firstLongWord = (((reverseBytes(-frameLength)) & 0xFFFF_FFFFL) << 32) | (headerSecondWord & 0xFFFF_FFFFL);
            secondLongWord = (((reverseBytes(frameOffset)) & 0xFFFF_FFFFL) << 32) | (sessionId & 0xFFFF_FFFFL);
        }

        buffer.putLongOrdered(frameOffset, firstLongWord);
        buffer.putLong(frameOffset + SIZE_OF_LONG, secondLongWord);
        buffer.putLong(frameOffset + (SIZE_OF_LONG * 2), defaultHeaderBuffer.getLong((SIZE_OF_LONG * 2)));
    }

    private int handleEndOfLogCondition(final UnsafeBuffer termBuffer, final int frameOffset, final int capacity)
    {
        int resultingOffset = FAILED;

        if (frameOffset <= (capacity - HEADER_LENGTH))
        {
            final int paddingLength = capacity - frameOffset;
            applyDefaultHeader(termBuffer, frameOffset, paddingLength, defaultHeader);
            frameType(termBuffer, frameOffset, PADDING_FRAME_TYPE);
            frameLengthOrdered(termBuffer, frameOffset, paddingLength);

            resultingOffset = TRIPPED;
        }

        return resultingOffset;
    }
}
