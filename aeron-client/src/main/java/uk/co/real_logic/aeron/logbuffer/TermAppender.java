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

import java.nio.ByteOrder;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.UnsafeAccess;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static java.lang.Integer.reverseBytes;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.BEGIN_FRAG_FLAG;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.END_FRAG_FLAG;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.PADDING_FRAME_TYPE;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.checkHeaderLength;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.frameFlags;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.frameLengthOrdered;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.frameType;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.TERM_STATUS_OFFSET;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.checkMetaDataBuffer;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.checkTermLength;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.*;
import static uk.co.real_logic.aeron.protocol.HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET;
import static uk.co.real_logic.agrona.BitUtil.align;

/**
 * Term buffer appender which supports many producers concurrently writing an append-only log.
 *
 * <b>Note:</b> This class is threadsafe.
 *
 * Messages are appended to a term using a framing protocol as described in {@link FrameDescriptor}.
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

    private final UnsafeBuffer termBuffer;
    private final UnsafeBuffer metaDataBuffer;
    private final MutableDirectBuffer defaultHeader;

    /**
     * Construct a view over a term buffer and state buffer for appending frames.
     *
     * @param termBuffer     for where messages are stored.
     * @param metaDataBuffer for where the state of writers is stored manage concurrency.
     * @param defaultHeader  to be applied for each frame logged.
     */
    public TermAppender(
        final UnsafeBuffer termBuffer, final UnsafeBuffer metaDataBuffer, final MutableDirectBuffer defaultHeader)
    {
        checkTermLength(termBuffer.capacity());
        checkMetaDataBuffer(metaDataBuffer);

        checkHeaderLength(defaultHeader.capacity());
        termBuffer.verifyAlignment();
        metaDataBuffer.verifyAlignment();

        this.termBuffer = termBuffer;
        this.metaDataBuffer = metaDataBuffer;
        this.defaultHeader = defaultHeader;
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

    public int claim(
        final long headerVersionFlagsType,
        final long headerSessionId,
        final int length,
        final BufferClaim bufferClaim)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final int frameOffset = metaDataBuffer.getAndAddInt(TERM_TAIL_COUNTER_OFFSET, alignedLength);
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int capacity = termBuffer.capacity();

        int resultingOffset = frameOffset + alignedLength;
        if (resultingOffset > (capacity - HEADER_LENGTH))
        {
            resultingOffset = handleEndOfLogCondition(
                termBuffer, frameOffset, headerVersionFlagsType, headerSessionId, capacity);
        }
        else
        {
            applyDefaultHeader(
                termBuffer, frameOffset, frameLength, headerVersionFlagsType, headerSessionId, defaultHeader);
            bufferClaim.wrap(termBuffer, frameOffset, frameLength);
        }

        return resultingOffset;
    }

    public int appendUnfragmentedMessage(
        final long headerVersionFlagsType,
        final long headerSessionId,
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int length)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final int frameOffset = metaDataBuffer.getAndAddInt(TERM_TAIL_COUNTER_OFFSET, alignedLength);
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int capacity = termBuffer.capacity();

        int resultingOffset = frameOffset + alignedLength;
        if (resultingOffset > (capacity - HEADER_LENGTH))
        {
            resultingOffset = handleEndOfLogCondition(
                termBuffer, frameOffset, headerVersionFlagsType, headerSessionId, capacity);
        }
        else
        {
            applyDefaultHeader(
                termBuffer, frameOffset, frameLength, headerVersionFlagsType, headerSessionId, defaultHeader);
            termBuffer.putBytes(frameOffset + HEADER_LENGTH, srcBuffer, srcOffset, length);
            frameLengthOrdered(termBuffer, frameOffset, frameLength);
        }

        return resultingOffset;
    }

    public int appendFragmentedMessage(
        final long headerVersionFlagsType,
        final long headerSessionId,
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int length,
        final int maxPayloadLength)
    {
        final int numMaxPayloads = length / maxPayloadLength;
        final int remainingPayload = length % maxPayloadLength;
        final int lastFrameLength = (remainingPayload > 0) ? align(remainingPayload + HEADER_LENGTH, FRAME_ALIGNMENT) : 0;
        final int requiredLength = (numMaxPayloads * (maxPayloadLength + HEADER_LENGTH)) + lastFrameLength;
        int frameOffset = metaDataBuffer.getAndAddInt(TERM_TAIL_COUNTER_OFFSET, requiredLength);
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int capacity = termBuffer.capacity();

        int resultingOffset = frameOffset + requiredLength;
        if (resultingOffset > (capacity - HEADER_LENGTH))
        {
            resultingOffset = handleEndOfLogCondition(
                termBuffer, frameOffset, headerVersionFlagsType, headerSessionId, capacity);
        }
        else
        {
            byte flags = BEGIN_FRAG_FLAG;
            int remaining = length;
            do
            {
                final int bytesToWrite = Math.min(remaining, maxPayloadLength);
                final int frameLength = bytesToWrite + HEADER_LENGTH;
                final int alignedLength = align(frameLength, FRAME_ALIGNMENT);

                applyDefaultHeader(
                    termBuffer, frameOffset, frameLength, headerVersionFlagsType, headerSessionId, defaultHeader);
                termBuffer.putBytes(
                    frameOffset + HEADER_LENGTH,
                    srcBuffer,
                    srcOffset + (length - remaining),
                    bytesToWrite);

                if (remaining <= maxPayloadLength)
                {
                    flags |= END_FRAG_FLAG;
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
        final long headerVersionFlagsType,
        final long headerSessionId,
        final MutableDirectBuffer defaultHeaderBuffer)
    {
        long lengthVersionFlagsType;
        long termOffsetAndSessionId;

        if (ByteOrder.nativeOrder() == LITTLE_ENDIAN)
        {
            lengthVersionFlagsType = headerVersionFlagsType | ((-frameLength) & 0xFFFF_FFFFL);
            termOffsetAndSessionId = headerSessionId | frameOffset;
        }
        else
        {
            lengthVersionFlagsType = (((reverseBytes(-frameLength)) & 0xFFFF_FFFFL) << 32) | headerVersionFlagsType;
            termOffsetAndSessionId = (((reverseBytes(frameOffset)) & 0xFFFF_FFFFL) << 32) | headerSessionId;
        }

        buffer.putLongOrdered(frameOffset + FRAME_LENGTH_FIELD_OFFSET, lengthVersionFlagsType);
        UnsafeAccess.UNSAFE.storeFence();

        buffer.putLong(frameOffset + TERM_OFFSET_FIELD_OFFSET, termOffsetAndSessionId);

        // read the stream(int) and term(int), this is the mutable part of the default header
        final long streamAndTermIds = defaultHeaderBuffer.getLong(STREAM_ID_FIELD_OFFSET);
        buffer.putLong(frameOffset + STREAM_ID_FIELD_OFFSET, streamAndTermIds);
    }

    private int handleEndOfLogCondition(
        final UnsafeBuffer termBuffer,
        final int frameOffset,
        final long headerVersionFlagsType,
        final long headerSessionId,
        final int capacity)
    {
        int resultingOffset = FAILED;

        if (frameOffset <= (capacity - HEADER_LENGTH))
        {
            final int paddingLength = capacity - frameOffset;
            applyDefaultHeader(
                termBuffer, frameOffset, paddingLength, headerVersionFlagsType, headerSessionId, defaultHeader);
            frameType(termBuffer, frameOffset, PADDING_FRAME_TYPE);
            frameLengthOrdered(termBuffer, frameOffset, paddingLength);

            resultingOffset = TRIPPED;
        }

        return resultingOffset;
    }
}
