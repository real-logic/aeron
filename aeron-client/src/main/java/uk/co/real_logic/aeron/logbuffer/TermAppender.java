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

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.BEGIN_FRAG_FLAG;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.END_FRAG_FLAG;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.PADDING_FRAME_TYPE;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.frameFlags;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.frameLengthOrdered;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.frameType;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.TERM_STATUS_OFFSET;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.termId;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.*;
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

    /**
     * Construct a view over a term buffer and state buffer for appending frames.
     *
     * @param termBuffer     for where messages are stored.
     * @param metaDataBuffer for where the state of writers is stored manage concurrency.
     */
    public TermAppender(final UnsafeBuffer termBuffer, final UnsafeBuffer metaDataBuffer)
    {
        this.termBuffer = termBuffer;
        this.metaDataBuffer = metaDataBuffer;
    }

    /**
     * The log of messages for a term.
     *
     * @return the log of messages for a term.
     */
    public UnsafeBuffer termBuffer()
    {
        return termBuffer;
    }

    /**
     * The meta data describing the term.
     *
     * @return the meta data describing the term.
     */
    public UnsafeBuffer metaDataBuffer()
    {
        return metaDataBuffer;
    }

    /**
     * Get the raw value current tail value in a volatile memory ordering fashion.
     *
     * @return the current tail value.
     */
    public long rawTailVolatile()
    {
        return metaDataBuffer.getLongVolatile(TERM_TAIL_COUNTER_OFFSET);
    }

    /**
     * Set the value for the tail counter.
     *
     * @param termId for the tail counter
     */
    public void tailTermId(final int termId)
    {
        metaDataBuffer.putLong(TERM_TAIL_COUNTER_OFFSET, ((long)termId) << 32);
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

    public int claim(final HeaderWriter header, final int length, final BufferClaim bufferClaim)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final long rawTail = getAndAddRawTail(alignedLength);
        final long termOffset = rawTail & 0xFFFF_FFFFL;
        final int termId = termId(rawTail);

        final UnsafeBuffer termBuffer = this.termBuffer;
        final int termLength = termBuffer.capacity();

        final long resultingOffset = termOffset + alignedLength;
        final int returnOffset;
        if (resultingOffset > (termLength - HEADER_LENGTH))
        {
            returnOffset = handleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            header.write(termBuffer, termOffset, frameLength, termId);
            bufferClaim.wrap(termBuffer, (int)termOffset, frameLength);
            returnOffset = (int)resultingOffset;
        }

        return returnOffset;
    }

    public int appendUnfragmentedMessage(
        final HeaderWriter header, final DirectBuffer srcBuffer, final int srcOffset, final int length)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final long rawTail = getAndAddRawTail(alignedLength);
        final long termOffset = rawTail & 0xFFFF_FFFFL;
        final int termId = termId(rawTail);

        final UnsafeBuffer termBuffer = this.termBuffer;
        final int termLength = termBuffer.capacity();

        final long resultingOffset = termOffset + alignedLength;
        final int returnOffset;
        if (resultingOffset > (termLength - HEADER_LENGTH))
        {
            returnOffset = handleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            header.write(termBuffer, termOffset, frameLength, termId);
            termBuffer.putBytes(termOffset + HEADER_LENGTH, srcBuffer, srcOffset, length);
            frameLengthOrdered(termBuffer, (int)termOffset, frameLength);
            returnOffset = (int)resultingOffset;
        }

        return returnOffset;
    }

    public int appendFragmentedMessage(
        final HeaderWriter header,
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int length,
        final int maxPayloadLength)
    {
        final int numMaxPayloads = length / maxPayloadLength;
        final int remainingPayload = length % maxPayloadLength;
        final int lastFrameLength = remainingPayload > 0 ? align(remainingPayload + HEADER_LENGTH, FRAME_ALIGNMENT) : 0;
        final int requiredLength = (numMaxPayloads * (maxPayloadLength + HEADER_LENGTH)) + lastFrameLength;
        final long rawTail = getAndAddRawTail(requiredLength);
        final int termId = termId(rawTail);
        long termOffset = rawTail & 0xFFFF_FFFFL;

        final UnsafeBuffer termBuffer = this.termBuffer;
        final int termLength = termBuffer.capacity();

        final long resultingOffset = termOffset + requiredLength;
        final int returnOffset;
        if (resultingOffset > (termLength - HEADER_LENGTH))
        {
            returnOffset = handleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
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

                header.write(termBuffer, termOffset, frameLength, termId);
                termBuffer.putBytes(
                    termOffset + HEADER_LENGTH,
                    srcBuffer,
                    srcOffset + (length - remaining),
                    bytesToWrite);

                if (remaining <= maxPayloadLength)
                {
                    flags |= END_FRAG_FLAG;
                }

                frameFlags(termBuffer, (int)termOffset, flags);
                frameLengthOrdered(termBuffer, (int)termOffset, frameLength);

                flags = 0;
                termOffset += alignedLength;
                remaining -= bytesToWrite;
            }
            while (remaining > 0);

            returnOffset = (int)resultingOffset;
        }

        return returnOffset;
    }

    private int handleEndOfLogCondition(
        final UnsafeBuffer termBuffer,
        final long termOffset,
        final HeaderWriter header,
        final int termLength,
        final int termId)
    {
        int resultingOffset = FAILED;

        if (termOffset <= (termLength - HEADER_LENGTH))
        {
            final int offset = (int)termOffset;
            final int paddingLength = termLength - offset;
            header.write(termBuffer, offset, paddingLength, termId);
            frameType(termBuffer, offset, PADDING_FRAME_TYPE);
            frameLengthOrdered(termBuffer, offset, paddingLength);

            resultingOffset = TRIPPED;
        }

        return resultingOffset;
    }

    private long getAndAddRawTail(final int alignedLength)
    {
        return metaDataBuffer.getAndAddLong(TERM_TAIL_COUNTER_OFFSET, alignedLength);
    }
}
