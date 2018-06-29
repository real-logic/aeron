/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.logbuffer;

import io.aeron.DirectBufferVector;
import io.aeron.ReservedValueSupplier;
import io.aeron.exceptions.AeronException;
import org.agrona.DirectBuffer;
import org.agrona.UnsafeAccess;
import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.logbuffer.FrameDescriptor.BEGIN_FRAG_FLAG;
import static io.aeron.logbuffer.FrameDescriptor.END_FRAG_FLAG;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.FrameDescriptor.PADDING_FRAME_TYPE;
import static io.aeron.logbuffer.FrameDescriptor.frameFlags;
import static io.aeron.logbuffer.FrameDescriptor.frameLengthOrdered;
import static io.aeron.logbuffer.FrameDescriptor.frameType;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_TAIL_COUNTERS_OFFSET;
import static io.aeron.logbuffer.LogBufferDescriptor.termId;
import static io.aeron.protocol.DataHeaderFlyweight.*;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.BitUtil.align;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Term buffer appender which supports many producers concurrently writing an append-only log.
 * <p>
 * <b>Note:</b> This class is threadsafe.
 * <p>
 * Messages are appended to a term using a framing protocol as described in {@link FrameDescriptor}.
 * <p>
 * A default message header is applied to each message with the fields filled in for fragment flags, type, term number,
 * as appropriate.
 * <p>
 * A message of type {@link FrameDescriptor#PADDING_FRAME_TYPE} is appended at the end of the buffer if claimed
 * space is not sufficiently large to accommodate the message about to be written.
 */
public class TermAppender
{
    /**
     * The append operation failed because it was past the end of the buffer.
     */
    public static final int FAILED = -2;

    private final long tailAddressOffset;
    private final byte[] tailBuffer;
    private final UnsafeBuffer termBuffer;

    /**
     * Construct a view over a term buffer and state buffer for appending frames.
     *
     * @param termBuffer     for where messages are stored.
     * @param metaDataBuffer for where the state of writers is stored manage concurrency.
     * @param partitionIndex for this will be the active appender.
     */
    public TermAppender(final UnsafeBuffer termBuffer, final UnsafeBuffer metaDataBuffer, final int partitionIndex)
    {
        final int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (partitionIndex * SIZE_OF_LONG);
        metaDataBuffer.boundsCheck(tailCounterOffset, SIZE_OF_LONG);

        this.termBuffer = termBuffer;
        tailBuffer = metaDataBuffer.byteArray();
        tailAddressOffset = metaDataBuffer.addressOffset() + tailCounterOffset;
    }

    /**
     * Get the raw current tail value in a volatile memory ordering fashion.
     *
     * @return the current tail value.
     */
    public long rawTailVolatile()
    {
        return UnsafeAccess.UNSAFE.getLongVolatile(tailBuffer, tailAddressOffset);
    }

    /**
     * Claim length of a the term buffer for writing in the message with zero copy semantics.
     *
     * @param header       for writing the default header.
     * @param length       of the message to be written.
     * @param bufferClaim  to be updated with the claimed region.
     * @param activeTermId used for flow control.
     * @return the resulting offset of the term after the append on success otherwise {@link #FAILED}.
     */
    public int claim(
        final HeaderWriter header,
        final int length,
        final BufferClaim bufferClaim,
        final int activeTermId)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final long rawTail = getAndAddRawTail(alignedLength);
        final int termId = termId(rawTail);
        final long termOffset = rawTail & 0xFFFF_FFFFL;
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int termLength = termBuffer.capacity();

        checkTerm(activeTermId, termId);

        long resultingOffset = termOffset + alignedLength;
        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            final int frameOffset = (int)termOffset;
            header.write(termBuffer, frameOffset, frameLength, termId);
            bufferClaim.wrap(termBuffer, frameOffset, frameLength);
        }

        return (int)resultingOffset;
    }

    /**
     * Append an unfragmented message to the the term buffer.
     *
     * @param header                for writing the default header.
     * @param srcBuffer             containing the message.
     * @param srcOffset             at which the message begins.
     * @param length                of the message in the source buffer.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @param activeTermId          used for flow control.
     * @return the resulting offset of the term after the append on success otherwise {@link #FAILED}
     */
    public int appendUnfragmentedMessage(
        final HeaderWriter header,
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int length,
        final ReservedValueSupplier reservedValueSupplier,
        final int activeTermId)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final long rawTail = getAndAddRawTail(alignedLength);
        final int termId = termId(rawTail);
        final long termOffset = rawTail & 0xFFFF_FFFFL;
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int termLength = termBuffer.capacity();

        checkTerm(activeTermId, termId);

        long resultingOffset = termOffset + alignedLength;
        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            final int frameOffset = (int)termOffset;
            header.write(termBuffer, frameOffset, frameLength, termId);
            termBuffer.putBytes(frameOffset + HEADER_LENGTH, srcBuffer, srcOffset, length);

            if (null != reservedValueSupplier)
            {
                final long reservedValue = reservedValueSupplier.get(termBuffer, frameOffset, frameLength);
                termBuffer.putLong(frameOffset + RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);
            }

            frameLengthOrdered(termBuffer, frameOffset, frameLength);
        }

        return (int)resultingOffset;
    }

    /**
     * Append an unfragmented message to the the term buffer as a gathering of vectors.
     *
     * @param header                for writing the default header.
     * @param vectors               to the buffers.
     * @param length                of the message as a sum of the vectors.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @param activeTermId          used for flow control.
     * @return the resulting offset of the term after the append on success otherwise {@link #FAILED}.
     */
    public int appendUnfragmentedMessage(
        final HeaderWriter header,
        final DirectBufferVector[] vectors,
        final int length,
        final ReservedValueSupplier reservedValueSupplier,
        final int activeTermId)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final long rawTail = getAndAddRawTail(alignedLength);
        final int termId = termId(rawTail);
        final long termOffset = rawTail & 0xFFFF_FFFFL;
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int termLength = termBuffer.capacity();

        checkTerm(activeTermId, termId);

        long resultingOffset = termOffset + alignedLength;
        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            final int frameOffset = (int)termOffset;
            header.write(termBuffer, frameOffset, frameLength, termId);

            int offset = frameOffset + HEADER_LENGTH;
            for (final DirectBufferVector vector : vectors)
            {
                termBuffer.putBytes(offset, vector.buffer, vector.offset, vector.length);
                offset += vector.length;
            }

            if (null != reservedValueSupplier)
            {
                final long reservedValue = reservedValueSupplier.get(termBuffer, frameOffset, frameLength);
                termBuffer.putLong(frameOffset + RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);
            }

            frameLengthOrdered(termBuffer, frameOffset, frameLength);
        }

        return (int)resultingOffset;
    }

    /**
     * Append a fragmented message to the the term buffer.
     * The message will be split up into fragments of MTU length minus header.
     *
     * @param header                for writing the default header.
     * @param srcBuffer             containing the message.
     * @param srcOffset             at which the message begins.
     * @param length                of the message in the source buffer.
     * @param maxPayloadLength      that the message will be fragmented into.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @param activeTermId          used for flow control.
     * @return the resulting offset of the term after the append on success otherwise  {@link #FAILED}.
     */
    public int appendFragmentedMessage(
        final HeaderWriter header,
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int length,
        final int maxPayloadLength,
        final ReservedValueSupplier reservedValueSupplier,
        final int activeTermId)
    {
        final int numMaxPayloads = length / maxPayloadLength;
        final int remainingPayload = length % maxPayloadLength;
        final int lastFrameLength = remainingPayload > 0 ? align(remainingPayload + HEADER_LENGTH, FRAME_ALIGNMENT) : 0;
        final int requiredLength = (numMaxPayloads * (maxPayloadLength + HEADER_LENGTH)) + lastFrameLength;
        final long rawTail = getAndAddRawTail(requiredLength);
        final int termId = termId(rawTail);
        final long termOffset = rawTail & 0xFFFF_FFFFL;
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int termLength = termBuffer.capacity();

        checkTerm(activeTermId, termId);

        long resultingOffset = termOffset + requiredLength;
        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            int frameOffset = (int)termOffset;
            byte flags = BEGIN_FRAG_FLAG;
            int remaining = length;

            do
            {
                final int bytesToWrite = Math.min(remaining, maxPayloadLength);
                final int frameLength = bytesToWrite + HEADER_LENGTH;
                final int alignedLength = align(frameLength, FRAME_ALIGNMENT);

                header.write(termBuffer, frameOffset, frameLength, termId);
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

                if (null != reservedValueSupplier)
                {
                    final long reservedValue = reservedValueSupplier.get(termBuffer, frameOffset, frameLength);
                    termBuffer.putLong(frameOffset + RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);
                }

                frameLengthOrdered(termBuffer, frameOffset, frameLength);

                flags = 0;
                frameOffset += alignedLength;
                remaining -= bytesToWrite;
            }
            while (remaining > 0);
        }

        return (int)resultingOffset;
    }

    /**
     * Append a fragmented message to the the term buffer.
     * The message will be split up into fragments of MTU length minus header.
     *
     * @param header                for writing the default header.
     * @param vectors               to the buffers.
     * @param length                of the message as a sum of the vectors.
     * @param maxPayloadLength      that the message will be fragmented into.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @param activeTermId          used for flow control.
     * @return the resulting offset of the term after the append on success otherwise {@link #FAILED}.
     */
    public int appendFragmentedMessage(
        final HeaderWriter header,
        final DirectBufferVector[] vectors,
        final int length,
        final int maxPayloadLength,
        final ReservedValueSupplier reservedValueSupplier,
        final int activeTermId)
    {
        final int numMaxPayloads = length / maxPayloadLength;
        final int remainingPayload = length % maxPayloadLength;
        final int lastFrameLength = remainingPayload > 0 ? align(remainingPayload + HEADER_LENGTH, FRAME_ALIGNMENT) : 0;
        final int requiredLength = (numMaxPayloads * (maxPayloadLength + HEADER_LENGTH)) + lastFrameLength;
        final long rawTail = getAndAddRawTail(requiredLength);
        final int termId = termId(rawTail);
        final long termOffset = rawTail & 0xFFFF_FFFFL;
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int termLength = termBuffer.capacity();

        checkTerm(activeTermId, termId);

        long resultingOffset = termOffset + requiredLength;
        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            int frameOffset = (int)termOffset;
            byte flags = BEGIN_FRAG_FLAG;
            int remaining = length;
            int vectorIndex = 0;
            int vectorOffset = 0;

            do
            {
                final int bytesToWrite = Math.min(remaining, maxPayloadLength);
                final int frameLength = bytesToWrite + HEADER_LENGTH;
                final int alignedLength = align(frameLength, FRAME_ALIGNMENT);

                header.write(termBuffer, frameOffset, frameLength, termId);

                int bytesWritten = 0;
                int payloadOffset = frameOffset + HEADER_LENGTH;
                do
                {
                    final DirectBufferVector vector = vectors[vectorIndex];
                    final int vectorRemaining = vector.length - vectorOffset;
                    final int numBytes = Math.min(bytesToWrite - bytesWritten, vectorRemaining);

                    termBuffer.putBytes(payloadOffset, vector.buffer, vector.offset + vectorOffset, numBytes);

                    bytesWritten += numBytes;
                    payloadOffset += numBytes;
                    vectorOffset += numBytes;

                    if (vectorRemaining <= numBytes)
                    {
                        vectorIndex++;
                        vectorOffset = 0;
                    }
                }
                while (bytesWritten < bytesToWrite);

                if (remaining <= maxPayloadLength)
                {
                    flags |= END_FRAG_FLAG;
                }

                frameFlags(termBuffer, frameOffset, flags);

                if (null != reservedValueSupplier)
                {
                    final long reservedValue = reservedValueSupplier.get(termBuffer, frameOffset, frameLength);
                    termBuffer.putLong(frameOffset + RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);
                }

                frameLengthOrdered(termBuffer, frameOffset, frameLength);

                flags = 0;
                frameOffset += alignedLength;
                remaining -= bytesToWrite;
            }
            while (remaining > 0);
        }

        return (int)resultingOffset;
    }

    private static void checkTerm(final int expectedTermId, final int termId)
    {
        if (termId != expectedTermId)
        {
            throw new AeronException(
                "Action possibly delayed: expectedTermId=" + expectedTermId + " termId=" + termId);
        }
    }

    private int handleEndOfLogCondition(
        final UnsafeBuffer termBuffer,
        final long termOffset,
        final HeaderWriter header,
        final int termLength,
        final int termId)
    {
        if (termOffset < termLength)
        {
            final int offset = (int)termOffset;
            final int paddingLength = termLength - offset;
            header.write(termBuffer, offset, paddingLength, termId);
            frameType(termBuffer, offset, PADDING_FRAME_TYPE);
            frameLengthOrdered(termBuffer, offset, paddingLength);
        }

        return FAILED;
    }

    private long getAndAddRawTail(final int alignedLength)
    {
        return UnsafeAccess.UNSAFE.getAndAddLong(tailBuffer, tailAddressOffset, alignedLength);
    }
}
