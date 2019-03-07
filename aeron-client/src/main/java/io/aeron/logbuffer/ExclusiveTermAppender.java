/*
 * Copyright 2014-2019 Real Logic Ltd.
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
import org.agrona.DirectBuffer;
import org.agrona.UnsafeAccess;
import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_TAIL_COUNTERS_OFFSET;
import static io.aeron.logbuffer.LogBufferDescriptor.packTail;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.DataHeaderFlyweight.RESERVED_VALUE_OFFSET;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.BitUtil.align;

/**
 * Term buffer appender which supports a single exclusive producer writing an append-only log.
 * <p>
 * <b>Note:</b> This class is NOT threadsafe.
 * <p>
 * Messages are appended to a term using a framing protocol as described in {@link FrameDescriptor}.
 * <p>
 * A default message header is applied to each message with the fields filled in for fragment flags, type, term number,
 * as appropriate.
 * <p>
 * A message of type {@link FrameDescriptor#PADDING_FRAME_TYPE} is appended at the end of the buffer if claimed
 * space is not sufficiently large to accommodate the message about to be written.
 */
public final class ExclusiveTermAppender
{
    /**
     * The append operation tripped the end of the buffer and needs to rotate.
     */
    public static final int FAILED = -1;

    private final long tailAddressOffset;
    private final UnsafeBuffer termBuffer;

    /**
     * Construct a view over a term buffer and state buffer for appending frames.
     *
     * @param termBuffer     for where messages are stored.
     * @param metaDataBuffer for where the state of writers is stored manage concurrency.
     * @param partitionIndex for this will be the active appender.
     */
    public ExclusiveTermAppender(
        final UnsafeBuffer termBuffer, final UnsafeBuffer metaDataBuffer, final int partitionIndex)
    {
        final int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (partitionIndex * SIZE_OF_LONG);
        metaDataBuffer.boundsCheck(tailCounterOffset, SIZE_OF_LONG);

        this.termBuffer = termBuffer;
        tailAddressOffset = metaDataBuffer.addressOffset() + tailCounterOffset;
    }

    /**
     * Claim length of a the term buffer for writing in the message with zero copy semantics.
     *
     * @param termId      for the current term.
     * @param termOffset  in the term at which to append.
     * @param header      for writing the default header.
     * @param length      of the message to be written.
     * @param bufferClaim to be updated with the claimed region.
     * @return the resulting offset of the term after the append on success otherwise {@link #FAILED}.
     */
    public int claim(
        final int termId,
        final int termOffset,
        final HeaderWriter header,
        final int length,
        final BufferClaim bufferClaim)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int termLength = termBuffer.capacity();

        int resultingOffset = termOffset + alignedLength;
        putRawTailOrdered(termId, resultingOffset);

        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            header.write(termBuffer, termOffset, frameLength, termId);
            bufferClaim.wrap(termBuffer, termOffset, frameLength);
        }

        return resultingOffset;
    }

    /**
     * Pad a length of the term buffer with a padding record.
     *
     * @param termId     for the current term.
     * @param termOffset in the term at which to append.
     * @param header     for writing the default header.
     * @param length     of the padding to be written.
     * @return the resulting offset of the term after success otherwise {@link #FAILED}.
     */
    public int appendPadding(
        final int termId,
        final int termOffset,
        final HeaderWriter header,
        final int length)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int termLength = termBuffer.capacity();

        int resultingOffset = termOffset + alignedLength;
        putRawTailOrdered(termId, resultingOffset);

        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            header.write(termBuffer, termOffset, frameLength, termId);
            frameType(termBuffer, termOffset, PADDING_FRAME_TYPE);
            frameLengthOrdered(termBuffer, termOffset, frameLength);
        }

        return resultingOffset;
    }

    /**
     * Append an unfragmented message to the the term buffer.
     *
     * @param termId                for the current term.
     * @param termOffset            in the term at which to append.
     * @param header                for writing the default header.
     * @param srcBuffer             containing the message.
     * @param srcOffset             at which the message begins.
     * @param length                of the message in the source buffer.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @return the resulting offset of the term after the append on success otherwise {@link #FAILED}.
     */
    public int appendUnfragmentedMessage(
        final int termId,
        final int termOffset,
        final HeaderWriter header,
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int length,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int termLength = termBuffer.capacity();

        int resultingOffset = termOffset + alignedLength;
        putRawTailOrdered(termId, resultingOffset);

        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            header.write(termBuffer, termOffset, frameLength, termId);
            termBuffer.putBytes(termOffset + HEADER_LENGTH, srcBuffer, srcOffset, length);

            if (null != reservedValueSupplier)
            {
                final long reservedValue = reservedValueSupplier.get(termBuffer, termOffset, frameLength);
                termBuffer.putLong(termOffset + RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);
            }

            frameLengthOrdered(termBuffer, termOffset, frameLength);
        }

        return resultingOffset;
    }

    /**
     * Append an unfragmented message to the the term buffer.
     *
     * @param termId                for the current term.
     * @param termOffset            in the term at which to append.
     * @param header                for writing the default header.
     * @param bufferOne             containing the first part of the message.
     * @param offsetOne             at which the first part of the message begins.
     * @param lengthOne             of the first part of the message.
     * @param bufferTwo             containing the second part of the message.
     * @param offsetTwo             at which the second part of the message begins.
     * @param lengthTwo             of the second part of the message.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @return the resulting offset of the term after the append on success otherwise {@link #FAILED}.
     */
    public int appendUnfragmentedMessage(
        final int termId,
        final int termOffset,
        final HeaderWriter header,
        final DirectBuffer bufferOne,
        final int offsetOne,
        final int lengthOne,
        final DirectBuffer bufferTwo,
        final int offsetTwo,
        final int lengthTwo,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int frameLength = lengthOne + lengthTwo + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int termLength = termBuffer.capacity();

        int resultingOffset = termOffset + alignedLength;
        putRawTailOrdered(termId, resultingOffset);

        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            header.write(termBuffer, termOffset, frameLength, termId);
            termBuffer.putBytes(termOffset + HEADER_LENGTH, bufferOne, offsetOne, lengthOne);
            termBuffer.putBytes(termOffset + HEADER_LENGTH + lengthOne, bufferTwo, offsetTwo, lengthTwo);

            if (null != reservedValueSupplier)
            {
                final long reservedValue = reservedValueSupplier.get(termBuffer, termOffset, frameLength);
                termBuffer.putLong(termOffset + RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);
            }

            frameLengthOrdered(termBuffer, termOffset, frameLength);
        }

        return resultingOffset;
    }

    /**
     * Append an unfragmented message to the the term buffer as a gathering of vectors.
     *
     * @param termId                for the current term.
     * @param termOffset            in the term at which to append.
     * @param header                for writing the default header.
     * @param vectors               to the buffers.
     * @param length                of the message as a sum of the vectors.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @return the resulting offset of the term after the append on success otherwise {@link #FAILED}.
     */
    public int appendUnfragmentedMessage(
        final int termId,
        final int termOffset,
        final HeaderWriter header,
        final DirectBufferVector[] vectors,
        final int length,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int termLength = termBuffer.capacity();

        int resultingOffset = termOffset + alignedLength;
        putRawTailOrdered(termId, resultingOffset);

        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            header.write(termBuffer, termOffset, frameLength, termId);

            int offset = termOffset + HEADER_LENGTH;
            for (final DirectBufferVector vector : vectors)
            {
                termBuffer.putBytes(offset, vector.buffer, vector.offset, vector.length);
                offset += vector.length;
            }

            if (null != reservedValueSupplier)
            {
                final long reservedValue = reservedValueSupplier.get(termBuffer, termOffset, frameLength);
                termBuffer.putLong(termOffset + RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);
            }

            frameLengthOrdered(termBuffer, termOffset, frameLength);
        }

        return resultingOffset;
    }

    /**
     * Append a fragmented message to the the term buffer.
     * The message will be split up into fragments of MTU length minus header.
     *
     * @param termId                for the current term.
     * @param termOffset            in the term at which to append.
     * @param header                for writing the default header.
     * @param srcBuffer             containing the message.
     * @param srcOffset             at which the message begins.
     * @param length                of the message in the source buffer.
     * @param maxPayloadLength      that the message will be fragmented into.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @return the resulting offset of the term after the append on success otherwise {@link #FAILED}.
     */
    public int appendFragmentedMessage(
        final int termId,
        final int termOffset,
        final HeaderWriter header,
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int length,
        final int maxPayloadLength,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int numMaxPayloads = length / maxPayloadLength;
        final int remainingPayload = length % maxPayloadLength;
        final int lastFrameLength = remainingPayload > 0 ? align(remainingPayload + HEADER_LENGTH, FRAME_ALIGNMENT) : 0;
        final int requiredLength = (numMaxPayloads * (maxPayloadLength + HEADER_LENGTH)) + lastFrameLength;
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int termLength = termBuffer.capacity();

        int resultingOffset = termOffset + requiredLength;
        putRawTailOrdered(termId, resultingOffset);

        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            int frameOffset = termOffset;
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

        return resultingOffset;
    }

    /**
     * Append a fragmented message to the the term buffer.
     * The message will be split up into fragments of MTU length minus header.
     *
     * @param termId                for the current term.
     * @param termOffset            in the term at which to append.
     * @param header                for writing the default header.
     * @param bufferOne             containing the first part of the message.
     * @param offsetOne             at which the first part of the message begins.
     * @param lengthOne             of the first part of the message.
     * @param bufferTwo             containing the second part of the message.
     * @param offsetTwo             at which the second part of the message begins.
     * @param lengthTwo             of the second part of the message.
     * @param maxPayloadLength      that the message will be fragmented into.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @return the resulting offset of the term after the append on success otherwise {@link #FAILED}.
     */
    public int appendFragmentedMessage(
        final int termId,
        final int termOffset,
        final HeaderWriter header,
        final DirectBuffer bufferOne,
        final int offsetOne,
        final int lengthOne,
        final DirectBuffer bufferTwo,
        final int offsetTwo,
        final int lengthTwo,
        final int maxPayloadLength,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int length = lengthOne + lengthTwo;
        final int numMaxPayloads = length / maxPayloadLength;
        final int remainingPayload = length % maxPayloadLength;
        final int lastFrameLength = remainingPayload > 0 ? align(remainingPayload + HEADER_LENGTH, FRAME_ALIGNMENT) : 0;
        final int requiredLength = (numMaxPayloads * (maxPayloadLength + HEADER_LENGTH)) + lastFrameLength;
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int termLength = termBuffer.capacity();

        int resultingOffset = termOffset + requiredLength;
        putRawTailOrdered(termId, resultingOffset);

        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            int frameOffset = termOffset;
            byte flags = BEGIN_FRAG_FLAG;
            int remaining = length;
            int positionOne = 0;
            int positionTwo = 0;

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
                    final int remainingOne = lengthOne - positionOne;
                    if (remainingOne > 0)
                    {
                        final int numBytes = Math.min(bytesToWrite - bytesWritten, remainingOne);
                        termBuffer.putBytes(payloadOffset, bufferOne, offsetOne + positionOne, numBytes);

                        bytesWritten += numBytes;
                        payloadOffset += numBytes;
                        positionOne += numBytes;
                    }
                    else
                    {
                        final int numBytes = Math.min(bytesToWrite - bytesWritten, lengthTwo - positionTwo);
                        termBuffer.putBytes(payloadOffset, bufferTwo, offsetTwo + positionTwo, numBytes);

                        bytesWritten += numBytes;
                        payloadOffset += numBytes;
                        positionTwo += numBytes;
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

        return resultingOffset;
    }

    /**
     * Append a fragmented message to the the term buffer.
     * The message will be split up into fragments of MTU length minus header.
     *
     * @param termId                for the current term.
     * @param termOffset            in the term at which to append.
     * @param header                for writing the default header.
     * @param vectors               to the buffers.
     * @param length                of the message in the source buffer.
     * @param maxPayloadLength      that the message will be fragmented into.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @return the resulting offset of the term after the append on success otherwise {@link #FAILED}.
     */
    public int appendFragmentedMessage(
        final int termId,
        final int termOffset,
        final HeaderWriter header,
        final DirectBufferVector[] vectors,
        final int length,
        final int maxPayloadLength,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int numMaxPayloads = length / maxPayloadLength;
        final int remainingPayload = length % maxPayloadLength;
        final int lastFrameLength = remainingPayload > 0 ? align(remainingPayload + HEADER_LENGTH, FRAME_ALIGNMENT) : 0;
        final int requiredLength = (numMaxPayloads * (maxPayloadLength + HEADER_LENGTH)) + lastFrameLength;
        final UnsafeBuffer termBuffer = this.termBuffer;
        final int termLength = termBuffer.capacity();

        int resultingOffset = termOffset + requiredLength;
        putRawTailOrdered(termId, resultingOffset);

        if (resultingOffset > termLength)
        {
            resultingOffset = handleEndOfLogCondition(termBuffer, termOffset, header, termLength, termId);
        }
        else
        {
            int frameOffset = termOffset;
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

        return resultingOffset;
    }

    private static int handleEndOfLogCondition(
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

    private void putRawTailOrdered(final int termId, final int termOffset)
    {
        UnsafeAccess.UNSAFE.putOrderedLong(null, tailAddressOffset, packTail(termId, termOffset));
    }
}
