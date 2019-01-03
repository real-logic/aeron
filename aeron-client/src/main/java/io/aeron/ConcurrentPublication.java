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
package io.aeron;

import io.aeron.logbuffer.*;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.ReadablePosition;

import static io.aeron.logbuffer.LogBufferDescriptor.*;

/**
 * Aeron publisher API for sending messages to subscribers of a given channel and streamId pair. {@link Publication}s
 * are created via the {@link Aeron#addPublication(String, int)} method, and messages are sent via one of the
 * {@link #offer(DirectBuffer)} methods, or a {@link #tryClaim(int, BufferClaim)} and {@link BufferClaim#commit()}
 * method combination.
 * <p>
 * The APIs used for try claim and offer are non-blocking and thread safe.
 * <p>
 * <b>Note:</b> Instances are threadsafe and can be shared between publishing threads.
 *
 * @see Aeron#addPublication(String, int)
 * @see BufferClaim
 */
public class ConcurrentPublication extends Publication
{
    private final TermAppender[] termAppenders = new TermAppender[PARTITION_COUNT];

    ConcurrentPublication(
        final ClientConductor clientConductor,
        final String channel,
        final int streamId,
        final int sessionId,
        final ReadablePosition positionLimit,
        final int channelStatusId,
        final LogBuffers logBuffers,
        final long originalRegistrationId,
        final long registrationId)
    {
        super(
            clientConductor,
            channel,
            streamId,
            sessionId,
            positionLimit,
            channelStatusId,
            logBuffers,
            originalRegistrationId,
            registrationId);

        final UnsafeBuffer[] buffers = logBuffers.duplicateTermBuffers();

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termAppenders[i] = new TermAppender(buffers[i], logMetaDataBuffer, i);
        }
    }

    public long availableWindow()
    {
        if (isClosed)
        {
            return CLOSED;
        }

        return positionLimit.getVolatile() - position();
    }

    /**
     * Non-blocking publish of a partial buffer containing a message.
     *
     * @param buffer                containing message.
     * @param offset                offset in the buffer at which the encoded message begins.
     * @param length                in bytes of the encoded message.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     */
    public long offer(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final ReservedValueSupplier reservedValueSupplier)
    {
        long newPosition = CLOSED;
        if (!isClosed)
        {
            final long limit = positionLimit.getVolatile();
            final int termCount = activeTermCount(logMetaDataBuffer);
            final TermAppender termAppender = termAppenders[indexByTermCount(termCount)];
            final long rawTail = termAppender.rawTailVolatile();
            final long termOffset = rawTail & 0xFFFF_FFFFL;
            final int termId = termId(rawTail);
            final long position = computeTermBeginPosition(termId, positionBitsToShift, initialTermId) + termOffset;

            if (termCount != (termId - initialTermId))
            {
                return ADMIN_ACTION;
            }

            if (position < limit)
            {
                final int resultingOffset;
                if (length <= maxPayloadLength)
                {
                    checkPositiveLength(length);
                    resultingOffset = termAppender.appendUnfragmentedMessage(
                        headerWriter, buffer, offset, length, reservedValueSupplier, termId);
                }
                else
                {
                    checkMaxMessageLength(length);
                    resultingOffset = termAppender.appendFragmentedMessage(
                        headerWriter, buffer, offset, length, maxPayloadLength, reservedValueSupplier, termId);
                }

                newPosition = newPosition(termCount, (int)termOffset, termId, position, resultingOffset);
            }
            else
            {
                newPosition = backPressureStatus(position, length);
            }
        }

        return newPosition;
    }


    /**
     * Non-blocking publish of a message composed of two parts, e.g. a header and encapsulated payload.
     *
     * @param bufferOne             containing the first part of the message.
     * @param offsetOne             at which the first part of the message begins.
     * @param lengthOne             of the first part of the message.
     * @param bufferTwo             containing the second part of the message.
     * @param offsetTwo             at which the second part of the message begins.
     * @param lengthTwo             of the second part of the message.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     */
    public long offer(
        final DirectBuffer bufferOne,
        final int offsetOne,
        final int lengthOne,
        final DirectBuffer bufferTwo,
        final int offsetTwo,
        final int lengthTwo,
        final ReservedValueSupplier reservedValueSupplier)
    {
        long newPosition = CLOSED;
        if (!isClosed)
        {
            final long limit = positionLimit.getVolatile();
            final int termCount = activeTermCount(logMetaDataBuffer);
            final TermAppender termAppender = termAppenders[indexByTermCount(termCount)];
            final long rawTail = termAppender.rawTailVolatile();
            final long termOffset = rawTail & 0xFFFF_FFFFL;
            final int termId = termId(rawTail);
            final long position = computeTermBeginPosition(termId, positionBitsToShift, initialTermId) + termOffset;

            if (termCount != (termId - initialTermId))
            {
                return ADMIN_ACTION;
            }

            final int length = validateAndComputeLength(lengthOne, lengthTwo);
            if (position < limit)
            {
                final int resultingOffset;
                if (length <= maxPayloadLength)
                {
                    resultingOffset = termAppender.appendUnfragmentedMessage(
                        headerWriter,
                        bufferOne, offsetOne, lengthOne,
                        bufferTwo, offsetTwo, lengthTwo,
                        reservedValueSupplier,
                        termId);
                }
                else
                {
                    checkMaxMessageLength(length);
                    resultingOffset = termAppender.appendFragmentedMessage(
                        headerWriter,
                        bufferOne, offsetOne, lengthOne,
                        bufferTwo, offsetTwo, lengthTwo,
                        maxPayloadLength,
                        reservedValueSupplier,
                        termId);
                }

                newPosition = newPosition(termCount, (int)termOffset, termId, position, resultingOffset);
            }
            else
            {
                newPosition = backPressureStatus(position, length);
            }
        }

        return newPosition;
    }

    /**
     * Non-blocking publish by gathering buffer vectors into a message.
     *
     * @param vectors               which make up the message.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     */
    public long offer(final DirectBufferVector[] vectors, final ReservedValueSupplier reservedValueSupplier)
    {
        final int length = DirectBufferVector.validateAndComputeLength(vectors);
        long newPosition = CLOSED;

        if (!isClosed)
        {
            final long limit = positionLimit.getVolatile();
            final int termCount = activeTermCount(logMetaDataBuffer);
            final TermAppender termAppender = termAppenders[indexByTermCount(termCount)];
            final long rawTail = termAppender.rawTailVolatile();
            final long termOffset = rawTail & 0xFFFF_FFFFL;
            final int termId = termId(rawTail);
            final long position = computeTermBeginPosition(termId, positionBitsToShift, initialTermId) + termOffset;

            if (termCount != (termId - initialTermId))
            {
                return ADMIN_ACTION;
            }

            if (position < limit)
            {
                final int resultingOffset;
                if (length <= maxPayloadLength)
                {
                    resultingOffset = termAppender.appendUnfragmentedMessage(
                        headerWriter, vectors, length, reservedValueSupplier, termId);
                }
                else
                {
                    checkMaxMessageLength(length);
                    resultingOffset = termAppender.appendFragmentedMessage(
                        headerWriter, vectors, length, maxPayloadLength, reservedValueSupplier, termId);
                }

                newPosition = newPosition(termCount, (int)termOffset, termId, position, resultingOffset);
            }
            else
            {
                newPosition = backPressureStatus(position, length);
            }
        }

        return newPosition;
    }

    /**
     * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
     * Once the message has been written then {@link BufferClaim#commit()} should be called thus making it available.
     * <p>
     * <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
     * If the claim is held for more than the aeron.publication.unblock.timeout system property then the driver will
     * assume the publication thread is dead and will unblock the claim thus allowing other threads to make progress or
     * to reach end-of-stream (EOS).
     * <pre>{@code
     *     final BufferClaim bufferClaim = new BufferClaim(); // Can be stored and reused to avoid allocation
     *
     *     if (publication.tryClaim(messageLength, bufferClaim) > 0L)
     *     {
     *         try
     *         {
     *              final MutableDirectBuffer buffer = bufferClaim.buffer();
     *              final int offset = bufferClaim.offset();
     *
     *              // Work with buffer directly or wrap with a flyweight
     *         }
     *         finally
     *         {
     *             bufferClaim.commit();
     *         }
     *     }
     * }</pre>
     *
     * @param length      of the range to claim, in bytes..
     * @param bufferClaim to be populated if the claim succeeds.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     * @throws IllegalArgumentException if the length is greater than {@link #maxPayloadLength()} within an MTU.
     * @see BufferClaim#commit()
     * @see BufferClaim#abort()
     */
    public long tryClaim(final int length, final BufferClaim bufferClaim)
    {
        checkPayloadLength(length);
        long newPosition = CLOSED;

        if (!isClosed)
        {
            final long limit = positionLimit.getVolatile();
            final int termCount = activeTermCount(logMetaDataBuffer);
            final TermAppender termAppender = termAppenders[indexByTermCount(termCount)];
            final long rawTail = termAppender.rawTailVolatile();
            final long termOffset = rawTail & 0xFFFF_FFFFL;
            final int termId = termId(rawTail);
            final long position = computeTermBeginPosition(termId, positionBitsToShift, initialTermId) + termOffset;

            if (termCount != (termId - initialTermId))
            {
                return ADMIN_ACTION;
            }

            if (position < limit)
            {
                final int resultingOffset = termAppender.claim(headerWriter, length, bufferClaim, termId);
                newPosition = newPosition(termCount, (int)termOffset, termId, position, resultingOffset);
            }
            else
            {
                newPosition = backPressureStatus(position, length);
            }
        }

        return newPosition;
    }

    private long newPosition(
        final int termCount, final int termOffset, final int termId, final long position, final int resultingOffset)
    {
        if (resultingOffset > 0)
        {
            return (position - termOffset) + resultingOffset;
        }

        if ((position + termOffset) > maxPossiblePosition)
        {
            return MAX_POSITION_EXCEEDED;
        }

        rotateLog(logMetaDataBuffer, termCount, termId);

        return ADMIN_ACTION;
    }
}

