/*
 * Copyright 2014-2017 Real Logic Ltd.
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

import io.aeron.logbuffer.ExclusiveBufferClaim;
import io.aeron.logbuffer.ExclusiveTermAppender;
import io.aeron.logbuffer.FrameDescriptor;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.ReadablePosition;

import static io.aeron.logbuffer.LogBufferDescriptor.*;

/**
 * Aeron publisher API for sending messages to subscribers of a given channel and streamId pair. ExclusivePublications
 * each get their own session id so multiple can be concurrently active on the same media driver as independent streams.
 * <p>
 * {@link ExclusivePublication}s are created via the {@link Aeron#addExclusivePublication(String, int)} method,
 * and messages are sent via one of the {@link #offer(DirectBuffer)} methods, or a
 * {@link #tryClaim(int, ExclusiveBufferClaim)} and {@link ExclusiveBufferClaim#commit()} method combination.
 * <p>
 * {@link ExclusivePublication}s have the potential to provide greater throughput than {@link Publication}s.
 * <p>
 * The APIs used for try claim and offer are non-blocking.
 * <p>
 * <b>Note:</b> Instances are NOT threadsafe for offer and try claim methods but are for the others.
 *
 * @see Aeron#addExclusivePublication(String, int)
 * @see ExclusiveBufferClaim
 */
public class ExclusivePublication extends Publication implements AutoCloseable
{
    private long termBeginPosition;
    private int activePartitionIndex;
    private int termId;
    private int termOffset;

    private volatile boolean isClosed = false;

    private final ExclusiveTermAppender[] termAppenders = new ExclusiveTermAppender[PARTITION_COUNT];

    ExclusivePublication(
        final ClientConductor clientConductor,
        final String channel,
        final int streamId,
        final int sessionId,
        final ReadablePosition positionLimit,
        final LogBuffers logBuffers,
        final long originalRegistrationId,
        final long registrationId)
    {
        super(clientConductor,
              channel,
              streamId,
              sessionId,
              positionLimit,
              logBuffers,
              originalRegistrationId,
              registrationId,
              FrameDescriptor.computeExclusiveMaxMessageLength(logBuffers.termLength()));

        final UnsafeBuffer[] buffers = logBuffers.duplicateTermBuffers();
        final UnsafeBuffer logMetaDataBuffer = logBuffers.metaDataBuffer();

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termAppenders[i] = new ExclusiveTermAppender(buffers[i], logMetaDataBuffer, i);
        }

        final int termCount = activeTermCount(logMetaDataBuffer);
        final int index = indexByTermCount(termCount);
        activePartitionIndex = index;

        final long rawTail = rawTail(logMetaDataBuffer, index);
        termId = termId(rawTail);
        termOffset = termOffset(rawTail);
        termBeginPosition = computeTermBeginPosition(termId, positionBitsToShift, initialTermId);
    }

    /**
     * Non-blocking publish of a buffer containing a message.
     *
     * @param buffer containing message.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     */
    public long offer(final DirectBuffer buffer)
    {
        return offer(buffer, 0, buffer.capacity());
    }

    /**
     * Non-blocking publish of a partial buffer containing a message.
     *
     * @param buffer containing message.
     * @param offset offset in the buffer at which the encoded message begins.
     * @param length in bytes of the encoded message.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     */
    public long offer(final DirectBuffer buffer, final int offset, final int length)
    {
        return offer(buffer, offset, length, null);
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
            final ExclusiveTermAppender termAppender = termAppenders[activePartitionIndex];
            final long position = termBeginPosition + termOffset;

            if (position < limit)
            {
                final int result;
                if (length <= maxPayloadLength)
                {
                    result = termAppender.appendUnfragmentedMessage(
                        termId, termOffset, headerWriter, buffer, offset, length, reservedValueSupplier);
                }
                else
                {
                    checkForMaxMessageLength(length);
                    result = termAppender.appendFragmentedMessage(
                        termId,
                        termOffset,
                        headerWriter,
                        buffer,
                        offset,
                        length,
                        maxPayloadLength,
                        reservedValueSupplier);
                }

                newPosition = newPosition(result);
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
     * @param vectors which make up the message.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     */
    public long offer(final DirectBufferVector[] vectors)
    {
        return offer(vectors, null);
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
            final ExclusiveTermAppender termAppender = termAppenders[activePartitionIndex];
            final long position = termBeginPosition + termOffset;

            if (position < limit)
            {
                final int result;
                if (length <= maxPayloadLength)
                {
                    result = termAppender.appendUnfragmentedMessage(
                        termId, termOffset, headerWriter, vectors, length, reservedValueSupplier);
                }
                else
                {
                    checkForMaxMessageLength(length);
                    result = termAppender.appendFragmentedMessage(
                        termId,
                        termOffset,
                        headerWriter,
                        vectors,
                        length,
                        maxPayloadLength,
                        reservedValueSupplier);
                }

                newPosition = newPosition(result);
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
     * Once the message has been written then {@link ExclusiveBufferClaim#commit()} should be called thus making it
     * available.
     * <p>
     * <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
     * If the claim is held for more than the aeron.publication.unblock.timeout system property then the driver will
     * assume the publication thread is dead and will unblock the claim thus allowing other threads to make progress.
     * <pre>{@code
     *     final ExclusiveBufferClaim bufferClaim = new ExclusiveBufferClaim();
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
     * @see ExclusiveBufferClaim#commit()
     * @see ExclusiveBufferClaim#abort()
     */
    public long tryClaim(final int length, final ExclusiveBufferClaim bufferClaim)
    {
        checkForMaxPayloadLength(length);
        long newPosition = CLOSED;

        if (!isClosed)
        {
            final long limit = positionLimit.getVolatile();
            final ExclusiveTermAppender termAppender = termAppenders[activePartitionIndex];
            final long position = termBeginPosition + termOffset;

            if (position < limit)
            {
                final int result = termAppender.claim(termId, termOffset, headerWriter, length, bufferClaim);
                newPosition = newPosition(result);
            }
            else
            {
                newPosition = backPressureStatus(position, length);
            }
        }

        return newPosition;
    }

    /**
     * Append a padding record log of a given length to make up the log to a position.
     *
     * @param length of the range to claim, in bytes..
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     * @throws IllegalArgumentException if the length is greater than {@link #maxMessageLength()}.
     */
    public long appendPadding(final int length)
    {
        checkForMaxMessageLength(length);
        long newPosition = CLOSED;

        if (!isClosed)
        {
            final long limit = positionLimit.getVolatile();
            final ExclusiveTermAppender termAppender = termAppenders[activePartitionIndex];
            final long position = termBeginPosition + termOffset;

            if (position < limit)
            {
                final int result = termAppender.appendPadding(termId, termOffset, headerWriter, length);
                newPosition = newPosition(result);
            }
            else
            {
                newPosition = backPressureStatus(position, length);
            }
        }

        return newPosition;
    }

    private long newPosition(final int resultingOffset)
    {
        if (resultingOffset > 0)
        {
            termOffset = resultingOffset;

            return termBeginPosition + resultingOffset;
        }

        if ((termBeginPosition + termBufferLength()) >= maxPossiblePosition)
        {
            return MAX_POSITION_EXCEEDED;
        }

        final int nextIndex = nextPartitionIndex(activePartitionIndex);
        final int nextTermId = termId + 1;

        activePartitionIndex = nextIndex;
        termOffset = 0;
        termId = nextTermId;
        termBeginPosition = computeTermBeginPosition(nextTermId, positionBitsToShift, initialTermId);

        final int termCount = nextTermId - initialTermId;

        initialiseTailWithTermId(logMetaDataBuffer, nextIndex, nextTermId);
        activeTermCountOrdered(logMetaDataBuffer, termCount);

        return ADMIN_ACTION;
    }
}
