/*
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron;

import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ExclusiveTermAppender;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.ReadablePosition;

import static io.aeron.logbuffer.LogBufferDescriptor.PARTITION_COUNT;
import static io.aeron.protocol.DataHeaderFlyweight.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

@SuppressWarnings("unused")
abstract class ExclusivePublicationLhsPadding extends Publication
{
    ExclusivePublicationLhsPadding(
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
    }

    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
}

abstract class ExclusivePublicationValues extends ExclusivePublicationLhsPadding
{
    ExclusivePublicationValues(
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
    }

    int termOffset;
    int termId;
    int activePartitionIndex;
    long termBeginPosition;
}

/**
 * Aeron publisher API for sending messages to subscribers of a given channel and streamId pair. ExclusivePublications
 * each get their own session id so multiple can be concurrently active on the same media driver as independent streams.
 * <p>
 * {@link ExclusivePublication}s are created via the {@link Aeron#addExclusivePublication(String, int)} method,
 * and messages are sent via one of the {@link #offer(DirectBuffer)} methods, or a
 * {@link #tryClaim(int, BufferClaim)} and {@link BufferClaim#commit()} method combination.
 * <p>
 * {@link ExclusivePublication}s have the potential to provide greater throughput than the default {@link Publication}
 * which supports concurrent access.
 * <p>
 * The APIs used for tryClaim and offer are non-blocking.
 * <p>
 * <b>Note:</b> Instances are NOT threadsafe for offer and tryClaim methods but are for the others.
 *
 * @see Aeron#addExclusivePublication(String, int)
 */
public final class ExclusivePublication extends ExclusivePublicationValues
{
    byte p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;

    private final ExclusiveTermAppender[] termAppenders = new ExclusiveTermAppender[PARTITION_COUNT];

    ExclusivePublication(
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
        final UnsafeBuffer logMetaDataBuffer = logBuffers.metaDataBuffer();

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termAppenders[i] = new ExclusiveTermAppender(buffers[i], logMetaDataBuffer, i);
        }

        final int termCount = LogBufferDescriptor.activeTermCount(logMetaDataBuffer);
        final int index = LogBufferDescriptor.indexByTermCount(termCount);
        activePartitionIndex = index;

        final long rawTail = LogBufferDescriptor.rawTail(logMetaDataBuffer, index);
        termId = LogBufferDescriptor.termId(rawTail);
        termOffset = LogBufferDescriptor.termOffset(rawTail);
        termBeginPosition = LogBufferDescriptor.computeTermBeginPosition(termId, positionBitsToShift, initialTermId);
    }

    /**
     * {@inheritDoc}
     */
    public long position()
    {
        if (isClosed)
        {
            return CLOSED;
        }

        return termBeginPosition + termOffset;
    }

    /**
     * {@inheritDoc}
     */
    public long availableWindow()
    {
        if (isClosed)
        {
            return CLOSED;
        }

        return positionLimit.getVolatile() - (termBeginPosition + termOffset);
    }

    /**
     * The current term-id of the publication.
     *
     * @return the current term-id of the publication.
     */
    public int termId()
    {
        return termId;
    }

    /**
     * The current term-offset of the publication.
     *
     * @return the current term-offset of the publication.
     */
    public int termOffset()
    {
        return termOffset;
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
                    checkPositiveLength(length);
                    result = termAppender.appendUnfragmentedMessage(
                        termId, termOffset, headerWriter, buffer, offset, length, reservedValueSupplier);
                }
                else
                {
                    checkMaxMessageLength(length);
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
            final ExclusiveTermAppender termAppender = termAppenders[activePartitionIndex];
            final long position = termBeginPosition + termOffset;
            final int length = validateAndComputeLength(lengthOne, lengthTwo);

            if (position < limit)
            {
                final int result;
                if (length <= maxPayloadLength)
                {
                    checkPositiveLength(length);
                    result = termAppender.appendUnfragmentedMessage(
                        termId,
                        termOffset,
                        headerWriter,
                        bufferOne, offsetOne, lengthOne,
                        bufferTwo, offsetTwo, lengthTwo,
                        reservedValueSupplier);
                }
                else
                {
                    checkMaxMessageLength(length);
                    result = termAppender.appendFragmentedMessage(
                        termId,
                        termOffset,
                        headerWriter,
                        bufferOne, offsetOne, lengthOne,
                        bufferTwo, offsetTwo, lengthTwo,
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
                    checkMaxMessageLength(length);
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
     * Once the message has been written then {@link BufferClaim#commit()} should be called thus making it
     * available.
     * <p>
     * <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
     * If the claim is held after the publication is closed, or the client dies, then it will be unblocked to reach
     * end-of-stream (EOS).
     * <pre>{@code
     *     final BufferClaim bufferClaim = new BufferClaim();
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
        checkMaxMessageLength(length);
        long newPosition = CLOSED;

        if (!isClosed)
        {
            final long limit = positionLimit.getVolatile();
            final ExclusiveTermAppender termAppender = termAppenders[activePartitionIndex];
            final long position = termBeginPosition + termOffset;

            if (position < limit)
            {
                checkPositiveLength(length);
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

    /**
     * Offer a block of pre-formatted message fragments directly into the current term.
     *
     * @param buffer containing the pre-formatted block of message fragments.
     * @param offset offset in the buffer at which the first fragment begins.
     * @param length in bytes of the encoded block.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     * @throws IllegalArgumentException if the length is greater than remaining size of the current term.
     * @throws IllegalArgumentException if the first frame within the block is not properly formatted, i.e. if the
     *                                  {@code streamId} is not equal to the value returned by the {@link #streamId()}
     *                                  method or if the {@code sessionId} is not equal to the value returned by the
     *                                  {@link #sessionId()} method or if the frame type is not equal to the
     *                                  {@link io.aeron.protocol.HeaderFlyweight#HDR_TYPE_DATA}.
     */
    public long offerBlock(final MutableDirectBuffer buffer, final int offset, final int length)
    {
        if (isClosed)
        {
            return CLOSED;
        }

        if (termOffset >= termBufferLength)
        {
            rotateTerm();
        }

        final long limit = positionLimit.getVolatile();
        final long position = termBeginPosition + termOffset;

        if (position < limit)
        {
            checkBlockLength(length);
            checkFirstFrame(buffer, offset);

            final ExclusiveTermAppender termAppender = termAppenders[activePartitionIndex];
            final int result = termAppender.appendBlock(termId, termOffset, buffer, offset, length);

            return newPosition(result);
        }
        else
        {
            return backPressureStatus(position, length);
        }
    }

    private void checkBlockLength(final int length)
    {
        final int remaining = termBufferLength - termOffset;
        if (length > remaining)
        {
            throw new IllegalArgumentException(
                "invalid block length " + length + ", remaining space in term is " + remaining);
        }
    }

    private void checkFirstFrame(final MutableDirectBuffer buffer, final int offset)
    {
        final int frameType = HDR_TYPE_DATA;
        final int blockTermOffset = buffer.getInt(offset + TERM_OFFSET_FIELD_OFFSET, LITTLE_ENDIAN);
        final int blockSessionId = buffer.getInt(offset + SESSION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
        final int blockStreamId = buffer.getInt(offset + STREAM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
        final int blockTermId = buffer.getInt(offset + TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
        final int blockFrameType = buffer.getShort(offset + TYPE_FIELD_OFFSET, LITTLE_ENDIAN) & 0xFFFF;

        if (blockTermOffset != termOffset ||
            blockSessionId != sessionId ||
            blockStreamId != streamId ||
            blockTermId != termId ||
            frameType != blockFrameType)
        {
            throw new IllegalArgumentException("improperly formatted block:" +
                " termOffset=" + blockTermOffset + " (expected=" + termOffset + ")," +
                " sessionId=" + blockSessionId + " (expected=" + sessionId + ")," +
                " streamId=" + blockStreamId + " (expected=" + streamId + ")," +
                " termId=" + blockTermId + " (expected=" + termId + ")," +
                " frameType=" + blockFrameType + " (expected=" + frameType + ")");
        }
    }

    private long newPosition(final int resultingOffset)
    {
        if (resultingOffset > 0)
        {
            termOffset = resultingOffset;
            return termBeginPosition + resultingOffset;
        }

        if ((termBeginPosition + termBufferLength) >= maxPossiblePosition)
        {
            return MAX_POSITION_EXCEEDED;
        }

        rotateTerm();

        return ADMIN_ACTION;
    }

    private void rotateTerm()
    {
        final int nextIndex = LogBufferDescriptor.nextPartitionIndex(activePartitionIndex);
        final int nextTermId = termId + 1;

        activePartitionIndex = nextIndex;
        termOffset = 0;
        termId = nextTermId;
        termBeginPosition += termBufferLength;

        final int termCount = nextTermId - initialTermId;

        LogBufferDescriptor.initialiseTailWithTermId(logMetaDataBuffer, nextIndex, nextTermId);
        LogBufferDescriptor.activeTermCountOrdered(logMetaDataBuffer, termCount);
    }
}
