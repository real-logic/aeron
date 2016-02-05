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
package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.logbuffer.*;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.status.ReadablePosition;

import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;

/**
 * Aeron Publisher API for sending messages to subscribers of a given channel and streamId pair. Publishers
 * are created via an {@link Aeron} object, and messages are sent via an offer method or a claim and commit
 * method combination.
 * <p>
 * The APIs used to send are all non-blocking.
 * <p>
 * Note: Publication instances are threadsafe and can be shared between publishing threads.
 * @see Aeron#addPublication(String, int)
 */
public class Publication implements AutoCloseable
{
    /**
     * The publication is not yet connected to a subscriber.
     */
    public static final long NOT_CONNECTED = -1;

    /**
     * The offer failed due to back pressure from the subscribers preventing further transmission.
     */
    public static final long BACK_PRESSURED = -2;

    /**
     * The offer failed due to an administration action and should be retried.
     */
    public static final long ADMIN_ACTION = -3;

    /**
     * The {@link Publication} has been closed and should no longer be used.
     */
    public static final long CLOSED = -4;

    private final long registrationId;
    private final int streamId;
    private final int sessionId;
    private final int initialTermId;
    private final int maxPayloadLength;
    private final int positionBitsToShift;
    private final ReadablePosition positionLimit;
    private final TermAppender[] termAppenders = new TermAppender[PARTITION_COUNT];
    private final UnsafeBuffer logMetaDataBuffer;
    private final HeaderWriter headerWriter;
    private final LogBuffers logBuffers;
    private final ClientConductor clientConductor;
    private final String channel;

    private volatile boolean isClosed = false;
    private int refCount = 0;

    Publication(
        final ClientConductor clientConductor,
        final String channel,
        final int streamId,
        final int sessionId,
        final ReadablePosition positionLimit,
        final LogBuffers logBuffers,
        final long registrationId)
    {
        final UnsafeBuffer[] buffers = logBuffers.atomicBuffers();
        final UnsafeBuffer logMetaDataBuffer = buffers[LOG_META_DATA_SECTION_INDEX];

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termAppenders[i] = new TermAppender(buffers[i], buffers[i + PARTITION_COUNT]);
        }

        this.maxPayloadLength = mtuLength(logMetaDataBuffer) - HEADER_LENGTH;
        this.clientConductor = clientConductor;
        this.channel = channel;
        this.streamId = streamId;
        this.sessionId = sessionId;
        this.initialTermId = initialTermId(logMetaDataBuffer);
        this.logMetaDataBuffer = logMetaDataBuffer;
        this.registrationId = registrationId;
        this.positionLimit = positionLimit;
        this.logBuffers = logBuffers;
        this.positionBitsToShift = Integer.numberOfTrailingZeros(logBuffers.termLength());
        this.headerWriter = new HeaderWriter(defaultFrameHeader(logMetaDataBuffer));
    }

    /**
     * Get the length in bytes for each term partition in the log buffer.
     *
     * @return the length in bytes for each term partition in the log buffer.
     */
    public int termBufferLength()
    {
        return logBuffers.termLength();
    }

    /**
     * Media address for delivery to the channel.
     *
     * @return Media address for delivery to the channel.
     */
    public String channel()
    {
        return channel;
    }

    /**
     * Stream identity for scoping within the channel media address.
     *
     * @return Stream identity for scoping within the channel media address.
     */
    public int streamId()
    {
        return streamId;
    }

    /**
     * Session under which messages are published. Identifies this Publication instance.
     *
     * @return the session id for this publication.
     */
    public int sessionId()
    {
        return sessionId;
    }

    /**
     * Maximum message length supported in bytes.
     *
     * @return maximum message length supported in bytes.
     */
    public int maxMessageLength()
    {
        return FrameDescriptor.computeMaxMessageLength(logBuffers.termLength());
    }

    /**
     * Has this {@link Publication} been connected to a {@link Subscription}?
     *
     * @return true if this {@link Publication} been connected to a {@link Subscription} otherwise false.
     */
    public boolean hasBeenConnected()
    {
        return !isClosed && positionLimit.getVolatile() > 0;
    }

    /**
     * Has the {@link Publication} seen an active Subscriber recently?
     *
     * @return true if this {@link Publication} has seen an active subscriber otherwise false.
     */
    public boolean isStillConnected()
    {
        return clientConductor.isPublicationConnected(LogBufferDescriptor.timeOfLastSm(logMetaDataBuffer));
    }

    /**
     * Release resources used by this Publication when there are no more references.
     *
     * Publications are reference counted and are only truly closed when the ref count reaches zero.
     */
    public void close()
    {
        synchronized (clientConductor)
        {
            if (--refCount == 0)
            {
                release();
            }
        }
    }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    public boolean isClosed()
    {
        return isClosed;
    }

    /**
     * Release resources and forcibly close the Publication regardless of reference count.
     */
    void release()
    {
        if (!isClosed)
        {
            isClosed = true;
            clientConductor.releasePublication(this);
            logBuffers.close();
        }
    }

    /**
     * Get the current position to which the publication has advanced for this stream.
     *
     * @return the current position to which the publication has advanced for this stream.
     * @throws IllegalStateException if the publication is closed.
     */
    public long position()
    {
        if (isClosed)
        {
            return CLOSED;
        }

        final long rawTail = termAppenders[activePartitionIndex(logMetaDataBuffer)].rawTailVolatile();
        final int termOffset = termOffset(rawTail, logBuffers.termLength());

        return computePosition(termId(rawTail), termOffset, positionBitsToShift, initialTermId);
    }

    /**
     * Get the position limit beyond which this {@link Publication} will be back pressured.
     *
     * This should only be used as a guide to determine when back pressure is likely to be applied.
     *
     * @return the position limit beyond which this {@link Publication} will be back pressured.
     */
    public long positionLimit()
    {
        if (isClosed)
        {
            return CLOSED;
        }

        return positionLimit.getVolatile();
    }

    /**
     * Non-blocking publish of a buffer containing a message.
     *
     * @param buffer containing message.
     * @return The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
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
     * @return The new stream position, otherwise a negative error value {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED} or
     * {@link #ADMIN_ACTION}.
     */
    public long offer(final DirectBuffer buffer, final int offset, final int length)
    {
        long newPosition = CLOSED;
        if (!isClosed)
        {
            final long limit = positionLimit.getVolatile();
            final int partitionIndex = activePartitionIndex(logMetaDataBuffer);
            final TermAppender termAppender = termAppenders[partitionIndex];
            final long rawTail = termAppender.rawTailVolatile();
            final long termOffset = rawTail & 0xFFFF_FFFFL;
            final long position = computeTermBeginPosition(termId(rawTail), positionBitsToShift, initialTermId) + termOffset;

            if (position < limit)
            {
                final long result;
                if (length <= maxPayloadLength)
                {
                    result = termAppender.appendUnfragmentedMessage(headerWriter, buffer, offset, length);
                }
                else
                {
                    checkForMaxMessageLength(length);
                    result = termAppender.appendFragmentedMessage(headerWriter, buffer, offset, length, maxPayloadLength);
                }

                newPosition = newPosition(partitionIndex, (int)termOffset, position, result);
            }
            else if (0 == limit)
            {
                newPosition = NOT_CONNECTED;
            }
            else
            {
                newPosition = BACK_PRESSURED;
            }
        }

        return newPosition;
    }

    /**
     * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
     * Once the message has been written then {@link BufferClaim#commit()} should be called thus making it available.
     * <p>
     * <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
     *
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
     * @return The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED},
     * {@link #ADMIN_ACTION} or {@link #CLOSED}.
     * @throws IllegalArgumentException if the length is greater than max payload length within an MTU.
     * @see BufferClaim#commit()
     * @see BufferClaim#abort()
     */
    public long tryClaim(final int length, final BufferClaim bufferClaim)
    {
        long newPosition = CLOSED;
        if (!isClosed)
        {
            checkForMaxPayloadLength(length);

            final long limit = positionLimit.getVolatile();
            final int partitionIndex = activePartitionIndex(logMetaDataBuffer);
            final TermAppender termAppender = termAppenders[partitionIndex];
            final long rawTail = termAppender.rawTailVolatile();
            final long termOffset = rawTail & 0xFFFF_FFFFL;
            final long position = computeTermBeginPosition(termId(rawTail), positionBitsToShift, initialTermId) + termOffset;

            if (position  < limit)
            {
                final long result = termAppender.claim(headerWriter, length, bufferClaim);
                newPosition = newPosition(partitionIndex, (int)termOffset, position, result);
            }
            else if (0 == limit)
            {
                newPosition = NOT_CONNECTED;
            }
            else
            {
                newPosition = BACK_PRESSURED;
            }
        }

        return newPosition;
    }

    /**
     * Return the registration id used to register this Publication with the media driver.
     *
     * @return registration id
     */
    public long registrationId()
    {
        return registrationId;
    }

    /**
     * @see Publication#close()
     */
    void incRef()
    {
        synchronized (clientConductor)
        {
            ++refCount;
        }
    }

    private long newPosition(final int index, final int currentTail, final long position, final long result)
    {
        long newPosition = ADMIN_ACTION;
        final int termOffset = TermAppender.termOffset(result);
        if (termOffset > 0)
        {
            newPosition = (position - currentTail) + termOffset;
        }
        else if (termOffset == TermAppender.TRIPPED)
        {
            final int nextIndex = nextPartitionIndex(index);
            final int nextNextIndex = nextPartitionIndex(nextIndex);

            termAppenders[nextIndex].tailTermId(TermAppender.termId(result) + 1);
            termAppenders[nextNextIndex].statusOrdered(NEEDS_CLEANING);
            LogBufferDescriptor.activePartitionIndex(logMetaDataBuffer, nextIndex);
        }

        return newPosition;
    }

    private void checkForMaxPayloadLength(final int length)
    {
        if (length > maxPayloadLength)
        {
            throw new IllegalArgumentException(String.format(
                "Claim exceeds maxPayloadLength of %d, length=%d", maxPayloadLength, length));
        }
    }

    private void checkForMaxMessageLength(final int length)
    {
        final int maxMessageLength = maxMessageLength();
        if (length > maxMessageLength)
        {
            throw new IllegalArgumentException(String.format(
                "Encoded message exceeds maxMessageLength of %d, length=%d", maxMessageLength, length));
        }
    }
}
