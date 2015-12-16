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

import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.logbuffer.TermAppender;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.status.ReadablePosition;

import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.*;

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
    private final int positionBitsToShift;
    private final TermAppender[] termAppenders = new TermAppender[PARTITION_COUNT];
    private final ReadablePosition positionLimit;
    private final UnsafeBuffer logMetaDataBuffer;
    private final ClientConductor clientConductor;
    private final String channel;
    private final LogBuffers logBuffers;

    private int refCount = 0;
    private volatile boolean isClosed = false;

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
        final UnsafeBuffer[] defaultFrameHeaders = defaultFrameHeaders(logMetaDataBuffer);
        final int mtuLength = mtuLength(logMetaDataBuffer);

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termAppenders[i] = new TermAppender(buffers[i], buffers[i + PARTITION_COUNT], defaultFrameHeaders[i], mtuLength);
        }

        this.clientConductor = clientConductor;
        this.channel = channel;
        this.streamId = streamId;
        this.sessionId = sessionId;
        this.initialTermId = initialTermId(logMetaDataBuffer);
        this.logBuffers = logBuffers;
        this.logMetaDataBuffer = logMetaDataBuffer;
        this.registrationId = registrationId;
        this.positionLimit = positionLimit;
        this.positionBitsToShift = Integer.numberOfTrailingZeros(logBuffers.termLength());
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
        return termAppenders[0].maxMessageLength();
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

        final int activeTermId = activeTermId(logMetaDataBuffer);
        final int currentTail = termAppenders[indexByTerm(initialTermId, activeTermId)].tailVolatile();

        return computePosition(activeTermId, currentTail, positionBitsToShift, initialTermId);
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
     * @return The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED} or {@link #ADMIN_ACTION}.
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
        if (isClosed)
        {
            return CLOSED;
        }

        final long limit = positionLimit.getVolatile();
        final int initialTermId = this.initialTermId;
        final int activeTermId = activeTermId(logMetaDataBuffer);
        final int activeIndex = indexByTerm(initialTermId, activeTermId);
        final TermAppender termAppender = termAppenders[activeIndex];
        final int currentTail = termAppender.rawTailVolatile();
        final long position = computePosition(activeTermId, currentTail, positionBitsToShift, initialTermId);
        long newPosition = BACK_PRESSURED;

        if (position < limit)
        {
            final int nextOffset = termAppender.append(buffer, offset, length);
            newPosition = newPosition(activeTermId, activeIndex, currentTail, position, nextOffset);
        }
        else if (0 == limit)
        {
            newPosition = NOT_CONNECTED;
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
     * @return The new stream position, otherwise {@link #NOT_CONNECTED}, {@link #BACK_PRESSURED} or {@link #ADMIN_ACTION}.
     * @throws IllegalArgumentException if the length is greater than max payload length within an MTU.
     * @see BufferClaim#commit()
     * @see BufferClaim#abort()
     */
    public long tryClaim(final int length, final BufferClaim bufferClaim)
    {
        if (isClosed)
        {
            return CLOSED;
        }

        final long limit = positionLimit.getVolatile();
        final int initialTermId = this.initialTermId;
        final int activeTermId = activeTermId(logMetaDataBuffer);
        final int activeIndex = indexByTerm(initialTermId, activeTermId);
        final TermAppender termAppender = termAppenders[activeIndex];
        final int currentTail = termAppender.rawTailVolatile();
        final long position = computePosition(activeTermId, currentTail, positionBitsToShift, initialTermId);
        long newPosition = BACK_PRESSURED;

        if (position < limit)
        {
            final int nextOffset = termAppender.claim(length, bufferClaim);
            newPosition = newPosition(activeTermId, activeIndex, currentTail, position, nextOffset);
        }
        else if (0 == limit)
        {
            newPosition = NOT_CONNECTED;
        }

        return newPosition;
    }

    long registrationId()
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

    private long newPosition(
        final int activeTermId, final int activeIndex, final int currentTail, final long position, final int nextOffset)
    {
        long newPosition = BACK_PRESSURED;
        if (nextOffset > 0)
        {
            newPosition = (position - currentTail) + nextOffset;
        }
        else if (nextOffset == TermAppender.TRIPPED)
        {
            final int newTermId = activeTermId + 1;
            final int nextIndex = nextPartitionIndex(activeIndex);
            final int nextNextIndex = nextPartitionIndex(nextIndex);

            defaultHeaderTermId(logMetaDataBuffer, nextIndex, newTermId);

            // Need to advance the term id in case a publication takes an interrupt
            // between reading the active term and incrementing the tail.
            // This covers the case of an interrupt taking longer than
            // the time taken to complete the current term.
            defaultHeaderTermId(logMetaDataBuffer, nextNextIndex, newTermId + 1);

            termAppenders[nextNextIndex].statusOrdered(NEEDS_CLEANING);
            LogBufferDescriptor.activeTermId(logMetaDataBuffer, newTermId);

            newPosition = ADMIN_ACTION;
        }

        return newPosition;
    }
}
