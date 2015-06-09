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
 * Note: Publication instances are threadsafe and can be shared between publisher threads.
 * @see Aeron#addPublication(String, int)
 * @see Aeron#addPublication(String, int, int)
 */
public class Publication implements AutoCloseable
{
    /**
     * The publication is not yet connected to a subscriber.
     */
    public static final long NOT_CONNECTED = -1;

    /**
     * The offer failed due to back pressure preventing further transmission.
     */
    public static final long BACK_PRESSURED = -2;

    private final long registrationId;
    private final int streamId;
    private final int sessionId;
    private final String channel;
    private final ClientConductor clientConductor;
    private final LogBuffers logBuffers;
    private final TermAppender[] termAppenders = new TermAppender[PARTITION_COUNT];
    private final ReadablePosition publicationLimit;
    private final UnsafeBuffer logMetaDataBuffer;
    private final int positionBitsToShift;

    private int refCount = 0;
    private volatile boolean isClosed = false;

    Publication(
        final ClientConductor clientConductor,
        final String channel,
        final int streamId,
        final int sessionId,
        final ReadablePosition publicationLimit,
        final LogBuffers logBuffers,
        final long registrationId)
    {
        final UnsafeBuffer[] buffers = logBuffers.atomicBuffers();
        final UnsafeBuffer logMetaDataBuffer = buffers[LOG_META_DATA_SECTION_INDEX];
        final UnsafeBuffer[] defaultFrameHeaders = defaultFrameHeaders(logMetaDataBuffer);
        final int mtuLength = mtuLength(logMetaDataBuffer);
        activeTermId(logMetaDataBuffer, initialTermId(logMetaDataBuffer));

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termAppenders[i] = new TermAppender(buffers[i], buffers[i + PARTITION_COUNT], defaultFrameHeaders[i], mtuLength);
        }

        this.clientConductor = clientConductor;
        this.channel = channel;
        this.streamId = streamId;
        this.sessionId = sessionId;
        this.logBuffers = logBuffers;
        this.logMetaDataBuffer = logMetaDataBuffer;
        this.registrationId = registrationId;
        this.publicationLimit = publicationLimit;
        this.positionBitsToShift = Integer.numberOfTrailingZeros(termAppenders[0].termBuffer().capacity());
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
     * Release resources used by this Publication.
     *
     * Publications are reference counted and are only truly closed when the ref count reaches zero.
     */
    public void close()
    {
        synchronized (clientConductor)
        {
            if (--refCount == 0)
            {
                isClosed = true;
                logBuffers.close();
                clientConductor.releasePublication(this);
            }
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
        ensureOpen();

        final int initialTermId = initialTermId(logMetaDataBuffer);
        final int activeTermId = activeTermId(logMetaDataBuffer);
        final int currentTail = termAppenders[indexByTerm(initialTermId, activeTermId)].tailVolatile();

        return computePosition(activeTermId, currentTail, positionBitsToShift, initialTermId);
    }

    /**
     * Non-blocking publish of a buffer containing a message.
     *
     * @param buffer containing message.
     * @return The new stream position on success, otherwise {@link #BACK_PRESSURED} or {@link #NOT_CONNECTED}.
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
     * @return The new stream position on success, otherwise {@link #BACK_PRESSURED} or {@link #NOT_CONNECTED}.
     * @throws IllegalStateException if the publication is closed.
     */
    public long offer(final DirectBuffer buffer, final int offset, final int length)
    {
        ensureOpen();

        final int initialTermId = initialTermId(logMetaDataBuffer);
        final int activeTermId = activeTermId(logMetaDataBuffer);
        final int activeIndex = indexByTerm(initialTermId, activeTermId);
        final TermAppender termAppender = termAppenders[activeIndex];
        final int currentTail = termAppender.rawTailVolatile();
        final long position = computePosition(activeTermId, currentTail, positionBitsToShift, initialTermId);
        final int capacity = termAppender.termBuffer().capacity();

        final long limit = publicationLimit.getVolatile();
        long newPosition = limit > 0 ? BACK_PRESSURED : NOT_CONNECTED;

        if (currentTail < capacity && position < limit)
        {
            final int nextOffset = termAppender.append(buffer, offset, length);
            newPosition = newPosition(activeTermId, activeIndex, currentTail, position, nextOffset);
        }

        return newPosition;
    }

    /**
     * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
     * Once the message has been written then {@link BufferClaim#commit()} should be called thus making it available.
     * <p>
     * <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
     *U
     * <pre>{@code
     *     final BufferClaim bufferClaim = new BufferClaim(); // Can be stored and reused to avoid allocation
     *
     *     if (publication.tryClaim(messageLength, bufferClaim))
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
     * @param bufferClaim to be populate if the claim succeeds.
     * @return The new stream position on success, otherwise {@link #BACK_PRESSURED} or {@link #NOT_CONNECTED}.
     * @throws IllegalArgumentException if the length is greater than max payload length within an MTU.
     * @throws IllegalStateException if the publication is closed.
     * @see BufferClaim#commit()
     */
    public long tryClaim(final int length, final BufferClaim bufferClaim)
    {
        ensureOpen();

        final int initialTermId = initialTermId(logMetaDataBuffer);
        final int activeTermId = activeTermId(logMetaDataBuffer);
        final int activeIndex = indexByTerm(initialTermId, activeTermId);
        final TermAppender termAppender = termAppenders[activeIndex];
        final int currentTail = termAppender.rawTailVolatile();
        final long position = computePosition(activeTermId, currentTail, positionBitsToShift, initialTermId);
        final int capacity = termAppender.termBuffer().capacity();

        final long limit = publicationLimit.getVolatile();
        long newPosition = limit > 0 ? BACK_PRESSURED : NOT_CONNECTED;

        if (currentTail < capacity && position < limit)
        {
            final int nextOffset = termAppender.claim(length, bufferClaim);
            newPosition = newPosition(activeTermId, activeIndex, currentTail, position, nextOffset);
        }

        return newPosition;
    }

    long registrationId()
    {
        return registrationId;
    }

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
        final long newPosition;
        switch (nextOffset)
        {
            case TermAppender.TRIPPED:
            {
                final int newTermId = activeTermId + 1;
                final int nextIndex = nextPartitionIndex(activeIndex);
                final int nextNextIndex = nextPartitionIndex(nextIndex);

                LogBufferDescriptor.defaultHeaderTermId(logMetaDataBuffer, nextIndex, newTermId);

                // Need to advance the term id in case a publication takes an interrupt between reading the active term
                // and incrementing the tail. This covers the case of interrupt talking over one term in duration.
                LogBufferDescriptor.defaultHeaderTermId(logMetaDataBuffer, nextNextIndex, newTermId + 1);

                termAppenders[nextNextIndex].statusOrdered(NEEDS_CLEANING);
                LogBufferDescriptor.activeTermId(logMetaDataBuffer, newTermId);
            }

                // fall through
            case TermAppender.FAILED:
                newPosition = BACK_PRESSURED;
                break;

            default:
                newPosition = (position - currentTail) + nextOffset;
        }

        return newPosition;
    }

    private void ensureOpen()
    {
        if (isClosed)
        {
            throw new IllegalStateException(String.format(
                "Publication is closed: channel=%s streamId=%d sessionId=%d registrationId=%d",
                channel, streamId, sessionId, registrationId));
        }
    }
}
