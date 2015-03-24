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

import uk.co.real_logic.aeron.common.concurrent.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.status.PositionIndicator;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight.TERM_ID_FIELD_OFFSET;

/**
 * Publication end of a channel and stream for publishing messages to subscribers.
 * <p>
 * Note: Publication instances are threadsafe and can be shared between publisher threads.
 */
public class Publication implements AutoCloseable
{
    private final ClientConductor clientConductor;
    private final LogBuffers logBuffers;
    private final String channel;
    private final int streamId;
    private final int sessionId;
    private final long registrationId;
    private final LogAppender[] logAppenders;
    private final PositionIndicator limit;
    private final UnsafeBuffer logMetaDataBuffer;
    private final int positionBitsToShift;

    private int refCount = 1;

    Publication(
        final ClientConductor clientConductor,
        final String channel,
        final int streamId,
        final int sessionId,
        final LogAppender[] logAppenders,
        final PositionIndicator limit,
        final LogBuffers logBuffers,
        final UnsafeBuffer logMetaDataBuffer,
        final long registrationId)
    {
        this.clientConductor = clientConductor;
        this.channel = channel;
        this.streamId = streamId;
        this.sessionId = sessionId;
        this.logBuffers = logBuffers;
        this.logMetaDataBuffer = logMetaDataBuffer;
        this.registrationId = registrationId;
        this.logAppenders = logAppenders;
        this.limit = limit;

        activeTermId(logMetaDataBuffer, initialTermId(logMetaDataBuffer));
        this.positionBitsToShift = Integer.numberOfTrailingZeros(logAppenders[0].termBuffer().capacity());
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
     * Session under which messages are published.
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
        return logAppenders[0].maxMessageLength();
    }

    public void close()
    {
        synchronized (clientConductor)
        {
            if (--refCount == 0)
            {
                clientConductor.releasePublication(this);
                logBuffers.close();
            }
        }
    }

    /**
     * Non-blocking publish of a buffer containing a message.
     *
     * @param buffer containing message.
     * @return true if buffer is sent otherwise false.
     */
    public boolean offer(final DirectBuffer buffer)
    {
        return offer(buffer, 0, buffer.capacity());
    }

    /**
     * Non-blocking publish of a partial buffer containing a message.
     *
     * @param buffer containing message.
     * @param offset offset in the buffer at which the encoded message begins.
     * @param length in bytes of the encoded message.
     * @return true if the message was published otherwise false.
     */
    public boolean offer(final DirectBuffer buffer, final int offset, final int length)
    {
        boolean succeeded = false;
        final int initialTermId = initialTermId(logMetaDataBuffer);
        final int activeTermId = activeTermId(logMetaDataBuffer);
        final int activeIndex = indexByTerm(initialTermId, activeTermId);
        final LogAppender logAppender = logAppenders[activeIndex];
        final int currentTail = logAppender.tailVolatile();

        if (isWithinFlowControlLimit(initialTermId, activeTermId, currentTail))
        {
            switch (logAppender.append(buffer, offset, length))
            {
                case SUCCEEDED:
                    succeeded = true;
                    break;

                case TRIPPED:
                    nextPartition(activeTermId, activeIndex);
                    break;

                case FAILED:
                    break;
            }
        }

        return succeeded;
    }

    /**
     * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
     * Once the message has been written then {@link BufferClaim#commit()} should be called thus making it available.
     *
     * <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
     *
     * <code>
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
     * </code>
     *
     * @param length      of the range to claim.
     * @param bufferClaim to be populate if the claim succeeds.
     * @return true if the claim was successful otherwise false.
     * @throws IllegalArgumentException if the length is greater than max payload length within an MTU.
     * @see uk.co.real_logic.aeron.common.concurrent.logbuffer.BufferClaim#commit()
     */
    public boolean tryClaim(final int length, final BufferClaim bufferClaim)
    {
        boolean succeeded = false;
        final int initialTermId = initialTermId(logMetaDataBuffer);
        final int activeTermId = activeTermId(logMetaDataBuffer);
        final int activeIndex = indexByTerm(initialTermId, activeTermId);
        final LogAppender logAppender = logAppenders[activeIndex];
        final int currentTail = logAppender.tailVolatile();

        if (isWithinFlowControlLimit(initialTermId, activeTermId, currentTail))
        {
            switch (logAppender.claim(length, bufferClaim))
            {
                case SUCCEEDED:
                    succeeded = true;
                    break;

                case TRIPPED:
                    nextPartition(activeTermId, activeIndex);
                    break;

                case FAILED:
                    break;
            }
        }

        return succeeded;
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

    private void nextPartition(final int activeTermId, final int activeIndex)
    {
        final int newTermId = activeTermId + 1;
        final int nextIndex = nextPartitionIndex(activeIndex);

        final LogAppender[] logAppenders = this.logAppenders;
        logAppenders[nextIndex].defaultHeader().putInt(TERM_ID_FIELD_OFFSET, newTermId, LITTLE_ENDIAN);

        final int previousIndex = previousPartitionIndex(activeIndex);
        final LogAppender previousAppender = logAppenders[previousIndex];

        // Need to advance the term id in case a publication takes an interrupt between reading the active term
        // and incrementing the tail. This covers the case of interrupt talking over one term in duration.
        previousAppender.defaultHeader().putInt(TERM_ID_FIELD_OFFSET, newTermId + 1, LITTLE_ENDIAN);

        previousAppender.statusOrdered(NEEDS_CLEANING);

        LogBufferDescriptor.activeTermId(logMetaDataBuffer, newTermId);
    }

    private boolean isWithinFlowControlLimit(final int initialTermId, final int activeTermId, final int currentTail)
    {
        return computePosition(activeTermId, currentTail, positionBitsToShift, initialTermId) < limit.position();
    }
}
