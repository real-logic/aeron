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
package io.aeron;

import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.*;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.ReadablePosition;

import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;

/**
 * Aeron publisher API for sending messages to subscribers of a given channel and streamId pair. {@link Publication}s
 * are created via the {@link Aeron#addPublication(String, int)} {@link Aeron#addExclusivePublication(String, int)}
 * methods, and messages are sent via one of the {@link #offer(DirectBuffer)} methods.
 * <p>
 * The APIs used for tryClaim and offer are non-blocking.
 * <p>
 * <b>Note:</b> All methods are threadsafe with the exception of offer and tryClaim for the subclass
 * {@link ExclusivePublication}. In the case of {@link ConcurrentPublication} all methods are threadsafe.
 *
 * @see ConcurrentPublication
 * @see ExclusivePublication
 * @see Aeron#addPublication(String, int)
 * @see Aeron#addExclusivePublication(String, int)
 */
public abstract class Publication implements AutoCloseable
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
     * The action is an operation such as log rotation which is likely to have succeeded by the next retry attempt.
     */
    public static final long ADMIN_ACTION = -3;

    /**
     * The {@link Publication} has been closed and should no longer be used.
     */
    public static final long CLOSED = -4;

    /**
     * The offer failed due to reaching the maximum position of the stream given term buffer length times the total
     * possible number of terms.
     * <p>
     * If this happen then the publication should be closed and a new one added. To make it less likely to happen then
     * increase the term buffer length.
     */
    public static final long MAX_POSITION_EXCEEDED = -5;

    protected final long originalRegistrationId;
    protected final long registrationId;
    protected final long maxPossiblePosition;
    protected final int channelStatusId;
    protected final int streamId;
    protected final int sessionId;
    protected final int maxMessageLength;
    protected final int initialTermId;
    protected final int maxPayloadLength;
    protected final int positionBitsToShift;
    protected final int termBufferLength;
    protected volatile boolean isClosed = false;

    protected final ReadablePosition positionLimit;
    protected final UnsafeBuffer logMetaDataBuffer;
    protected final HeaderWriter headerWriter;
    protected final LogBuffers logBuffers;
    protected final ClientConductor conductor;
    protected final String channel;

    protected Publication(
        final ClientConductor clientConductor,
        final String channel,
        final int streamId,
        final int sessionId,
        final ReadablePosition positionLimit,
        final int channelStatusId,
        final LogBuffers logBuffers,
        final long originalRegistrationId,
        final long registrationId,
        final int maxMessageLength)
    {
        final UnsafeBuffer logMetaDataBuffer = logBuffers.metaDataBuffer();
        this.termBufferLength = logBuffers.termLength();
        this.maxMessageLength = maxMessageLength;
        this.maxPayloadLength = LogBufferDescriptor.mtuLength(logMetaDataBuffer) - HEADER_LENGTH;
        this.maxPossiblePosition = termBufferLength * (1L << 31L);
        this.conductor = clientConductor;
        this.channel = channel;
        this.streamId = streamId;
        this.sessionId = sessionId;
        this.initialTermId = LogBufferDescriptor.initialTermId(logMetaDataBuffer);
        this.logMetaDataBuffer = logMetaDataBuffer;
        this.originalRegistrationId = originalRegistrationId;
        this.registrationId = registrationId;
        this.positionLimit = positionLimit;
        this.channelStatusId = channelStatusId;
        this.logBuffers = logBuffers;
        this.positionBitsToShift = LogBufferDescriptor.positionBitsToShift(termBufferLength);
        this.headerWriter = HeaderWriter.newInstance(defaultFrameHeader(logMetaDataBuffer));
    }

    /**
     * Get the length in bytes for each term partition in the log buffer.
     *
     * @return the length in bytes for each term partition in the log buffer.
     */
    public int termBufferLength()
    {
        return termBufferLength;
    }

    /**
     * The maximum possible position this stream can reach due to its term buffer length.
     * <p>
     * Maximum possible position is term-length times 2^31 in bytes.
     *
     * @return the maximum possible position this stream can reach due to it term buffer length.
     */
    public long maxPossiblePosition()
    {
        return maxPossiblePosition;
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
     * Session under which messages are published. Identifies this Publication instance. Sessions are unique across
     * all active publications on a driver instance.
     *
     * @return the session id for this publication.
     */
    public int sessionId()
    {
        return sessionId;
    }

    /**
     * The initial term id assigned when this {@link Publication} was created. This can be used to determine how many
     * terms have passed since creation.
     *
     * @return the initial term id.
     */
    public int initialTermId()
    {
        return initialTermId;
    }

    /**
     * Maximum message length supported in bytes. Messages may be made of multiple fragments if greater than
     * MTU length.
     *
     * @return maximum message length supported in bytes.
     */
    public int maxMessageLength()
    {
        return maxMessageLength;
    }

    /**
     * Maximum length of a message payload that fits within a message fragment.
     * <p>
     * This is he MTU length minus the message fragment header length.
     *
     * @return maximum message fragment payload length.
     */
    public int maxPayloadLength()
    {
        return maxPayloadLength;
    }

    /**
     * Get the registration used to register this Publication with the media driver by the first publisher.
     *
     * @return original registration id
     */
    public long originalRegistrationId()
    {
        return originalRegistrationId;
    }

    /**
     * Is this Publication the original instance added to the driver? If not then it was added after another client
     * has already added the publication.
     *
     * @return true if this instance is the first added otherwise false.
     */
    public boolean isOriginal()
    {
        return originalRegistrationId == registrationId;
    }

    /**
     * Get the registration id used to register this Publication with the media driver.
     * <p>
     * If this value is different from the {@link #originalRegistrationId()} then a previous active registration exists.
     *
     * @return registration id
     */
    public long registrationId()
    {
        return registrationId;
    }

    /**
     * Has the {@link Publication} seen an active Subscriber recently?
     *
     * @return true if this {@link Publication} has recently seen an active subscriber otherwise false.
     */
    public boolean isConnected()
    {
        return !isClosed && LogBufferDescriptor.isConnected(logMetaDataBuffer);
    }

    /**
     * Release resources used by this Publication when there are no more references.
     * <p>
     * Publications are reference counted and are only truly closed when the ref count reaches zero.
     */
    public void close()
    {
        conductor.releasePublication(this);
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
     * Get the status of the media channel for this Publication.
     * <p>
     * The status will be {@link io.aeron.status.ChannelEndpointStatus#ERRORED} if a socket exception occurs on setup
     * and {@link io.aeron.status.ChannelEndpointStatus#ACTIVE} if all is well.
     *
     * @return status for the channel as one of the constants from {@link ChannelEndpointStatus} with it being
     * {@link ChannelEndpointStatus#NO_ID_ALLOCATED} if the publication is closed.
     * @see io.aeron.status.ChannelEndpointStatus
     */
    public long channelStatus()
    {
        if (isClosed)
        {
            return ChannelEndpointStatus.NO_ID_ALLOCATED;
        }

        return conductor.channelStatus(channelStatusId);
    }

    /**
     * Get the counter used to represent the channel status for this publication.
     *
     * @return the counter used to represent the channel status for this publication.
     */
    public int channelStatusId()
    {
        return channelStatusId;
    }

    /**
     * Get the current position to which the publication has advanced for this stream.
     *
     * @return the current position to which the publication has advanced for this stream or {@link #CLOSED}.
     */
    public long position()
    {
        if (isClosed)
        {
            return CLOSED;
        }

        final long rawTail = rawTailVolatile(logMetaDataBuffer);
        final int termOffset = termOffset(rawTail, termBufferLength);

        return computePosition(termId(rawTail), termOffset, positionBitsToShift, initialTermId);
    }

    /**
     * Get the position limit beyond which this {@link Publication} will be back pressured.
     * <p>
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
     * Get the counter id for the position limit after which the publication will be back pressured.
     *
     * @return the counter id for the position limit after which the publication will be back pressured.
     */
    public int positionLimitId()
    {
        return positionLimit.id();
    }

    /**
     * Non-blocking publish of a buffer containing a message.
     *
     * @param buffer containing message.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     */
    public final long offer(final DirectBuffer buffer)
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
    public final long offer(final DirectBuffer buffer, final int offset, final int length)
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
    public abstract long offer(
        DirectBuffer buffer, int offset, int length, ReservedValueSupplier reservedValueSupplier);

    /**
     * Non-blocking publish by gathering buffer vectors into a message.
     *
     * @param vectors which make up the message.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     */
    public final long offer(final DirectBufferVector[] vectors)
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
    public abstract long offer(DirectBufferVector[] vectors, ReservedValueSupplier reservedValueSupplier);

    /**
     * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
     * Once the message has been written then {@link BufferClaim#commit()} should be called thus making it available.
     * <p>
     * <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
     * If the claim is held for more than the aeron.publication.unblock.timeout system property then the driver will
     * assume the publication thread is dead and will unblock the claim thus allowing other threads to make progress
     * for {@link ConcurrentPublication} and other claims to be sent to reach end-of-stream (EOS).
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
    public abstract long tryClaim(int length, BufferClaim bufferClaim);

    /**
     * Add a destination manually to a multi-destination-cast Publication.
     *
     * @param endpointChannel for the destination to add
     */
    public void addDestination(final String endpointChannel)
    {
        if (isClosed)
        {
            throw new AeronException("Publication is closed");
        }

        conductor.addDestination(registrationId, endpointChannel);
    }

    /**
     * Remove a previously added destination manually from a multi-destination-cast Publication.
     *
     * @param endpointChannel for the destination to remove
     */
    public void removeDestination(final String endpointChannel)
    {
        if (isClosed)
        {
            throw new AeronException("Publication is closed");
        }

        conductor.removeDestination(registrationId, endpointChannel);
    }

    void internalClose()
    {
        isClosed = true;
    }

    LogBuffers logBuffers()
    {
        return logBuffers;
    }

    final long backPressureStatus(final long currentPosition, final int messageLength)
    {
        if ((currentPosition + messageLength) >= maxPossiblePosition)
        {
            return MAX_POSITION_EXCEEDED;
        }

        if (LogBufferDescriptor.isConnected(logMetaDataBuffer))
        {
            return BACK_PRESSURED;
        }

        return NOT_CONNECTED;
    }

    final void checkForMaxPayloadLength(final int length)
    {
        if (length > maxPayloadLength)
        {
            throw new IllegalArgumentException(
                "Claim exceeds maxPayloadLength of " + maxPayloadLength + ", length=" + length);
        }
    }

    final void checkForMaxMessageLength(final int length)
    {
        if (length > maxMessageLength)
        {
            throw new IllegalArgumentException(
                "Message exceeds maxMessageLength of " + maxMessageLength + ", length=" + length);
        }
    }
}
