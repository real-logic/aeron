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
package io.aeron.agent;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.agent.CommonEventEncoder.encode;
import static io.aeron.agent.CommonEventEncoder.*;
import static io.aeron.agent.DriverEventCode.*;
import static io.aeron.agent.DriverEventEncoder.encode;
import static io.aeron.agent.DriverEventEncoder.*;
import static io.aeron.agent.EventConfiguration.DRIVER_EVENT_CODES;
import static io.aeron.agent.EventConfiguration.EVENT_RING_BUFFER;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Event logger interface used by interceptors for recording into a {@link RingBuffer} for a
 * {@link io.aeron.driver.MediaDriver} via a Java Agent.
 */
public final class DriverEventLogger
{
    public static final DriverEventLogger LOGGER = new DriverEventLogger(EVENT_RING_BUFFER);

    private final ManyToOneRingBuffer ringBuffer;

    DriverEventLogger(final ManyToOneRingBuffer ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    /**
     * Log an event for the driver.
     *
     * @param code   for the type of event.
     * @param buffer containing the encoded event.
     * @param offset in the buffer at which the event begins.
     * @param length of the encoded event.
     */
    public void log(final DriverEventCode code, final DirectBuffer buffer, final int offset, final int length)
    {
        if (DRIVER_EVENT_CODES.contains(code))
        {
            final int captureLength = captureLength(length);
            final int encodedLength = encodedLength(captureLength);

            final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
            final int index = ringBuffer.tryClaim(toEventCodeId(code), encodedLength);
            if (index > 0)
            {
                try
                {
                    encode((UnsafeBuffer)ringBuffer.buffer(), index, captureLength, length, buffer, offset);
                }
                finally
                {
                    ringBuffer.commit(index);
                }
            }
        }
    }

    /**
     * Log a frame coming in from the media.
     *
     * @param buffer      containing the frame.
     * @param offset      in the buffer at which the frame begins.
     * @param frameLength of the frame.
     * @param dstAddress  for the frame.
     */
    public void logFrameIn(
        final DirectBuffer buffer, final int offset, final int frameLength, final InetSocketAddress dstAddress)
    {
        final int length = frameLength + socketAddressLength(dstAddress);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(FRAME_IN), encodedLength);
        if (index > 0)
        {
            try
            {
                encode((UnsafeBuffer)ringBuffer.buffer(), index, captureLength, length, buffer, offset, dstAddress);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log a frame being sent out from the driver to the media.
     *
     * @param buffer     containing the frame.
     * @param dstAddress for the frame.
     */
    public void logFrameOut(final ByteBuffer buffer, final InetSocketAddress dstAddress)
    {
        final int length = buffer.remaining() + socketAddressLength(dstAddress);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(FRAME_OUT), encodedLength);
        if (index > 0)
        {
            try
            {
                encode(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    buffer,
                    buffer.position(),
                    dstAddress);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log the removal of a publication.
     *
     * @param uri       for the channel.
     * @param sessionId for the publication.
     * @param streamId  within the channel.
     */
    public void logPublicationRemoval(final String uri, final int sessionId, final int streamId)
    {
        final int length = SIZE_OF_INT * 3 + uri.length();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(REMOVE_PUBLICATION_CLEANUP), encodedLength);
        if (index > 0)
        {
            try
            {
                final UnsafeBuffer buffer = (UnsafeBuffer)ringBuffer.buffer();
                encodePublicationRemoval(buffer, index, captureLength, length, uri, sessionId, streamId);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log the removal of a subscription.
     *
     * @param uri            for the channel.
     * @param streamId       within the channel.
     * @param subscriptionId for the subscription.
     */
    public void logSubscriptionRemoval(final String uri, final int streamId, final long subscriptionId)
    {
        final int length = SIZE_OF_INT * 2 + SIZE_OF_LONG + uri.length();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(REMOVE_SUBSCRIPTION_CLEANUP), encodedLength);
        if (index > 0)
        {
            try
            {
                final UnsafeBuffer buffer = (UnsafeBuffer)ringBuffer.buffer();
                encodeSubscriptionRemoval(buffer, index, captureLength, length, uri, streamId, subscriptionId);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log the removal of an image from the driver.
     *
     * @param uri           for the channel.
     * @param sessionId     for the image.
     * @param streamId      for the image.
     * @param correlationId for the image.
     */
    public void logImageRemoval(final String uri, final int sessionId, final int streamId, final long correlationId)
    {
        final int length = SIZE_OF_INT * 3 + SIZE_OF_LONG + uri.length();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(REMOVE_IMAGE_CLEANUP), encodedLength);
        if (index > 0)
        {
            try
            {
                final UnsafeBuffer buffer = (UnsafeBuffer)ringBuffer.buffer();
                encodeImageRemoval(buffer, index, captureLength, length, uri, sessionId, streamId, correlationId);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log a generic string associated with an event.
     *
     * @param code  for the event type.
     * @param value of the string to be logged.
     */
    public void logString(final DriverEventCode code, final String value)
    {
        final int length = value.length() + SIZE_OF_INT;
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(code), encodedLength);
        if (index > 0)
        {
            try
            {
                encode((UnsafeBuffer)ringBuffer.buffer(), index, captureLength, length, value);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log an untethered subscription state change.
     *
     * @param oldState       before the change.
     * @param newState       after the change.
     * @param subscriptionId to which the change applies.
     * @param streamId       of the image.
     * @param sessionId      of the image.
     * @param <E>            type of the event.
     */
    public <E extends Enum<E>> void logUntetheredSubscriptionStateChange(
        final E oldState, final E newState, final long subscriptionId, final int streamId, final int sessionId)
    {
        final int length = untetheredSubscriptionStateChangeLength(oldState, newState);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(UNTETHERED_SUBSCRIPTION_STATE_CHANGE), encodedLength);

        if (index > 0)
        {
            try
            {
                encodeUntetheredSubscriptionStateChange(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    oldState,
                    newState,
                    subscriptionId,
                    streamId,
                    sessionId);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log an address with associated event.
     *
     * @param code    representing the event type.
     * @param address to be logged.
     */
    public void logAddress(final DriverEventCode code, final InetSocketAddress address)
    {
        final int length = socketAddressLength(address);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);

        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(toEventCodeId(code), encodedLength);
        if (index > 0)
        {
            try
            {
                encode((UnsafeBuffer)ringBuffer.buffer(), index, captureLength, length, address);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    static int toEventCodeId(final DriverEventCode code)
    {
        return EVENT_CODE_TYPE << 16 | (code.id() & 0xFFFF);
    }
}
