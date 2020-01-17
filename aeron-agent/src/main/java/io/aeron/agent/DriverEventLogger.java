/*
 * Copyright 2014-2020 Real Logic Limited.
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
import org.agrona.concurrent.ringbuffer.RingBuffer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.agent.CommonEventEncoder.encode;
import static io.aeron.agent.DriverEventCode.*;
import static io.aeron.agent.DriverEventEncoder.encode;
import static io.aeron.agent.DriverEventEncoder.encodeImageRemoval;
import static io.aeron.agent.DriverEventEncoder.encodePublicationRemoval;
import static io.aeron.agent.DriverEventEncoder.encodeSubscriptionRemoval;
import static io.aeron.agent.EventConfiguration.*;
import static java.lang.ThreadLocal.withInitial;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.BufferUtil.allocateDirectAligned;

/**
 * Event logger interface used by interceptors for recording into a {@link RingBuffer} for a
 * {@link io.aeron.driver.MediaDriver} via a Java Agent.
 */
public final class DriverEventLogger
{
    public static final DriverEventLogger LOGGER = new DriverEventLogger(EVENT_RING_BUFFER);

    private static final ThreadLocal<UnsafeBuffer> ENCODING_BUFFER = withInitial(
        () -> new UnsafeBuffer(allocateDirectAligned(MAX_EVENT_LENGTH, CACHE_LINE_LENGTH)));

    private final RingBuffer ringBuffer;

    DriverEventLogger(final RingBuffer ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    public void log(final DriverEventCode code, final DirectBuffer buffer, final int offset, final int length)
    {
        if (DRIVER_EVENT_CODES.contains(code))
        {
            final UnsafeBuffer encodedBuffer = ENCODING_BUFFER.get();
            final int encodedLength = encode(encodedBuffer, buffer, offset, length);

            ringBuffer.write(toEventCodeId(code), encodedBuffer, 0, encodedLength);
        }
    }

    public void logFrameIn(
        final DirectBuffer buffer, final int offset, final int length, final InetSocketAddress dstAddress)
    {
        final UnsafeBuffer encodedBuffer = ENCODING_BUFFER.get();
        final int encodedLength = encode(encodedBuffer, buffer, offset, length, dstAddress);

        ringBuffer.write(toEventCodeId(FRAME_IN), encodedBuffer, 0, encodedLength);
    }

    public void logFrameOut(final ByteBuffer buffer, final InetSocketAddress dstAddress)
    {
        final UnsafeBuffer encodedBuffer = ENCODING_BUFFER.get();
        final int encodedLength = encode(encodedBuffer, buffer, buffer.position(), buffer.remaining(), dstAddress);

        ringBuffer.write(toEventCodeId(FRAME_OUT), encodedBuffer, 0, encodedLength);
    }

    public void logPublicationRemoval(final String uri, final int sessionId, final int streamId)
    {
        final UnsafeBuffer encodedBuffer = ENCODING_BUFFER.get();
        final int encodedLength = encodePublicationRemoval(encodedBuffer, uri, sessionId, streamId);

        ringBuffer.write(toEventCodeId(REMOVE_PUBLICATION_CLEANUP), encodedBuffer, 0, encodedLength);
    }

    public void logSubscriptionRemoval(final String uri, final int streamId, final long id)
    {
        final UnsafeBuffer encodedBuffer = ENCODING_BUFFER.get();
        final int encodedLength = encodeSubscriptionRemoval(encodedBuffer, uri, streamId, id);

        ringBuffer.write(toEventCodeId(REMOVE_SUBSCRIPTION_CLEANUP), encodedBuffer, 0, encodedLength);
    }

    public void logImageRemoval(final String uri, final int sessionId, final int streamId, final long id)
    {
        final UnsafeBuffer encodedBuffer = ENCODING_BUFFER.get();
        final int encodedLength = encodeImageRemoval(encodedBuffer, uri, sessionId, streamId, id);

        ringBuffer.write(toEventCodeId(REMOVE_IMAGE_CLEANUP), encodedBuffer, 0, encodedLength);
    }

    public void logString(final DriverEventCode code, final String value)
    {
        final UnsafeBuffer encodedBuffer = ENCODING_BUFFER.get();
        final int encodingLength = encode(encodedBuffer, value);

        ringBuffer.write(toEventCodeId(code), encodedBuffer, 0, encodingLength);
    }

    public static int toEventCodeId(final DriverEventCode code)
    {
        return EVENT_CODE_TYPE << 16 | (code.id() & 0xFFFF);
    }
}
