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
package io.aeron.agent;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Event logger interface used by interceptors for recording into a {@link RingBuffer}.
 */
public class EventLogger
{
    public static final long ENABLED_EVENT_CODES = EventConfiguration.getEnabledEventCodes();

    public static final boolean IS_FRAME_IN_ENABLED =
        (ENABLED_EVENT_CODES & EventCode.FRAME_IN.tagBit()) == EventCode.FRAME_IN.tagBit();

    public static final boolean IS_FRAME_OUT_ENABLED =
        (ENABLED_EVENT_CODES & EventCode.FRAME_OUT.tagBit()) == EventCode.FRAME_OUT.tagBit();

    public static final EventLogger LOGGER = new EventLogger(EventConfiguration.EVENT_RING_BUFFER);

    private static final ThreadLocal<MutableDirectBuffer> ENCODING_BUFFER = ThreadLocal.withInitial(
        () -> new UnsafeBuffer(ByteBuffer.allocateDirect(EventConfiguration.MAX_EVENT_LENGTH)));

    private final RingBuffer ringBuffer;

    public EventLogger(final RingBuffer ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    public void log(final EventCode code, final DirectBuffer buffer, final int offset, final int length)
    {
        if (EventCode.isEnabled(code, ENABLED_EVENT_CODES))
        {
            final MutableDirectBuffer encodedBuffer = ENCODING_BUFFER.get();
            final int encodedLength = EventEncoder.encode(encodedBuffer, buffer, offset, length);

            ringBuffer.write(code.id(), encodedBuffer, 0, encodedLength);
        }
    }

    public void logFrameIn(
        final DirectBuffer buffer, final int offset, final int length, final InetSocketAddress dstAddress)
    {
        if (IS_FRAME_IN_ENABLED)
        {
            final MutableDirectBuffer encodedBuffer = ENCODING_BUFFER.get();
            final int encodedLength = EventEncoder.encode(encodedBuffer, buffer, offset, length, dstAddress);

            ringBuffer.write(EventCode.FRAME_IN.id(), encodedBuffer, 0, encodedLength);
        }
    }

    public void logFrameOut(final ByteBuffer buffer, final InetSocketAddress dstAddress)
    {
        if (IS_FRAME_OUT_ENABLED)
        {
            final MutableDirectBuffer encodedBuffer = ENCODING_BUFFER.get();
            final int encodedLength = EventEncoder.encode(
                encodedBuffer, buffer, buffer.position(), buffer.remaining(), dstAddress);

            ringBuffer.write(EventCode.FRAME_OUT.id(), encodedBuffer, 0, encodedLength);
        }
    }

    public void logPublicationRemoval(final CharSequence uri, final int sessionId, final int streamId)
    {
        if (EventCode.isEnabled(EventCode.REMOVE_PUBLICATION_CLEANUP, ENABLED_EVENT_CODES))
        {
            final String msg = uri + " " + sessionId + ":" + streamId;
            logString(EventCode.REMOVE_PUBLICATION_CLEANUP, msg);
        }
    }

    public void logSubscriptionRemoval(final CharSequence uri, final int streamId, final long id)
    {
        if (EventCode.isEnabled(EventCode.REMOVE_SUBSCRIPTION_CLEANUP, ENABLED_EVENT_CODES))
        {
            final String msg = uri + " " + streamId + " [" + id + "]";
            logString(EventCode.REMOVE_SUBSCRIPTION_CLEANUP, msg);
        }
    }

    public void logImageRemoval(final CharSequence uri, final int sessionId, final int streamId, final long id)
    {
        if (EventCode.isEnabled(EventCode.REMOVE_IMAGE_CLEANUP, ENABLED_EVENT_CODES))
        {
            final String msg = uri + " " + sessionId + ":" + streamId + " [" + id + "]";
            logString(EventCode.REMOVE_IMAGE_CLEANUP, msg);
        }
    }

    public void logChannelCreated(final EventCode code, final String description)
    {
        if (EventCode.isEnabled(code, ENABLED_EVENT_CODES))
        {
            logString(code, description);
        }
    }

    private void logString(final EventCode code, final String value)
    {
        final MutableDirectBuffer encodedBuffer = ENCODING_BUFFER.get();
        final int encodingLength = EventEncoder.encode(encodedBuffer, value);

        ringBuffer.write(code.id(), encodedBuffer, 0, encodingLength);
    }
}
