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
package uk.co.real_logic.aeron.driver.event;

import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.driver.event.EventCode.*;

/**
 * Event logger interface for applications/libraries
 */
public class EventLogger
{
    public static final long ENABLED_EVENT_CODES = EventConfiguration.getEnabledEventCodes();

    public static final boolean IS_FRAME_IN_ENABLED =
        (ENABLED_EVENT_CODES & FRAME_IN.tagBit()) == FRAME_IN.tagBit();

    public static final boolean IS_FRAME_IN_DROPPED_ENABLED =
        (ENABLED_EVENT_CODES & FRAME_IN_DROPPED.tagBit()) == FRAME_IN_DROPPED.tagBit();

    public static final boolean IS_FRAME_OUT_ENABLED =
        (ENABLED_EVENT_CODES & FRAME_OUT.tagBit()) == FRAME_OUT.tagBit();

    public static final boolean IS_FRAME_LOGGING_ENABLED =
        IS_FRAME_IN_ENABLED || IS_FRAME_IN_DROPPED_ENABLED || IS_FRAME_OUT_ENABLED;

    private static final ThreadLocal<MutableDirectBuffer> ENCODING_BUFFER = ThreadLocal.withInitial(
        () -> new UnsafeBuffer(ByteBuffer.allocateDirect(EventConfiguration.MAX_EVENT_LENGTH)));

    private final ManyToOneRingBuffer ringBuffer;

    public EventLogger(final ByteBuffer buffer)
    {
        if (null != buffer)
        {
            this.ringBuffer = new ManyToOneRingBuffer(new UnsafeBuffer(buffer));
        }
        else
        {
            this.ringBuffer = null;
        }
    }

    public void log(final EventCode code, final MutableDirectBuffer buffer, final int offset, final int length)
    {
        if (isEnabled(code, ENABLED_EVENT_CODES))
        {
            final MutableDirectBuffer encodedBuffer = ENCODING_BUFFER.get();
            final int encodedLength = EventEncoder.encode(encodedBuffer, buffer, offset, length);

            ringBuffer.write(code.id(), encodedBuffer, 0, encodedLength);
        }
    }

    public void log(final EventCode code, final File file)
    {
        if (isEnabled(code, ENABLED_EVENT_CODES))
        {
            logString(code, file.toString());
        }
    }

    public void logFrameIn(
        final ByteBuffer buffer,
        final int offset,
        final int length,
        final InetSocketAddress dstAddress)
    {
        if (IS_FRAME_IN_ENABLED)
        {
            final MutableDirectBuffer encodedBuffer = ENCODING_BUFFER.get();
            final int encodedLength = EventEncoder.encode(encodedBuffer, buffer, offset, length, dstAddress);

            ringBuffer.write(FRAME_IN.id(), encodedBuffer, 0, encodedLength);
        }
    }

    public void logFrameInDropped(
        final ByteBuffer buffer,
        final int offset,
        final int length,
        final InetSocketAddress dstAddress)
    {
        if (IS_FRAME_IN_DROPPED_ENABLED)
        {
            final MutableDirectBuffer encodedBuffer = ENCODING_BUFFER.get();
            final int encodedLength = EventEncoder.encode(encodedBuffer, buffer, offset, length, dstAddress);

            ringBuffer.write(FRAME_IN_DROPPED.id(), encodedBuffer, 0, encodedLength);
        }
    }

    public void logFrameOut(final ByteBuffer buffer, final InetSocketAddress dstAddress)
    {
        if (IS_FRAME_OUT_ENABLED)
        {
            final MutableDirectBuffer encodedBuffer = ENCODING_BUFFER.get();
            final int encodedLength =
                EventEncoder.encode(encodedBuffer, buffer, buffer.position(), buffer.remaining(), dstAddress);

            ringBuffer.write(FRAME_OUT.id(), encodedBuffer, 0, encodedLength);
        }
    }

    public void logPublicationRemoval(final CharSequence uri, final int sessionId, final int streamId)
    {
        if (isEnabled(EventCode.REMOVE_PUBLICATION_CLEANUP, ENABLED_EVENT_CODES))
        {
            logString(EventCode.REMOVE_PUBLICATION_CLEANUP, String.format("%s %d:%d", uri, sessionId, streamId));
        }
    }

    public void logSubscriptionRemoval(final CharSequence uri, final int streamId, final long id)
    {
        if (isEnabled(EventCode.REMOVE_SUBSCRIPTION_CLEANUP, ENABLED_EVENT_CODES))
        {
            logString(EventCode.REMOVE_SUBSCRIPTION_CLEANUP, String.format("%s %d [%d]", uri, streamId, id));
        }
    }

    public void logImageRemoval(final CharSequence uri, final int sessionId, final int streamId, final long id)
    {
        if (isEnabled(EventCode.REMOVE_IMAGE_CLEANUP, ENABLED_EVENT_CODES))
        {
            logString(EventCode.REMOVE_IMAGE_CLEANUP, String.format("%s %d:%d [%d]", uri, sessionId, streamId, id));
        }
    }

    public void logChannelCreated(final String description)
    {
        if (isEnabled(EventCode.CHANNEL_CREATION, ENABLED_EVENT_CODES))
        {
            logString(EventCode.CHANNEL_CREATION, description);
        }
    }

    public void logException(final Throwable ex)
    {
        if (isEnabled(EXCEPTION, ENABLED_EVENT_CODES))
        {
            final MutableDirectBuffer encodedBuffer = ENCODING_BUFFER.get();
            final int encodedLength = EventEncoder.encode(encodedBuffer, ex);

            ringBuffer.write(EXCEPTION.id(), encodedBuffer, 0, encodedLength);
        }
        else
        {
            ex.printStackTrace();
        }
    }

    private void logString(final EventCode code, final String value)
    {
        final MutableDirectBuffer encodedBuffer = ENCODING_BUFFER.get();
        final int encodingLength = EventEncoder.encode(encodedBuffer, value);
        ringBuffer.write(code.id(), encodedBuffer, 0, encodingLength);
    }

    private static boolean isEnabled(final EventCode code, final long enabledEventCodes)
    {
        final long tagBit = code.tagBit();
        return (enabledEventCodes & tagBit) == tagBit;
    }
}
