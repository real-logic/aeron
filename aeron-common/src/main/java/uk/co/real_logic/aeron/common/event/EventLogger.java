/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.common.event;

import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.common.event.EventCode.EXCEPTION;
import static uk.co.real_logic.aeron.common.event.EventCode.INVOCATION;

/**
 * Event logger interface for applications/libraries
 */
public class EventLogger
{
    private static final ThreadLocal<AtomicBuffer> encodingBuffer = ThreadLocal.withInitial(
        () -> new AtomicBuffer(ByteBuffer.allocateDirect(EventConfiguration.MAX_EVENT_LENGTH)));

    /**
     *  The index in the stack trace of the method that called logException().
     *
     *  NB: stack[0] is Thread.currentThread().getStackTrace() and
     *  stack[1] is logException().
     */
    private static final int INVOKING_METHOD_INDEX = 2;

    private final ManyToOneRingBuffer ringBuffer;
    private final long enabledEventCodes;

    public EventLogger(final ByteBuffer buffer, final long enabledEventCodes)
    {
        this.ringBuffer = new ManyToOneRingBuffer(new AtomicBuffer(buffer));
        this.enabledEventCodes = enabledEventCodes;
    }

    public void log(final EventCode code, final AtomicBuffer buffer, final int offset, final int length)
    {
        if (isEnabled(code, enabledEventCodes))
        {
            final AtomicBuffer encodedBuffer = encodingBuffer.get();
            final int encodedLength = EventCodec.encode(encodedBuffer, buffer, offset, length);

            ringBuffer.write(code.id(), encodedBuffer, 0, encodedLength);
        }
    }

    public void log(
        final EventCode code, final ByteBuffer buffer, final int offset, final int length, final InetSocketAddress dstAddress)
    {
        if (isEnabled(code, enabledEventCodes))
        {
            final AtomicBuffer encodedBuffer = encodingBuffer.get();
            final int encodedLength = EventCodec.encode(encodedBuffer, buffer, offset, length, dstAddress);

            ringBuffer.write(code.id(), encodedBuffer, 0, encodedLength);
        }
    }

    public void log(final EventCode code, final File file)
    {
        if (isEnabled(code, enabledEventCodes))
        {
            logString(code, file.toString());
        }
    }

    public void logIncompleteSend(final CharSequence type, final int sent, final int expected)
    {
        if (isEnabled(EventCode.FRAME_OUT_INCOMPLETE_SEND, enabledEventCodes))
        {
            logString(EventCode.FRAME_OUT_INCOMPLETE_SEND, String.format("%s %d/%d", type, sent, expected));
        }
    }

    public void logPublicationRemoval(final CharSequence uri, final int sessionId, final int streamId)
    {
        if (isEnabled(EventCode.REMOVE_PUBLICATION_CLEANUP, enabledEventCodes))
        {
            logString(EventCode.REMOVE_PUBLICATION_CLEANUP, String.format("%s %x:%x", uri, sessionId, streamId));
        }
    }

    public void logSubscriptionRemoval(final CharSequence uri, final int streamId, final long id)
    {
        if (isEnabled(EventCode.REMOVE_SUBSCRIPTION_CLEANUP, enabledEventCodes))
        {
            logString(EventCode.REMOVE_SUBSCRIPTION_CLEANUP, String.format("%s %x %d", uri, streamId, id));
        }
    }

    public void logConnectionRemoval(final CharSequence uri, final int sessionId, final int streamId)
    {
        if (isEnabled(EventCode.REMOVE_CONNECTION_CLEANUP, enabledEventCodes))
        {
            logString(EventCode.REMOVE_CONNECTION_CLEANUP, String.format("%s %x:%x", uri, sessionId, streamId));
        }
    }

    public void logOverRun(final long proposedPos, final long subscriberPos, final long windowSize)
    {
        if (isEnabled(EventCode.FLOW_CONTROL_OVERRUN, enabledEventCodes))
        {
            logString(EventCode.FLOW_CONTROL_OVERRUN, String.format("%x > %x + %d", proposedPos, subscriberPos, windowSize));
        }
    }

    public void logInvocation()
    {
        if (isEnabled(INVOCATION, enabledEventCodes))
        {
            final StackTraceElement[] stack = Thread.currentThread().getStackTrace();

            final AtomicBuffer encodedBuffer = encodingBuffer.get();
            final int encodedLength = EventCodec.encode(encodedBuffer, stack[INVOKING_METHOD_INDEX]);

            ringBuffer.write(INVOCATION.id(), encodedBuffer, 0, encodedLength);
        }
    }

    public void logException(final Exception ex)
    {
        if (isEnabled(EXCEPTION, enabledEventCodes))
        {
            final AtomicBuffer encodedBuffer = encodingBuffer.get();
            final int encodedLength = EventCodec.encode(encodedBuffer, ex);

            while (!ringBuffer.write(EXCEPTION.id(), encodedBuffer, 0, encodedLength))
            {
                Thread.yield();
            }
        }
        else
        {
            ex.printStackTrace();
        }
    }

    private void logString(final EventCode code, final String value)
    {
        final AtomicBuffer encodedBuffer = encodingBuffer.get();
        final int encodingLength = EventCodec.encode(encodedBuffer, value);
        ringBuffer.write(code.id(), encodedBuffer, 0, encodingLength);
    }

    private static boolean isEnabled(final EventCode code, final long enabledEventCodes)
    {
        final long tagBit = code.tagBit();
        return (enabledEventCodes & tagBit) == tagBit;
    }
}
