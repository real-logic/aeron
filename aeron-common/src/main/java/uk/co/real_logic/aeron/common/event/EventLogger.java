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

import uk.co.real_logic.aeron.common.IoUtil;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

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

    private final MappedByteBuffer eventBuffer;
    private final ManyToOneRingBuffer ringBuffer;
    private final long enabledEventCodes;

    public EventLogger(final File bufferLocation, final long enabledEventCodes)
    {
        MappedByteBuffer tmpEventBuffer = null;
        try
        {
            tmpEventBuffer = IoUtil.mapExistingFile(bufferLocation, "event-buffer");
        }
        catch (final Exception ignore)
        {
        }

        if (null != tmpEventBuffer)
        {
            this.eventBuffer = tmpEventBuffer;
            this.ringBuffer = new ManyToOneRingBuffer(new AtomicBuffer(eventBuffer));
            this.enabledEventCodes = enabledEventCodes;
        }
        else
        {
            this.eventBuffer = null;
            this.ringBuffer = null;
            this.enabledEventCodes = 0L;
        }
    }

    public void close()
    {
        if (null != eventBuffer)
        {
            IoUtil.unmap(eventBuffer);
        }
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

    public void log(final EventCode code, final ByteBuffer buffer, final int offset,
                    final int length, final InetSocketAddress dstAddress)
    {
        if (isEnabled(code, enabledEventCodes))
        {
            final AtomicBuffer encodedBuffer = encodingBuffer.get();
            final int encodedLength = EventCodec.encode(encodedBuffer, buffer, offset, length, dstAddress);

            ringBuffer.write(code.id(), encodedBuffer, 0, encodedLength);
        }
    }

    public void log(final EventCode code, final String value)
    {
        if (isEnabled(code, enabledEventCodes))
        {
            logString(code, value);
        }
    }

    public void log(final EventCode code, final String format, final Object first)
    {
        if (isEnabled(code, enabledEventCodes))
        {
            logString(code, String.format(format, first));
        }
    }

    public void log(final EventCode code, final String format, final Object first, final Object second)
    {
        if (isEnabled(code, enabledEventCodes))
        {
            logString(code, String.format(format, first, second));
        }
    }

    public void log(
        final EventCode code, final String format, final Object first, final Object second, final Object third)
    {
        if (isEnabled(code, enabledEventCodes))
        {
            logString(code, String.format(format, first, second, third));
        }
    }

    private void logString(final EventCode code, final String value)
    {
        final AtomicBuffer encodedBuffer = encodingBuffer.get();
        final int encodingLength = EventCodec.encode(encodedBuffer, value);
        ringBuffer.write(code.id(), encodedBuffer, 0, encodingLength);
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

    private static boolean isEnabled(final EventCode code, final long enabledEventCodes)
    {
        final long tagBit = code.tagBit();
        return (enabledEventCodes & tagBit) == tagBit;
    }
}
