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
package uk.co.real_logic.aeron.util.event;

import uk.co.real_logic.aeron.util.IoUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Even logger interface for applications/libraries
 */
public class EventLogger
{
    private final static boolean ON;
    private final static ManyToOneRingBuffer ringBuffer;
    private final static ThreadLocal<AtomicBuffer> encodingBuffer;

    private byte[] className;

    static
    {
        ManyToOneRingBuffer tmpRingBuffer;
        MappedByteBuffer tmpBuffer;
        ThreadLocal<AtomicBuffer> tmpEncodingBuffer;
        final File bufferLocation = new File(System.getProperty(EventConfiguration.LOCATION_PROPERTY_NAME,
                                                                EventConfiguration.LOCATION_DEFAULT));

        // if can't map existing file, then turn logging off

        try
        {
            tmpBuffer = IoUtil.mapExistingFile(bufferLocation, "event-buffer");
            tmpRingBuffer = new ManyToOneRingBuffer(new AtomicBuffer(tmpBuffer));
            tmpEncodingBuffer = ThreadLocal.withInitial(
                () -> new AtomicBuffer(ByteBuffer.allocateDirect(EventConfiguration.MAX_EVENT_LENGTH)));
        }
        catch (final Exception ex)
        {
            tmpRingBuffer = null;
            tmpEncodingBuffer = null;
        }

        ringBuffer = tmpRingBuffer;
        encodingBuffer = tmpEncodingBuffer;

        // TODO: other config - like snaplen (tcpdump style)
        ON = Boolean.getBoolean(EventConfiguration.LOGGER_ON_PROPERTY_NAME) && null != tmpRingBuffer;
    }

    public EventLogger(final Class clazz)
    {
        className = clazz.getName().getBytes(StandardCharsets.UTF_8);
    }

    public byte[] classNameAsBytes()
    {
        return className;
    }

    public void log(final EventCode code, final AtomicBuffer buffer, final int offset, final int length)
    {
        if (ON)
        {
            final AtomicBuffer encodedBuffer = encodingBuffer.get();
            final int encodedLength = EventCodec.encode(encodedBuffer, buffer, offset, length);

            ringBuffer.write(code.id(), encodedBuffer, 0, encodedLength);
        }
    }

    public void log(final EventCode code, final ByteBuffer buffer, final int length)
    {
        if (ON)
        {
            final AtomicBuffer encodedBuffer = encodingBuffer.get();
            final int pos = buffer.position();
            final int encodedLength = EventCodec.encode(encodedBuffer, buffer, length);
            buffer.position(pos);

            ringBuffer.write(code.id(), encodedBuffer, 0, encodedLength);
        }
    }

    public void log(final EventCode code, final byte[] buffer, int offset, int length)
    {
        if (ON)
        {
            final AtomicBuffer encodedBuffer = encodingBuffer.get();
            final int encodingLength = EventCodec.encode(encodedBuffer, buffer, offset, length);

            ringBuffer.write(code.id(), encodedBuffer, 0, encodingLength);
        }
    }
}
