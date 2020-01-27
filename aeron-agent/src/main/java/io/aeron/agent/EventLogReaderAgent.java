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

import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.MessageHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

import static io.aeron.agent.CommonEventDissector.dissectLogStartMessage;
import static io.aeron.agent.EventConfiguration.*;
import static java.lang.System.*;
import static java.nio.channels.FileChannel.open;
import static java.nio.file.StandardOpenOption.*;
import static java.time.ZoneId.systemDefault;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.BufferUtil.allocateDirectAligned;

/**
 * Simple reader of {@link EventConfiguration#EVENT_RING_BUFFER} that appends to {@link System#out} by default
 * or to file if {@link #LOG_FILENAME_PROP_NAME} System property is set.
 */
final class EventLogReaderAgent implements Agent, MessageHandler
{
    /**
     * Event Buffer length system property name. If not set then output will default to {@link System#out}.
     */
    public static final String LOG_FILENAME_PROP_NAME = "aeron.event.log.filename";

    private final StringBuilder builder = new StringBuilder();
    private ByteBuffer byteBuffer;
    private FileChannel fileChannel = null;

    EventLogReaderAgent()
    {
    }

    public void onStart()
    {
        final String filename = getProperty(LOG_FILENAME_PROP_NAME);
        if (null != filename)
        {
            try
            {
                fileChannel = open(Paths.get(filename), CREATE, APPEND, WRITE);
            }
            catch (final IOException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            byteBuffer = allocateDirectAligned(MAX_EVENT_LENGTH * 2, CACHE_LINE_LENGTH);
        }

        dissectLogStartMessage(nanoTime(), currentTimeMillis(), systemDefault(), builder);
        builder.append(lineSeparator());

        if (null == fileChannel)
        {
            out.print(builder);
        }
        else
        {
            appendEvent(builder, byteBuffer, fileChannel);
            writeBuffer(byteBuffer, fileChannel);
        }
    }

    public void onClose()
    {
        CloseHelper.close(fileChannel);
    }

    public String roleName()
    {
        return "event-log-reader";
    }

    public int doWork()
    {
        final int eventsRead = EVENT_RING_BUFFER.read(this, EVENT_READER_FRAME_LIMIT);
        if (null != byteBuffer && byteBuffer.position() > 0)
        {
            writeBuffer(byteBuffer, fileChannel);
        }

        return eventsRead;
    }

    public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
    {
        final int eventCodeTypeId = msgTypeId >> 16;
        final int eventCodeId = msgTypeId & 0xFFFF;

        builder.setLength(0);

        if (DriverEventCode.EVENT_CODE_TYPE == eventCodeTypeId)
        {
            DriverEventCode.get(eventCodeId).decode(buffer, index, builder);
        }
        else if (ArchiveEventCode.EVENT_CODE_TYPE == eventCodeTypeId)
        {
            ArchiveEventCode.get(eventCodeId).decode(buffer, index, builder);
        }
        else if (ClusterEventCode.EVENT_CODE_TYPE == eventCodeTypeId)
        {
            ClusterEventCode.get(eventCodeId).decode(buffer, index, builder);
        }
        else
        {
            builder.append("Unknown EventCodeType: ").append(eventCodeTypeId);
        }

        builder.append(lineSeparator());

        if (null == fileChannel)
        {
            out.print(builder);
        }
        else
        {
            appendEvent(builder, byteBuffer, fileChannel);
        }
    }

    private static void appendEvent(final StringBuilder builder, final ByteBuffer buffer, final FileChannel fileChannel)
    {
        final int length = builder.length();

        if (buffer.position() + length > buffer.capacity())
        {
            writeBuffer(buffer, fileChannel);
        }

        final int position = buffer.position();

        for (int i = 0, p = position; i < length; i++, p++)
        {
            buffer.put(p, (byte)builder.charAt(i));
        }

        buffer.position(position + length);
    }

    private static void writeBuffer(final ByteBuffer buffer, final FileChannel fileChannel)
    {
        try
        {
            buffer.flip();

            do
            {
                fileChannel.write(buffer);
            }
            while (buffer.remaining() > 0);
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        finally
        {
            buffer.clear();
        }
    }
}
