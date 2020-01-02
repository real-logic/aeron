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
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import static io.aeron.agent.EventConfiguration.EVENT_READER_FRAME_LIMIT;
import static io.aeron.agent.EventConfiguration.EVENT_RING_BUFFER;
import static java.nio.file.StandardOpenOption.*;

/**
 * Simple reader of {@link EventConfiguration#EVENT_RING_BUFFER} that appends to {@link System#out} by default
 * or to file if {@link #LOG_FILENAME_PROP_NAME} System property is set.
 */
public class EventLogReaderAgent implements Agent, MessageHandler
{
    /**
     * Event Buffer length system property name. If not set then output will default to {@link System#out}.
     */
    public static final String LOG_FILENAME_PROP_NAME = "aeron.event.log.filename";

    private final StringBuilder builder = new StringBuilder();
    private CharsetEncoder encoder;
    private ByteBuffer byteBuffer;
    private FileChannel fileChannel = null;

    public void onStart()
    {
        final String filename = System.getProperty(LOG_FILENAME_PROP_NAME);
        if (null != filename)
        {
            try
            {
                fileChannel = FileChannel.open(Paths.get(filename), CREATE, APPEND, WRITE);
            }
            catch (final IOException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            encoder = StandardCharsets.UTF_8.newEncoder();
            byteBuffer = ByteBuffer.allocateDirect(
                EventConfiguration.MAX_EVENT_LENGTH + System.lineSeparator().length());
        }

        builder.setLength(0);
        DriverEventDissector.dissectLogStartMessage(System.nanoTime(), System.currentTimeMillis(), builder);
        builder.append(System.lineSeparator());

        if (null == fileChannel)
        {
            System.out.print(builder);
        }
        else
        {
            write(byteBuffer, fileChannel);
        }
    }

    public void onClose()
    {
        CloseHelper.close(fileChannel);
        fileChannel = null;
    }

    public String roleName()
    {
        return "event-log-reader";
    }

    public int doWork()
    {
        return EVENT_RING_BUFFER.read(this, EVENT_READER_FRAME_LIMIT);
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

        builder.append(System.lineSeparator());

        if (null == fileChannel)
        {
            System.out.print(builder);
        }
        else
        {
            write(byteBuffer, fileChannel);
        }
    }

    private void write(final ByteBuffer buffer, final FileChannel fileChannel)
    {
        try
        {
            buffer.clear();
            encoder.reset();

            final CoderResult coderResult = encoder.encode(CharBuffer.wrap(builder), buffer, false);
            if (CoderResult.UNDERFLOW != coderResult)
            {
                coderResult.throwException();
            }

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
    }
}
