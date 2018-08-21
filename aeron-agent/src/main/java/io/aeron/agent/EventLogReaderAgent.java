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
package io.aeron.agent;

import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.MessageHandler;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

import static io.aeron.agent.EventConfiguration.EVENT_READER_FRAME_LIMIT;
import static io.aeron.agent.EventConfiguration.EVENT_RING_BUFFER;

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

    private final FileOutputStream fileOutputStream;
    private final PrintStream out;
    private final StringBuilder builder = new StringBuilder();

    public EventLogReaderAgent()
    {
        final String filename = System.getProperty(LOG_FILENAME_PROP_NAME);
        if (null == filename)
        {
            fileOutputStream = null;
            out = System.out;
        }
        else
        {
            try
            {
                fileOutputStream = new FileOutputStream(filename, true);
                out = new PrintStream(fileOutputStream);
            }
            catch (final FileNotFoundException ex)
            {
                throw new RuntimeException(ex);
            }
        }
    }

    public void onClose()
    {
        out.flush();
        CloseHelper.close(fileOutputStream);
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
        builder.setLength(0);
        EventCode.get(msgTypeId).decode(buffer, index, builder);
        out.println(builder);
    }
}
