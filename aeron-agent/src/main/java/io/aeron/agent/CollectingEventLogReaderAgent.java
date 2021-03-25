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

import org.agrona.ExpandableArrayBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

import javax.management.*;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;

import static io.aeron.agent.EventConfiguration.EVENT_READER_FRAME_LIMIT;
import static io.aeron.agent.EventLogReaderAgent.decodeLogEvent;
import static org.agrona.BitUtil.SIZE_OF_INT;

/**
 * Simple reader of {@link EventConfiguration#EVENT_RING_BUFFER} that is useful for testing. It will register
 * itself into JMX and allow users to switch on and off capture of log events in memory and allows the user
 * to periodically write them to a file.
 */
public final class CollectingEventLogReaderAgent implements Agent, CollectingEventLogReaderAgentMBean
{
    /**
     * MBean name for this logging agent.
     */
    public static final String LOGGING_MBEAN_NAME = "io.aeron:type=logging";

    enum State
    {
        COLLECTING, IGNORING, RESET
    }

    private final ManyToOneRingBuffer ringBuffer = EventConfiguration.EVENT_RING_BUFFER;
    private final ExpandableArrayBuffer collectingBuffer = new ExpandableArrayBuffer();
    private final MessageHandler messageHandler = this::onMessage;
    private final Object mutex = new Object();

    private volatile State state = State.IGNORING;
    private int bufferPosition = 0;

    CollectingEventLogReaderAgent()
    {
    }

    /**
     * {@inheritDoc}
     */
    public void onStart()
    {
        try
        {
            final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            final ObjectName oName = new ObjectName(LOGGING_MBEAN_NAME);
            mBeanServer.registerMBean(this, oName);
        }
        catch (final MalformedObjectNameException |
            InstanceAlreadyExistsException |
            MBeanRegistrationException |
            NotCompliantMBeanException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return "inmemory-event-log-reader";
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        synchronized (mutex)
        {
            return ringBuffer.read(messageHandler, EVENT_READER_FRAME_LIMIT);
        }
    }

    private void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
    {
        if (state == State.IGNORING)
        {
            return;
        }
        else if (state == State.RESET)
        {
            bufferPosition = 0;
            state = State.IGNORING;
            return;
        }

        int position = bufferPosition;

        collectingBuffer.putInt(position, msgTypeId);
        position += SIZE_OF_INT;
        collectingBuffer.putInt(position, length);
        position += SIZE_OF_INT;
        collectingBuffer.putBytes(position, buffer, index, length);
        position += length;

        bufferPosition = position;
    }

    /**
     * {@inheritDoc}
     */
    public void isCollecting(final boolean isCollecting)
    {
        this.state = isCollecting ? State.COLLECTING : State.IGNORING;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isCollecting()
    {
        return state == State.COLLECTING;
    }

    /**
     * {@inheritDoc}
     */
    public void reset()
    {
        this.state = State.RESET;
    }

    /**
     * {@inheritDoc}
     */
    public void writeToFile(final String filename)
    {
        synchronized (mutex)
        {
            doOutputToFile(filename);
        }
    }

    private void doOutputToFile(final String filename)
    {
        System.out.println("Dumping to file: " + filename);

        try (PrintStream out = new PrintStream(filename))
        {
            final StringBuilder builder = new StringBuilder();
            final int terminalPosition = bufferPosition;

            int readingPosition = 0;
            while (readingPosition < terminalPosition)
            {
                final int msgTypeId = collectingBuffer.getInt(readingPosition);
                readingPosition += SIZE_OF_INT;
                final int length = collectingBuffer.getInt(readingPosition);
                readingPosition += SIZE_OF_INT;

                final int eventCodeTypeId = msgTypeId >> 16;
                final int eventCodeId = msgTypeId & 0xFFFF;

                builder.setLength(0);
                decodeLogEvent(collectingBuffer, readingPosition, eventCodeTypeId, eventCodeId, builder);
                readingPosition += length;

                out.print(builder);
            }

            bufferPosition = 0;
        }
        catch (final IOException ex)
        {
            System.err.println("Failed to write to output log: " + ex.getMessage());
            ex.printStackTrace();
        }
    }
}
