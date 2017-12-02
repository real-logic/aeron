/*
 * Copyright 2014-2017 Real Logic Ltd.
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

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.MessageHandler;

import static io.aeron.agent.EventConfiguration.EVENT_READER_FRAME_LIMIT;
import static io.aeron.agent.EventConfiguration.EVENT_RING_BUFFER;

/**
 * Simpler reader of an event log which decodes the event log and sends to {@link System#out}.
 */
public class EventLogReaderAgent implements Agent, MessageHandler
{
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
        System.out.println(EventCode.get(msgTypeId).decode(buffer, index));
    }
}
