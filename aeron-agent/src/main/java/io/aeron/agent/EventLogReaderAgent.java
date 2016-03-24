/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.MessageHandler;

import java.util.function.Consumer;

public class EventLogReaderAgent implements Agent
{
    final Consumer<String> eventConsumer = System.out::println;
    final MessageHandler onEventFunc =
        (typeId, buffer, offset, length) -> eventConsumer.accept(EventCode.get(typeId).decode(buffer, offset));

    public int doWork() throws Exception
    {
        return EventConfiguration.EVENT_RING_BUFFER.read(onEventFunc, EventConfiguration.EVENT_READER_FRAME_LIMIT);
    }

    public String roleName()
    {
        return null;
    }

    public void onClose()
    {
    }
}
