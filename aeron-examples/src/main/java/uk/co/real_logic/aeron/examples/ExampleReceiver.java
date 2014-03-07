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
package uk.co.real_logic.aeron.examples;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Destination;
import uk.co.real_logic.aeron.Receiver;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * Example Aeron receiver application
 */
public class ExampleReceiver
{
    public static final Destination DESTINATION = new Destination("udp://172.16.29.29:40123");
    private static final ExampleDataHandler[] CHANNELS = { new ExampleDataHandler(10), new ExampleDataHandler(20) };
    private static final ExampleEventHandler EVENTS = new ExampleEventHandler();

    public static void main(String[] args)
    {
        Executor executor = Executors.newSingleThreadExecutor();

        try
        {
            final Aeron aeron = Aeron.newSingleMediaDriver(null);
            Receiver.Builder builder = new Receiver.Builder()
                    .destination(DESTINATION);

            IntStream.range(0, CHANNELS.length).forEach(i -> builder.channel(CHANNELS[i].channelId(), CHANNELS[i]));

            builder.events(EVENTS);

            final Receiver rcv = aeron.newReceiver(builder);

            executor.execute(() ->
            {
                try
                {
                    while (true)
                    {
                        rcv.process();
                    }
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }

            });
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static class ExampleDataHandler implements Receiver.DataHandler
    {
        private final long channelId;

        public ExampleDataHandler(final long channelId)
        {
            this.channelId = channelId;
        }

        public long channelId()
        {
            return channelId;
        }

        @Override
        public void handleData(final ByteBuffer buffer, final int offset, final long sessionId, final Receiver.MessageFlags flags)
        {

        }
    }

    public static class ExampleEventHandler implements Receiver.EventHandler
    {
        public ExampleEventHandler()
        {
        }

        @Override
        public void handleNewSource(final int channelId, final long sessionId)
        {

        }

        @Override
        public void handleInactiveSource(final int channelId, final long sessionId)
        {

        }
    }
}
