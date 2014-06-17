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

import uk.co.real_logic.aeron.*;
import uk.co.real_logic.aeron.mediadriver.MediaDriver;
import uk.co.real_logic.aeron.util.BitUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.nio.ByteOrder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * Example Aeron subscriber application
 */
public class ExampleSubscriber
{
    public static final Destination DESTINATION = new Destination("udp://localhost:40123");
    private static final ExampleDataHandler[] CHANNELS = {new ExampleDataHandler(10), new ExampleDataHandler(20)};

    public static void main(final String[] args)
    {
        final ExecutorService executor = Executors.newFixedThreadPool(3);
        final Aeron.Context aeronContext = new Aeron.Context().errorHandler(ExampleSubscriber::onError);
        final Subscriber.Context subContext = new Subscriber.Context().destination(DESTINATION);
        final Subscriber.DataHandler messageHandler =
            (buffer, offset, length, sessionId) ->
            {
                final byte[] data = new byte[length];
                buffer.getBytes(offset, data);
                System.out.println("Message " + sessionId + " " + new String(data));
            };
        final Subscriber.NewSourceEventHandler newSourceHandler =
                (channelId, sessionId) -> System.out.println("new source " + sessionId + " " + channelId);
        final Subscriber.InactiveSourceEventHandler inactiveSourceHandler =
                (channelId, sessionId) -> System.out.println("inactive source " + sessionId + " " + channelId);

        // register some channels that use stateful objects
        IntStream.range(0, CHANNELS.length).forEach(i -> subContext.channel(CHANNELS[i].channelId(), CHANNELS[i]));

        // register a channel that uses a lambda
        subContext.channel(30, messageHandler)
            .newSourceEvent(newSourceHandler)
            .inactiveSourceEvent(inactiveSourceHandler);

        // make a reusable, parameterized event loop function
        final Consumer<Subscriber> loop = (subscriber) ->
        {
            try
            {
                while (true)
                {
                    subscriber.read();
                    Thread.sleep(1000);
                }
            }
            catch (final Exception ex)
            {
                ex.printStackTrace();
            }
        };

        try (final MediaDriver driver = ExampleUtil.createEmbeddedMediaDriver();
             final Aeron aeron = ExampleUtil.createAeron(aeronContext);
             final Subscriber subscriber1 = aeron.newSubscriber(subContext);
             // create a subscriber using the fluent style lambda expression
             final Subscriber subscriber2 = aeron.newSubscriber((ctx) ->
                 ctx.destination(DESTINATION).channel(100, messageHandler).newSourceEvent(newSourceHandler)))
        {
            // spin off the two subscriber threads
            executor.execute(() -> loop.accept(subscriber1));
            executor.execute(() -> loop.accept(subscriber2));

            // run aeron conductor thread from here
            aeron.run();
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }

        executor.shutdown();
    }

    public static void onError(final String destination,
                               final long sessionId,
                               final long channelId,
                               final String message,
                               final HeaderFlyweight cause)
    {
        System.err.println(message);
    }

    public static class ExampleDataHandler implements Subscriber.DataHandler
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

        public void onData(final AtomicBuffer buffer,
                           final int offset,
                           final int length,
                           final long sessionId)
        {
            final byte[] data = new byte[length];
            buffer.getBytes(offset, data);
            System.out.println("Message to channel: " + channelId() + ", from: " + sessionId + ", data (" + length +
                "@" + offset + ") <<" + new String(data) + ">>");
        }
    }
}
