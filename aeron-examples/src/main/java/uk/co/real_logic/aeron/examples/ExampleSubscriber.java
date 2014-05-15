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
import uk.co.real_logic.aeron.Subscriber;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * Example Aeron subscriber application
 */
public class ExampleSubscriber
{
    public static final Destination DESTINATION = new Destination("udp://172.16.29.29:40123");
    private static final ExampleDataHandler[] CHANNELS = {new ExampleDataHandler(10), new ExampleDataHandler(20)};

    public static void main(final String[] args)
    {
        final Executor executor = Executors.newFixedThreadPool(2);

        try
        {
            final Aeron.Context aeronContext = new Aeron.Context().errorHandler(ExampleSubscriber::onError);
            final Aeron aeron = Aeron.newSingleMediaDriver(aeronContext);
            final Subscriber.Context context = new Subscriber.Context().destination(DESTINATION);

            // register some channels that use stateful objects
            IntStream.range(0, CHANNELS.length).forEach(i -> context.channel(CHANNELS[i].channelId(), CHANNELS[i]));

            // register a channel that uses a lambda
            context.channel(30, (buffer, offset, sessionId, flags) -> { /* do something with message */ })
                   .newSourceEvent((channelId, sessionId) -> System.out.println("new source for channel"))
                   .inactiveSourceEvent((channelId, sessionId) -> System.out.println("inactive source for channel"));

            final Subscriber subscriber1 = aeron.newSubscriber(context);

            // create a subscriber using the fluent style lambda expression
            final Subscriber subscriber2 = aeron.newSubscriber(
                (ctx) ->
                {
                    ctx.destination(DESTINATION)
                       .channel(100, (buffer, offset, sessionId, flags) -> { /* do something */ })
                       .newSourceEvent((channelId, sessionId) -> System.out.println("new source for channel"));
                }
            );

            // make a reusable, parameterized event loop function
            final java.util.function.Consumer<Subscriber> loop =
                (rcv) ->
                {
                    try
                    {
                        while (true)
                        {
                            rcv.read();
                        }
                    }
                    catch (final Exception ex)
                    {
                        ex.printStackTrace();
                    }
                };

            // spin off the two subscriber threads
            executor.execute(() -> loop.accept(subscriber1));
            executor.execute(() -> loop.accept(subscriber2));
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
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
                           final long sessionId,
                           final Subscriber.MessageFlags flags)
        {
        }
    }
}
