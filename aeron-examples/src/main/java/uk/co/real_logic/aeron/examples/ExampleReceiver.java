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
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * Example Aeron receiver application
 */
public class ExampleReceiver
{
    public static final Destination DESTINATION = new Destination("udp://172.16.29.29:40123");
    private static final ExampleDataHandler[] CHANNELS = {new ExampleDataHandler(10), new ExampleDataHandler(20)};

    public static void main(String[] args)
    {
        final Executor executor = Executors.newFixedThreadPool(2);

        try
        {
            final Aeron.Builder aeronBuilder = new Aeron.Builder().errorHandler(ExampleReceiver::onError);
            final Aeron aeron = Aeron.newSingleMediaDriver(aeronBuilder);
            final Receiver.Builder builder = new Receiver.Builder().destination(DESTINATION);

            // register some channels that use stateful objects
            IntStream.range(0, CHANNELS.length).forEach(i -> builder.channel(CHANNELS[i].channelId(), CHANNELS[i]));

            // register a channel that uses a lambda
            builder.channel(30, (buffer, offset, sessionId, flags) -> { /* do something with message */ });

            // register for events using lambdas
            builder.newSourceEvent((channelId, sessionId) -> System.out.println("new source for channel"))
                   .inactiveSourceEvent((channelId, sessionId) -> System.out.println("inactive source for channel"));

            final Receiver rcv1 = aeron.newReceiver(builder);

            // create a receiver using the fluent style lambda
            final Receiver rcv2 =
                    aeron.newReceiver((bld) ->
                                      {
                                          bld.destination(DESTINATION)
                                             .channel(100, (buffer, offset, sessionId, flags) -> { /* do something */ })
                                             .newSourceEvent((channelId, sessionId) -> System.out.println("new source for channel"));
                                      });

            // make a reusable, parameterized event loop function
            final Consumer<Receiver> loop = (rcv) ->
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
            };

            // spin off the two receiver threads
            executor.execute(() -> loop.accept(rcv1));
            executor.execute(() -> loop.accept(rcv2));
        }
        catch (final Exception e)
        {
            e.printStackTrace();
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

        public void onData(final ByteBuffer buffer, final int offset, final long sessionId, final Receiver.MessageFlags flags)
        {
        }
    }
}
