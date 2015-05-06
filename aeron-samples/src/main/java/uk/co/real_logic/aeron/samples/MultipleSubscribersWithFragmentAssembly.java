/*
 * Copyright 2015 Kaazing Corporation
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
package uk.co.real_logic.aeron.samples;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.SigInt;

/**
 * A subscriber application with two subscriptions which can receive fragmented messages
 * Creates two subscriptions on a given CHANNEL which can receive two independent streams of messages.
 * The default STREAM_ID and CHANNEL is
 * is configured at {@link SampleConfiguration}. The default
 * channel and stream can be changed by setting java system properties at the command line.
 * i.e. (-Daeron.sample.channel=udp://localhost:5555 -Daeron.sample.streamId=20)
 */
public class MultipleSubscribersWithFragmentAssembly
{
    // A unique stream id within a given channel for subscriber one
    private static final int STREAM_ID_1 = SampleConfiguration.STREAM_ID;
    // A unique stream id within a given channel for subscriber two
    private static final int STREAM_ID_2 = SampleConfiguration.STREAM_ID + 1;
    // Channel ID for the application
    private static final String CHANNEL = SampleConfiguration.CHANNEL;

    // Maximum number of fragments to be read in a single poll operation
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

    public static void main(final String[] args) throws Exception
    {
        System.out.println("Subscribing to " + CHANNEL + " on stream Id " + STREAM_ID_1 + " and stream Id " + STREAM_ID_2);

        // Create a context for a client and specify callback methods when
        // a new connection starts (eventNewConnection)
        // a connection goes inactive (eventInactiveConnection)
        final Aeron.Context ctx = new Aeron.Context()
            .newConnectionHandler(MultipleSubscribersWithFragmentAssembly::eventNewConnection)
            .inactiveConnectionHandler(MultipleSubscribersWithFragmentAssembly::eventInactiveConnection);

        // dataHandler method is called for every new datagram received
        // When a message is completely reassembled, the delegate method 'reassembledStringMessage' is called
        final FragmentAssemblyAdapter dataHandler1 = new FragmentAssemblyAdapter(reassembledStringMessage1(STREAM_ID_1));

        // Another Data handler for a different stream
        final FragmentAssemblyAdapter dataHandler2 = new FragmentAssemblyAdapter(reassembledStringMessage2(STREAM_ID_2));

        final AtomicBoolean running = new AtomicBoolean(true);

        // Register a SIGINT handler
        SigInt.register(() -> running.set(false));

        // Create an Aeron instance with client provided context configuration and connect to media driver
        try (final Aeron aeron = Aeron.connect(ctx);
            // Add a subscription to Aeron for a given channel and steam. Also,
            // supply a dataHandler to be called when data arrives
            final Subscription subscription1 = aeron.addSubscription(CHANNEL, STREAM_ID_1, dataHandler1);
            final Subscription subscription2 = aeron.addSubscription(CHANNEL, STREAM_ID_2, dataHandler2))
        {
            // Initialize a backoff strategy to avoid excessive spinning
            final IdleStrategy idleStrategy = new BackoffIdleStrategy(
                100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));

            try
            {
                // Try to read the data for both the subscribers
                while (running.get())
                {
                    final int fragmentsRead1 = subscription1.poll(FRAGMENT_COUNT_LIMIT);
                    final int fragmentsRead2 = subscription2.poll(FRAGMENT_COUNT_LIMIT);
                    // Call a backoff strategy
                    idleStrategy.idle(fragmentsRead1 + fragmentsRead2);
                }
            }
            catch (final Exception ex)
            {
                ex.printStackTrace();
            }
            System.out.println("Shutting down...");
        }
    }
    /**
     * Print the information for a new connection to stdout.
     *
     * @param channel           for the connection
     * @param streamId          for the stream
     * @param sessionId         for the connection publication
     * @param position          in the stream
     * @param sourceInformation that is transport specific
     */
    public static void eventNewConnection(
        final String channel, final int streamId, final int sessionId, final long position, final String sourceInformation)
    {
        System.out.format(
            "new connection on %s streamId %x sessionId %x from %s%n",
            channel, streamId, sessionId, sourceInformation);

    }

    /**
     * This handler is called when connection goes inactive
     *
     * @param channel   for the connection
     * @param streamId  for the stream
     * @param sessionId for the connection publication
     * @param position  within the stream
     */
    public static void eventInactiveConnection(final String channel, final int streamId, final int sessionId, final long position)
    {
        System.out.format(
            "inactive connection on %s streamId %d sessionId %x%n",
            channel, streamId, sessionId);
    }

    /**
     * Return a reusable, parameterized {@link DataHandler} that prints to stdout for the first stream(STREAM)
     *
     * @param streamId to show when printing
     * @return subscription data handler function that prints the message contents
     * @throws Exception
     */
    public static DataHandler reassembledStringMessage1(final int streamId) throws Exception
    {
        return (buffer, offset, length, header) ->
        {
            final byte[] data = new byte[length];
            buffer.getBytes(offset, data);

            System.out.format(
                "message to stream %d from session %x term id %x term offset %d (%d@%d)%n",
                streamId, header.sessionId(), header.termId(), header.termOffset(), length, offset);
            if (length != 10000)
            {
                try
                {
                    throw new Exception(
                        "Received message is not assembled properly received lenth " + length + " Expecting " + "10000");
                }
                catch (final Exception e)
                {
                    e.printStackTrace();
                }
            }
        };
    }

/**
 * Return a reusable, parameterized {@link DataHandler} that prints to stdout for the second stream (STREAM + 1)
 *
 * @param streamId to show when printing
 * @return subscription data handler function that prints the message contents
 */
public static DataHandler reassembledStringMessage2(final int streamId) throws Exception
{
    return (buffer, offset, length, header) ->
    {
        final byte[] data = new byte[length];
        buffer.getBytes(offset, data);

        System.out.format(
            "message to stream %d from session %x term id %x term offset %d (%d@%d)%n",
             streamId, header.sessionId(), header.termId(), header.termOffset(), length, offset);
        if (length != 9000)
        {
            try
            {
                throw new Exception(
                    "Received message is not assembled properly, received lenth " + length + " Expecting " + "9000");
            }
            catch (final Exception e)
            {
                e.printStackTrace();
            }
        }
    };
}
}

