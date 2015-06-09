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

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.SigInt;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A subscriber application with two subscriptions which can receive fragmented messages
 * Creates two subscriptions on a given channel subscribed to two different stream IDs.
 * The default STREAM_ID and CHANNEL are configured in {@link SampleConfiguration}. The default
 * channel and stream IDs can be changed by setting Java system properties at the command line, e.g.:
 * -Daeron.sample.channel=udp://localhost:5555 -Daeron.sample.streamId=20
 */
public class MultipleSubscribersWithFragmentAssembly
{
    private static final String CHANNEL = SampleConfiguration.CHANNEL;
    private static final int STREAM_ID_1 = SampleConfiguration.STREAM_ID;
    private static final int STREAM_ID_2 = SampleConfiguration.STREAM_ID + 1;

    // Maximum number of fragments to be read in a single poll operation
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

    public static void main(final String[] args) throws Exception
    {
        System.out.format("Subscribing to %s on stream ID %d and stream ID %d%n",
            CHANNEL, STREAM_ID_1, STREAM_ID_2);

        final Aeron.Context ctx = new Aeron.Context()
            .newConnectionHandler(MultipleSubscribersWithFragmentAssembly::eventNewConnection)
            .inactiveConnectionHandler(MultipleSubscribersWithFragmentAssembly::eventInactiveConnection);

        // dataHandler method is called for every new message received
        // When a message is completely reassembled, the delegate method 'reassembledStringMessage' is called
        final FragmentAssemblyAdapter dataHandler1 = new FragmentAssemblyAdapter(reassembledStringMessage1(STREAM_ID_1));

        // Another Data handler for the second stream
        final FragmentAssemblyAdapter dataHandler2 = new FragmentAssemblyAdapter(reassembledStringMessage2(STREAM_ID_2));

        final AtomicBoolean running = new AtomicBoolean(true);

        // Register a SIGINT handler for graceful shutdown
        SigInt.register(() -> running.set(false));

        // Create an Aeron instance using the default configuration set in the Context, and
        // add two subscriptions with two different stream IDs.
        // The Aeron and Subscription classes both implement "AutoCloseable" and will
        // automatically clean up resources when this try block is finished
        try (final Aeron aeron = Aeron.connect(ctx);
             final Subscription subscription1 = aeron.addSubscription(CHANNEL, STREAM_ID_1);
             final Subscription subscription2 = aeron.addSubscription(CHANNEL, STREAM_ID_2))
        {
            // Initialize a backoff strategy to avoid excessive spinning
            final IdleStrategy idleStrategy = new BackoffIdleStrategy(
                100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));

            // Try to read the data for both the subscribers
            while (running.get())
            {
                final int fragmentsRead1 = subscription1.poll(dataHandler1, FRAGMENT_COUNT_LIMIT);
                final int fragmentsRead2 = subscription2.poll(dataHandler2, FRAGMENT_COUNT_LIMIT);
                // Give the IdleStrategy a chance to spin/yield/sleep to reduce CPU
                // use if no messages were received.
                idleStrategy.idle(fragmentsRead1 + fragmentsRead2);
            }

            System.out.println("Shutting down...");
        }
    }

    /**
     * Print the information for a new connection to stdout.
     *
     * @param channel        for the connection
     * @param streamId       for the stream
     * @param sessionId      for the connection publication
     * @param position       in the stream
     * @param sourceIdentity that is transport specific
     */
    public static void eventNewConnection(
        final String channel, final int streamId, final int sessionId, final long position, final String sourceIdentity)
    {
        System.out.format(
            "new connection on %s streamId %x sessionId %x from %s%n",
            channel, streamId, sessionId, sourceIdentity);

    }

    /**
     * This handler is called when connection goes inactive
     *
     * @param channel   for the connection
     * @param streamId  for the stream
     * @param sessionId for the connection publication
     * @param position  within the stream
     */
    public static void eventInactiveConnection(
        final String channel, final int streamId, final int sessionId, final long position)
    {
        System.out.format(
            "inactive connection on %s streamId %d sessionId %x%n",
            channel, streamId, sessionId);
    }

    /**
     * Return a reusable, parameterized {@link FragmentHandler} that prints to stdout for the first stream(STREAM)
     *
     * @param streamId to show when printing
     * @return subscription data handler function that prints the message contents
     * @throws Exception
     */
    public static FragmentHandler reassembledStringMessage1(final int streamId) throws Exception
    {
        return
            (buffer, offset, length, header) ->
            {
                final byte[] data = new byte[length];
                buffer.getBytes(offset, data);

                System.out.format(
                    "message to stream %d from session %x term id %x term offset %d (%d@%d)%n",
                    streamId, header.sessionId(), header.termId(), header.termOffset(), length, offset);

                if (length != 10000)
                {
                    System.out.format(
                        "Received message was not assembled properly; received length was %d, but was expecting 10000%n",
                        length);
                }
            };
    }

    /**
     * Return a reusable, parameterized {@link FragmentHandler} that prints to stdout for the second stream (STREAM + 1)
     *
     * @param streamId to show when printing
     * @return subscription data handler function that prints the message contents
     */
    public static FragmentHandler reassembledStringMessage2(final int streamId) throws Exception
    {
        return
            (buffer, offset, length, header) ->
            {
                final byte[] data = new byte[length];
                buffer.getBytes(offset, data);

                System.out.format(
                    "message to stream %d from session %x term id %x term offset %d (%d@%d)%n",
                    streamId, header.sessionId(), header.termId(), header.termOffset(), length, offset);

                if (length != 9000)
                {
                    System.out.format(
                        "Received message was not assembled properly; received length was %d, but was expecting 9000%n",
                        length);
                }
            };
    }
}
