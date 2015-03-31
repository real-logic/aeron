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
package uk.co.real_logic.aeron.samples;

//import java.io.PrintStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.SigInt;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.tools.MessageStream;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

//import static uk.co.real_logic.aeron.samples.SamplesUtil.printStringMessage;

/**
 * Multiple subscriber application which can receive fragmented messages
 * Create 2 subscribers on a given CHANNEL which can receive two independent stream of messages
 */
public class MultipleSubscriberWithFrag
{
    // A unique stream id within a given channel for subscriber one
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    // A unique stream id within a given channel for subscriber two
    private static final int STREAM_ID_2 = SampleConfiguration.STREAM_ID + 1;
    // Channel ID for the application
    private static final String CHANNEL = SampleConfiguration.CHANNEL;

    // Maximum Number of fragments to be read in a single poll operations
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

    //Create two message streams for two different subscribers
    private static MessageStream msgStream = new MessageStream();
    private static MessageStream msgStream2 = new MessageStream();

    public static void main(final String[] args) throws Exception
    {
        System.out.println("Subscribing to " + CHANNEL + " on stream Id " + STREAM_ID + " and stream Id " + STREAM_ID_2);

        // Create a context for a client and specify callback methods when
        // a new connection starts (eventNewConnection)
        // a connection goes inactive (eventInactiveConnection)
        final Aeron.Context ctx = new Aeron.Context() /* Callback at new producer starts */
            .newConnectionHandler(MultipleSubscriberWithFrag::eventNewConnection)
            .inactiveConnectionHandler(MultipleSubscriberWithFrag::eventInactiveConnection);

        // dataHandler method is called for every new datagram received
        // When a message is completely reassembled, the delegate method 'reassembledStringMessage' is called
        final FragmentAssemblyAdapter dataHandler = new FragmentAssemblyAdapter(reassembledStringMessage(STREAM_ID));

        // Another Data handler for a different stream
        final FragmentAssemblyAdapter dataHandler2 = new FragmentAssemblyAdapter(reassembledStringMessage2(STREAM_ID_2));

        final AtomicBoolean running = new AtomicBoolean(true);

        //Register a SIGINT handler
        SigInt.register(() -> running.set(false));

        // Create an Aeron instance with client provided context configuration and connect to media driver
        try (final Aeron aeron = Aeron.connect(ctx);
                //Add a subscription to Aeron for a given channel and steam. Also,
                // supply a dataHandler to be called when data arrives
                final Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID, dataHandler);
                final Subscription subscription2 = aeron.addSubscription(CHANNEL, STREAM_ID_2, dataHandler2))
                {
            // Initialize a backoff strategy to avoid excessive spinning
            final IdleStrategy idleStrategy = new BackoffIdleStrategy(
                    100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));

                try
                {
                    //Try to read the data for both the subscribers
                    while (running.get())
                    {
                        final int fragmentsRead = subscription.poll(FRAGMENT_COUNT_LIMIT);
                        idleStrategy.idle(fragmentsRead); // Call a backoff strategy

                        final int fragmentsRead2 = subscription2.poll(FRAGMENT_COUNT_LIMIT);
                        idleStrategy.idle(fragmentsRead2); // Call a backoff strategy
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
     * @param sourceInformation that is transport specific
     */
    public static void eventNewConnection(
        final String channel, final int streamId, final int sessionId, final String sourceInformation)
    {
        System.out.println(
            String.format(
                "new connection on %s streamId %x sessionId %x from %s",
                channel, streamId, sessionId, sourceInformation));
      //Reset the stream buffer because streams have restarted
        if (streamId == STREAM_ID)
        {
            msgStream.reset();
        }
        else if (streamId == STREAM_ID_2)
        {
            msgStream2.reset();
        }
        else
        {
            System.out.println("Invalid Stream ID : " + streamId);
        }

    }

    /**
     * This handler is called when connection goes inactive
     *
     * @param channel   for the connection
     * @param streamId  for the stream
     * @param sessionId for the connection publication
     */
    public static void eventInactiveConnection(final String channel, final int streamId, final int sessionId)
    {
        System.out.println(
            String.format(
                "inactive connection on %s streamId %d sessionId %x",
                channel, streamId, sessionId));
    }

    /**
     * Return a reusable, parameterized {@link DataHandler} that prints to stdout for the first stream(STREAM)
     *
     * @param streamId to show when printing
     * @return subscription data handler function that prints the message contents
     */
    public static DataHandler reassembledStringMessage(final int streamId)
    {
        return (buffer, offset, length, header) ->
        {
            final byte[] data = new byte[length];
            buffer.getBytes(offset, data);

            System.out.println(
                String.format(
                    "message to stream %d from session %x term id %x term offset %d (%d@%d)",
                    streamId, header.sessionId(), header.termId(), header.termOffset(), length, offset));
            try
            {
                if (streamId == STREAM_ID)
                {
                    msgStream.putNext(buffer, offset, length);
                }
                else
                {
                        System.out.println("Unknown Stream ID");
                }
            }
            catch (final Exception e)
            {
                e.printStackTrace();
            }
        };
    }



/**
 * Return a reusable, parameterized {@link DataHandler} that prints to stdout for the second stream (STREAM + 1)
 *
 * @param streamId to show when printing
 * @return subscription data handler function that prints the message contents
 */
public static DataHandler reassembledStringMessage2(final int streamId)
{
    return (buffer, offset, length, header) ->
    {
        final byte[] data = new byte[length];
        buffer.getBytes(offset, data);

        System.out.println(
            String.format(
                "message to stream %d from session %x term id %x term offset %d (%d@%d)",
                streamId, header.sessionId(), header.termId(), header.termOffset(), length, offset));
        try
        {

            if (streamId == STREAM_ID_2)
            {
                msgStream2.putNext(buffer, offset, length);
            }
            else
            {
                    System.out.println("Unknown Stream ID");
            }
        }
        catch (final Exception e)
        {
            e.printStackTrace();
        }
    };
}
}

