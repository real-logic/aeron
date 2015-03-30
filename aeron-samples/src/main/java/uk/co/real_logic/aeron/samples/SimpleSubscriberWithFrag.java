/*
 * Copyright 2015 Real Logic Ltd.
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

/**
 * Basic Aeron subscriber application which can receive fragmented messages
 */
public class SimpleSubscriberWithFrag
{

    private static MessageStream msgStream = new MessageStream();
    public static void main(final String[] args) throws Exception
    {
        final int fragmentCountLimit = 10; // Number of message fragments to limit for a single 'poll' operation
        String channel = new String("udp://localhost:40123"); // An End-point identifier to receive message from
        final int streamId = 10; //A unique identifier for a Stream within a channel. A value of 0 is reserved

        System.out.println("Subscribing to " + channel + " on stream Id " + streamId);

        // dataHandler method is called for every new datagram received
        // When a message is completely reassembled, the delegate method 'printStringMessage' is called
        final FragmentAssemblyAdapter dataHandler = new FragmentAssemblyAdapter(printStringMessage(streamId));

        // Variable 'running' is set to 'true'
        final AtomicBoolean running = new AtomicBoolean(true);

        //Register a SIGINT handler. On receipt of SIGINT, set variable 'running' to 'false'
        SigInt.register(() -> running.set(false));

        // Create a context, needed for client connection to media driver
        // A separate media driver process need to run prior to running this application
        final Aeron.Context ctx = new Aeron.Context();

        // Create an Aeron instance with client provided context configuration and connect to media driver
        try (final Aeron aeron = Aeron.connect(ctx);

                //Add a subscription to Aeron for a given channel and steam. Also,
                // supply a dataHandler to be called when data arrives
                final Subscription subscription = aeron.addSubscription(channel, streamId, dataHandler))
                {
                    // Initialize an 'Idlestrategy' class for a back off strategy
                    final IdleStrategy idleStrategy = new BackoffIdleStrategy(
                            100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));

                    try
                    {
                        //Try to read the data from subscriber
                        while (running.get())
                        {
                            // poll returns number of fragments read
                            final int dataRead = subscription.poll(fragmentCountLimit);
                            idleStrategy.idle(dataRead); // Idle strategy to avoid excessive spinning in case of slow publisher
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
     * Return a reusable, parameterized {@link DataHandler} that prints to stdout
     *
     * @param streamId to show when printing
     * @return subscription data handler function that prints the message contents
     */
    public static DataHandler printStringMessage(final int streamId)
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
                    msgStream.putNext(buffer, offset, length);
            }
            catch (final Exception e)
            {
                e.printStackTrace();
            }

        };
    }

}

