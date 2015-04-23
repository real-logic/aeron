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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.SigInt;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

/**
 * A very simple Aeron subscriber application which can receive small non-fragmented messages
 * on a fixed channel and stream.The datahandler method 'printStringMessage' is called when data
 * is received. This application doesn't handle large fragmented messages. For fragmented message reception,
 * look at the application at {@link MultipleSubscriberWithFragmentation}
 *
 */
public class SimpleSubscriber
{
    public static void main(final String[] args) throws Exception
    {
        // Number of message fragments to limit for a single 'poll' operation
        final int fragmentLimitCount = 10;

        // An end-point identifier to receive message from
        final String channel = new String("udp://localhost:40123");

        // A unique identifier for a Stream within a channel. A value of 0 is reserved
        final int streamId = 10;

        System.out.println("Subscribing to " + channel + " on stream Id " + streamId);

        // dataHandler method is called for every new datagram received
        //final DataHandler dataHandler = printStringMessage(streamId);
        // Variable 'running' is set to 'true'
        final AtomicBoolean running = new AtomicBoolean(true);

        final DataHandler dataHandler = new DataHandler()
        {
            @Override
            public void onData(DirectBuffer buffer, int offset, int length, Header header)
            {
                final byte[] data = new byte[length];
                buffer.getBytes(offset, data);

                System.out.println(
                    String.format(
                        "Received message (%s) to stream %d from session %x term id %x term offset %d (%d@%d)",
                        new String(data), streamId, header.sessionId(), header.termId(), header.termOffset(), length, offset));
                // Received the intended message, time to exit the program
                running.set(false);
             }
        };

        // Register a SIGINT handler. On receipt of SIGINT, set variable 'running' to 'false'
        SigInt.register(() -> running.set(false));

        // Create a context, needed for client connection to media driver
        // A separate media driver process need to run prior to running this application
        final Aeron.Context ctx = new Aeron.Context();

        // Create an Aeron instance with client provided context configuration and connect to media driver
        Subscription subscription = null;
        Aeron aeron = null;
        try
        {
            aeron = Aeron.connect(ctx);

            // Add a subscription to Aeron for a given channel and steam. Also,
            // supply a dataHandler to be called when data arrives
            // This works only if published data is not fragmented by Aeron
            subscription = aeron.addSubscription(channel, streamId, dataHandler);

                final IdleStrategy idleStrategy = new BackoffIdleStrategy(
                    100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));
                try
                {
                        // Try to read the data from subscriber
                        while (running.get())
                        {
                            // poll delivers messages to the datahandler as they arrive
                            // and returns number of fragments read, otherwise returns 0
                            // if no data is available.
                            final int fragmentsRead = subscription.poll(fragmentLimitCount);
                            // Call a backoff strategy
                            idleStrategy.idle(fragmentsRead);
                        }
                }
                catch (final Exception ex)
                {
                    ex.printStackTrace();
                }
                System.out.println("Shutting down...");
        }
        finally
        {
            subscription.close();
            aeron.close();
        }
    }
}

