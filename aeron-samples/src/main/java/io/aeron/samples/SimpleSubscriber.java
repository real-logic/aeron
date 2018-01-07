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
package io.aeron.samples;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SigInt;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A very simple Aeron subscriber application which can receive small non-fragmented messages
 * on a fixed channel and stream ID. The DataHandler method 'printStringMessage' is called when data
 * is received. This application doesn't handle large fragmented messages. For an example of
 * fragmented message reception, see {@link MultipleSubscribersWithFragmentAssembly}.
 */
public class SimpleSubscriber
{
    public static void main(final String[] args)
    {
        // Maximum number of message fragments to receive during a single 'poll' operation
        final int fragmentLimitCount = 10;

        // The channel (an endpoint identifier) to receive messages from
        final String channel = "aeron:udp?endpoint=localhost:40123";

        // A unique identifier for a stream within a channel. Stream ID 0 is reserved
        // for internal use and should not be used by applications.
        final int streamId = 10;

        System.out.println("Subscribing to " + channel + " on stream Id " + streamId);

        final AtomicBoolean running = new AtomicBoolean(true);
        // Register a SIGINT handler for graceful shutdown.
        SigInt.register(() -> running.set(false));

        // dataHandler method is called for every new datagram received
        final FragmentHandler fragmentHandler =
            (buffer, offset, length, header) ->
            {
                final byte[] data = new byte[length];
                buffer.getBytes(offset, data);

                System.out.println(String.format(
                    "Received message (%s) to stream %d from session %x term id %x term offset %d (%d@%d)",
                    new String(data), streamId, header.sessionId(),
                    header.termId(), header.termOffset(), length, offset));

                // Received the intended message, time to exit the program
                running.set(false);
            };

        // Create a context, needed for client connection to media driver
        // A separate media driver process need to run prior to running this application
        final Aeron.Context ctx = new Aeron.Context();

        // Create an Aeron instance with client-provided context configuration, connect to the
        // media driver, and add a subscription for the given channel and stream using the supplied
        // dataHandler method, which will be called with new messages as they are received.
        // The Aeron and Subscription classes implement AutoCloseable, and will automatically
        // clean up resources when this try block is finished.
        try (Aeron aeron = Aeron.connect(ctx);
            Subscription subscription = aeron.addSubscription(channel, streamId))
        {
            final IdleStrategy idleStrategy = new BackoffIdleStrategy(
                100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));

            // Try to read the data from subscriber
            while (running.get())
            {
                // poll delivers messages to the dataHandler as they arrive
                // and returns number of fragments read, or 0
                // if no data is available.
                final int fragmentsRead = subscription.poll(fragmentHandler, fragmentLimitCount);
                // Give the IdleStrategy a chance to spin/yield/sleep to reduce CPU
                // use if no messages were received.
                idleStrategy.idle(fragmentsRead);
            }

            System.out.println("Shutting down...");
        }
    }
}
