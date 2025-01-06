/*
 * Copyright 2014-2025 Real Logic Limited.
 * Copyright 2015 Kaazing Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.samples;

import io.aeron.Aeron;
import io.aeron.Publication;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;

/**
 * A very simple Aeron publisher application which publishes a fixed size message on a fixed channel and stream.
 */
public class SimplePublisher
{
    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     * @throws InterruptedException if the thread sleep delay is interrupted.
     */
    public static void main(final String[] args) throws InterruptedException
    {
        // Allocate enough buffer size to hold maximum message length
        // The UnsafeBuffer class is part of the Agrona library and is used for efficient buffer management
        final UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(512, BitUtil.CACHE_LINE_LENGTH));

        // The channel (an endpoint identifier) to send the message to
        final String channel = "aeron:udp?endpoint=localhost:40123";

        // A unique identifier for a stream within a channel. Stream ID 0 is reserved
        // for internal use and should not be used by applications.
        final int streamId = 10;

        System.out.println("Publishing to " + channel + " on stream id " + streamId);

        // Create a context, needed for client connection to media driver
        // A separate media driver process needs to be running prior to starting this application
        final Aeron.Context ctx = new Aeron.Context();

        // Create an Aeron instance with client-provided context configuration and connect to the
        // media driver, and create a Publication. The Aeron and Publication classes implement
        // AutoCloseable, and will automatically clean up resources when this try block is finished.
        try (Aeron aeron = Aeron.connect(ctx);
            Publication publication = aeron.addPublication(channel, streamId))
        {
            final String message = "Hello World! ";
            final byte[] messageBytes = message.getBytes();
            buffer.putBytes(0, messageBytes);

            // Wait for 5 seconds to connect to a subscriber
            final long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!publication.isConnected())
            {
                if ((deadlineNs - System.nanoTime()) < 0)
                {
                    System.out.println("Failed to connect to subscriber");
                    return;
                }

                Thread.sleep(1);
            }

            // Try to publish the buffer. 'offer' is a non-blocking call.
            // If it returns less than 0, the message was not sent, and the offer should be retried.
            final long position = publication.offer(buffer, 0, messageBytes.length);

            if (position < 0L)
            {
                if (position == Publication.BACK_PRESSURED)
                {
                    System.out.println(" Offer failed due to back pressure");
                }
                else if (position == Publication.NOT_CONNECTED)
                {
                    System.out.println(" Offer failed because publisher is not connected to subscriber");
                }
                else if (position == Publication.ADMIN_ACTION)
                {
                    System.out.println("Offer failed because of an administration action in the system");
                }
                else if (position == Publication.CLOSED)
                {
                    System.out.println("Offer failed publication is closed");
                }
                else if (position == Publication.MAX_POSITION_EXCEEDED)
                {
                    System.out.println("Offer failed due to publication reaching max position");
                }
                else
                {
                    System.out.println(" Offer failed due to unknown reason");
                }
            }
            else
            {
                System.out.println(" yay !!");
            }

            System.out.println("Done sending.");
        }
    }
}
