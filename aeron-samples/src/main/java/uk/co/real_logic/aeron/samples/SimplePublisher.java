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

import java.nio.ByteBuffer;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

/**
 * A  Aeron publisher application
 */
public class SimplePublisher
{
    public static void main(final String[] args) throws Exception
    {
        //Allocate enough buffer size to hold maximum stream buffer
        // 'UnsafeBuffer' class is part of agrona data structure, used for very efficient buffer management
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(512));

        final String channel = new String("udp://localhost:40123"); // An End-point identifier to receive message from
        final int streamId = 10; //A unique identifier for a Stream within a channel. A value of 0 is reserved

        System.out.println("Publishing to " + channel + " on stream Id " + streamId);

        // Create a context, needed for client connection to media driver
        // A separate media driver process need to run prior to running this application
        final Aeron.Context ctx = new Aeron.Context();

        // Create an Aeron instance with client provided context configuration and connect to media driver
        try (final Aeron aeron = Aeron.connect(ctx);
             final Publication publication = aeron.addPublication(channel, streamId))
        {
            //Prepare a buffer to be sent
            String message = "Hello World! " + 1;
            buffer.putBytes(0, message.getBytes());

            // Try to send 5 messages from publisher
            for (int i = 1; i < 6; i++)
            {
                    // Try to publish buffer from first publisher. Call to 'offer' is a non-blocking call.
                    // In case the call fails, it must be retried
                    System.out.print("offering " + i + "/" + 5);
                    final long result = publication.offer(buffer, 0, message.getBytes().length);

                    if (result < 0L)
                    {
                        System.out.println(" ah? "); // Failed to send the message
                    }
                    else
                    {
                        // Successfully sent the message
                        System.out.println(" yay !!");
                        message = "Hello World! " + (i + 1);
                        buffer.putBytes(0, message.getBytes());
                    }
                }
                Thread.sleep(1000);
            }
            System.out.println("Done sending.");
            // Linger for 5 seconds before exit so that all the data is drained out
            System.out.println("Lingering for " + 5000 + " milliseconds...");
                Thread.sleep(5000);
    }
}
