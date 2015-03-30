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

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.tools.MessageStream;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

/**
 * A  Aeron publisher application with fragmented data
 */
public class SimplePublisherWithFrag
{

    public static void main(final String[] args) throws Exception
    {
        //Allocate enough buffer size to hold maximum stream buffer
        // 'UnsafeBuffer' class is part of agrona data structure, used for very efficient buffer management
        final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(10000));

        String channel = new String("udp://localhost:40123"); // An End-point identifier to receive message from
        final int streamId = 10; //A unique identifier for a Stream within a channel. A value of 0 is reserved

        System.out.println("Publishing to " + channel + " on stream Id " + streamId);

        // Create a context, needed for client connection to media driver
        // A separate media driver process need to run prior to running this application
        final Aeron.Context ctx = new Aeron.Context();

        //Create an Aeron instance with client provided context configuration and connect to media driver
        try (final Aeron aeron = Aeron.connect(ctx);
             final Publication publication = aeron.addPublication(channel, streamId))
        {
         // Allocate a session buffer bigger than default Aeron MTU size (4096)
            MessageStream msgStream = new MessageStream(8192);
            int len = msgStream.getNext(buffer); // Size of 'BUFFER' must be big enough to hold (8192 + 12(header))
            // Try to send 5 messages from publisher. All five messages are considered as a part of a stream
            for (int i = 1; i < 6; i++)
            {
                    // Try to publish buffer from first publisher. Call to 'offer' is a non-blocking call.
                    // In case the call fails, it must be retried
                    System.out.print("offering " + i + "/" + 5);
                    // Try to publish buffer from first publisher
                    final boolean result = publication.offer(buffer, 0, len);
                    if (!result)
                    {
                        System.out.println(" ah? "); // Failed to send the message, Need to retry the send
                    }
                    else
                    {
                        // Get the next buffer in the stream
                        len = msgStream.getNext(buffer); // Get the next buffer to be sent in the stream
                        System.out.println(" yay for stream " + streamId + " and data length " + len + "!!");
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
