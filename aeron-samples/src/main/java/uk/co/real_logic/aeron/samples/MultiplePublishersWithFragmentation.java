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

import java.nio.ByteBuffer;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

/**
 * A publisher application with multiple publications which send
 * fragmented data to a channel and two different streams. The default STREAM_ID and CHANNEL is
 * configured at {@link SampleConfiguration}. The default
 * channel and stream can be changed by setting java system properties at the command line.
 * i.e. (-Daeron.sample.channel=udp://localhost:5555 -Daeron.sample.streamId=20)
 */
public class MultiplePublishersWithFragmentation
{
    private static final int STREAM_ID_1 = SampleConfiguration.STREAM_ID;
    private static final int STREAM_ID_2 = SampleConfiguration.STREAM_ID + 1;
    private static final String CHANNEL = SampleConfiguration.CHANNEL;

    // Allocate enough buffer for message to get fragmented
    private static final UnsafeBuffer BUFFER_1 = new UnsafeBuffer(ByteBuffer.allocateDirect(10000));
    private static final UnsafeBuffer BUFFER_2 = new UnsafeBuffer(ByteBuffer.allocateDirect(9000));

    public static void main(final String[] args) throws Exception
    {
        System.out.println("Publishing to " + CHANNEL + " on stream Id " + STREAM_ID_1 + " and stream Id " + STREAM_ID_2);
        // Create a context for client connection
        final Aeron.Context ctx = new Aeron.Context();

        // Create an Aeron instance using connection parameter specified by context
        // and add 2 publisher with two different session Ids
        // Aeron is "AutoClosable" and will automatically clean up resources when this try block is finished
        try (final Aeron aeron = Aeron.connect(ctx);
             final Publication publication1 = aeron.addPublication(CHANNEL, STREAM_ID_1);
             final Publication publication2 = aeron.addPublication(CHANNEL, STREAM_ID_2))
        {
            // Prepare a buffer to be sent
            int j = 1;
            int k = 1;
            final String message1 = "Hello World! " + j;
            BUFFER_1.putBytes(0, message1.getBytes());
            // Prepare a buffer to be sent
            final String message2 = "Hello World! " + k;
            BUFFER_2.putBytes(0, message2.getBytes());
            // Try to send 5000 messages from each publisher
            while (j <= 5000 || k <= 5000)
            {
                boolean offerStatus1 = false;
                boolean offerStatus2 = false;
                long result1 = 0;
                long result2 = 0;
                while (!(offerStatus1 || offerStatus2))
                {
                    // Try to publish buffer from first publisher
                    if (j <= 5000)
                    {
                        result1 = publication1.offer(BUFFER_1, 0, BUFFER_1.capacity());
                        if (result1 < 0L)
                        {
                            // Message offer failed
                            if (result1 == Publication.BACK_PRESSURE)
                            {
                                System.out.println(" Offer failed due to back pressure for stream Id " + STREAM_ID_1);
                            }
                            else if (result1 == Publication.NOT_CONNECTED)
                            {
                                System.out.println(" Offer failed because publisher is not yet " +
                                    "connected to subscriber for stream Id "  + STREAM_ID_1);
                            }
                            else
                            {
                                System.out.println(" Offer failed due to unknown reason");
                            }
                            offerStatus1 = false;
                        }
                        else
                        {
                            // Successfully send the data, get the next buffer in the stream
                            j++;
                            offerStatus1 = true;
                            System.out.println("Successfully sent data on stream " +
                                STREAM_ID_1 + " and data length " + BUFFER_1.capacity() + " at offset " + result1);
                        }
                    }
                    // Try to publish buffer from second publisher
                    if (k <= 5000)
                    {
                        result2 = publication2.offer(BUFFER_2, 0, BUFFER_2.capacity());
                        if (result2 < 0L)
                        {
                            // Message offer failed
                            if (result2 == Publication.BACK_PRESSURE)
                            {
                                System.out.println(" Offer failed because publisher is not yet " +
                                        "connected to subscriber for stream Id "  + STREAM_ID_2);
                            }
                            else if (result2 == Publication.NOT_CONNECTED)
                            {
                                System.out.println("Offer failed - publisher is not yet connected to subscriber" + STREAM_ID_2);
                            }
                            else
                            {
                                System.out.println("Offer failed due to unknown reason");
                            }
                            offerStatus2 = false;
                        }
                        else
                        {
                            // Successfully send the data, get the next buffer in the stream
                            k++;
                            offerStatus2 = true;
                            System.out.println("Successfully sent data on stream " + STREAM_ID_2 +
                                " and data length " + BUFFER_2.capacity() + " at offset " + result2);
                        }
                    }
                }
            }
            System.out.println("Done sending total messages for stream Id " +
                    STREAM_ID_1 + " = " + (j - 1) + " and stream Id " + STREAM_ID_2 + " = " + (k - 1));
        }
    }
}
