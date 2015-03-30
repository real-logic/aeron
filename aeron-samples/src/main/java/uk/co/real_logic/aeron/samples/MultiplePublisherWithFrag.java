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

import java.nio.ByteBuffer;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.tools.MessageStream;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
//import java.util.concurrent.TimeUnit;

/**
 * Basic Aeron publisher application
 */
public class MultiplePublisherWithFrag
{
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final int STREAM_ID_2 = SampleConfiguration.STREAM_ID + 1;
    private static final String CHANNEL = SampleConfiguration.CHANNEL;
    private static final long LINGER_TIMEOUT_MS = SampleConfiguration.LINGER_TIMEOUT_MS;

    private static final boolean EMBEDDED_MEDIA_DRIVER = SampleConfiguration.EMBEDDED_MEDIA_DRIVER;
    //Allocate enough buffer size to hold maximum stream buffer
    private static final UnsafeBuffer BUFFER = new UnsafeBuffer(ByteBuffer.allocateDirect(10000));
    private static final UnsafeBuffer BUFFER_2 = new UnsafeBuffer(ByteBuffer.allocateDirect(10000));

    public static void main(final String[] args) throws Exception
    {
        System.out.println("Publishing to " + CHANNEL + " on stream Id " + STREAM_ID);

        SamplesUtil.useSharedMemoryOnLinux();

        final MediaDriver driver = EMBEDDED_MEDIA_DRIVER ? MediaDriver.launch() : null;
        // Create a context for client connection
        final Aeron.Context ctx = new Aeron.Context();

        // Create an Aeron instance using connection parameter specified by context
        // and add 2 publisher with two different session Ids
        try (final Aeron aeron = Aeron.connect(ctx);
             final Publication publication = aeron.addPublication(CHANNEL, STREAM_ID);
             final Publication publication2 = aeron.addPublication(CHANNEL, STREAM_ID_2))
        {
            // Allocate 2 different session buffer
            MessageStream msgStream = new MessageStream(8192);
            MessageStream msgStream2 = new MessageStream(8192);
            // Try to send 5000 messages from each publishers
            int len = msgStream.getNext(BUFFER);
            int len2 = msgStream2.getNext(BUFFER_2);
            for (int i = 0; i < 5000; i++)
            {

                boolean offerStatus = false;
                while (!offerStatus)
                {
                    // Try to publish buffer from first publisher
                    final boolean result = publication.offer(BUFFER, 0, len);

                    if (!result)
                    {
                        System.out.println(" ah? from first publisher with stream ID " + STREAM_ID + "!!");
                        offerStatus = false;
                    }
                    else
                    {
                        len = msgStream.getNext(BUFFER);
                        offerStatus = true;
                        System.out.println(" yay for stream " + STREAM_ID + " and data length " + len + "!!");
                    }

                    // Try to publish buffer from second publisher
                    final boolean result2 = publication2.offer(BUFFER_2, 0, len2);

                    if (!result2)
                    {
                        System.out.println(" ah? from second publisher with stream ID " +
                                STREAM_ID_2 + " and data length " + len2 +  "!!");
                        offerStatus = false;
                    }
                    else
                    {
                        len2 = msgStream2.getNext(BUFFER_2);
                        offerStatus = true;
                        System.out.println(" yay for stream " + STREAM_ID + " and data length " + len + STREAM_ID_2 + " !! ");
                    }
                }
            }

            System.out.println("Done sending.");

            if (0 < LINGER_TIMEOUT_MS)
            {
                System.out.println("Lingering for " + LINGER_TIMEOUT_MS + " milliseconds...");
                Thread.sleep(LINGER_TIMEOUT_MS);
            }
        }

        CloseHelper.quietClose(driver);
    }
}
