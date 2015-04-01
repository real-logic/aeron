/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
import java.util.concurrent.TimeUnit;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

/**
 * Basic Aeron publisher application
 */
public class BasicPublisher
{
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final String CHANNEL = SampleConfiguration.CHANNEL;
    private static final long NUMBER_OF_MESSAGES = SampleConfiguration.NUMBER_OF_MESSAGES;
    private static final long LINGER_TIMEOUT_MS = SampleConfiguration.LINGER_TIMEOUT_MS;

    private static final boolean EMBEDDED_MEDIA_DRIVER = SampleConfiguration.EMBEDDED_MEDIA_DRIVER;
    private static final UnsafeBuffer BUFFER = new UnsafeBuffer(ByteBuffer.allocateDirect(256));

    public static void main(final String[] args) throws Exception
    {
        System.out.println("Publishing to " + CHANNEL + " on stream Id " + STREAM_ID);

        final MediaDriver driver = EMBEDDED_MEDIA_DRIVER ? MediaDriver.launch() : null;

        // Create an Aeron context for client connection to media driver
        final Aeron.Context ctx = new Aeron.Context();

        // Connect to media driver and add a publisher to Aeron instance
        try (final Aeron aeron = Aeron.connect(ctx);
             final Publication publication = aeron.addPublication(CHANNEL, STREAM_ID))
        {
            // Try to send messages
            for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
            {
                //Prepare a buffer to be sent
                final String message = "Hello World! " + i;
                BUFFER.putBytes(0, message.getBytes());

                System.out.print("offering " + i + "/" + NUMBER_OF_MESSAGES);
                // Try to send the message on configured CHANNEL and STREAM
                final boolean result = publication.offer(BUFFER, 0, message.getBytes().length);

                if (!result)
                {
                    // Message offer did not succeed
                    System.out.println(" ah?!");
                }
                else
                {
                    // Successful message send
                    System.out.println(" yay!");
                }
                //Sleep for a second
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
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
