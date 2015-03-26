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
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
//import java.util.concurrent.TimeUnit;

/**
 * Basic Aeron publisher application
 */
public class SimplePublisher
{
    private static final int STREAM_ID = 10;

    private static final String CHANNEL = System.getProperty("aeron.sample.channel", "udp://localhost:40123");

    //Allocate enough buffer size to hold maximum stream buffer
    private static final UnsafeBuffer BUFFER = new UnsafeBuffer(ByteBuffer.allocateDirect(512));

    public static void main(final String[] args) throws Exception
    {
        System.out.println("Publishing to " + CHANNEL + " on stream Id " + STREAM_ID);

        SamplesUtil.useSharedMemoryOnLinux();

        final MediaDriver driver = MediaDriver.launch();
        // Create a context for client connection
        final Aeron.Context ctx = new Aeron.Context();

        // Create an Aeron instance using connection parameter specified by context
        // and add 2 publisher with two different session Ids
        try (final Aeron aeron = Aeron.connect(ctx);
             final Publication publication = aeron.addPublication(CHANNEL, STREAM_ID))
        {
            //Prepare a buffer to be sent
            String message = "Hello World! " + 1;
            BUFFER.putBytes(0, message.getBytes());

            // Try to send 5 messages from publisher
            for (int i = 1; i < 6; i++)
            {
                    // Try to publish buffer from first publisher
                    System.out.print("offering " + i + "/" + 5);
                    boolean result = publication.offer(BUFFER, 0, message.getBytes().length);

                    if (!result)
                    {
                        System.out.println(" ah? ");
                    }
                    else
                    {
                        System.out.println(" yay !!");
                        message = "Hello World! " + (i + 1);
                        BUFFER.putBytes(0, message.getBytes());
                    }
                }

                Thread.sleep(1000);
            }

            System.out.println("Done sending.");
            System.out.println("Lingering for " + 5000 + " milliseconds...");
                Thread.sleep(5000);

        CloseHelper.quietClose(driver);
    }
}
