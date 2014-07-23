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
package uk.co.real_logic.aeron.examples;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.nio.ByteBuffer;
import java.util.concurrent.*;

/**
 * Example Aeron publisher application
 */
public class ExamplePublisher
{
    public static final int CHANNEL_ID = ExampleConfiguration.CHANNEL_ID;
    public static final String DESTINATION = ExampleConfiguration.DESTINATION;
    public static final long NUMBER_OF_MESSAGES = ExampleConfiguration.NUMBER_OF_MESSAGES;

    private static final AtomicBuffer BUFFER = new AtomicBuffer(ByteBuffer.allocateDirect(256));

    public static void main(final String[] args)
    {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final Aeron.ClientContext context = new Aeron.ClientContext();

        try (final MediaDriver driver = ExampleUtil.createEmbeddedMediaDriver();
             final Aeron aeron = ExampleUtil.createAeron(context, executor))
        {
            System.out.println("Publishing to " + DESTINATION + " on channel Id " + CHANNEL_ID);

            final Publication publication = aeron.addPublication(DESTINATION, CHANNEL_ID, 0);

            for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
            {
                final String message = "Hello World! " + i;
                BUFFER.putBytes(0, message.getBytes());

                System.out.print("offering " + i + "/" + NUMBER_OF_MESSAGES);
                final boolean result = publication.offer(BUFFER, 0, message.getBytes().length);

                if (!result)
                {
                    System.out.println(" ah?!");
                }
                else
                {
                    System.out.println(" yay!");
                }

                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            }

            aeron.shutdown();
            driver.shutdown();
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }

        executor.shutdown();
    }
}
