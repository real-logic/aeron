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
import uk.co.real_logic.aeron.mediadriver.MediaDriver;
import uk.co.real_logic.aeron.util.RateReporter;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Publisher that sends as fast as possible a given number of messages at a given size
 */
public class StreamingPublisher
{
    public static final int CHANNEL_ID = 10;
    public static final String DESTINATION = "udp://localhost:40123";
    public static final int MESSAGE_LENGTH = 256;
    public static final long NUMBER_OF_MESSAGES = 100;
    public static final long LINGER_TIMEOUT = TimeUnit.SECONDS.toMillis(5);

    private static final AtomicBuffer buffer = new AtomicBuffer(ByteBuffer.allocateDirect(MESSAGE_LENGTH));

    public static void main(final String[] args)
    {
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        final Aeron.ClientContext context = new Aeron.ClientContext();

        try (final MediaDriver driver = ExampleUtil.createEmbeddedMediaDriver();
             final Aeron aeron = ExampleUtil.createAeron(context, executor))
        {
            System.out.println("Streaming " + NUMBER_OF_MESSAGES + " messages of size " + MESSAGE_LENGTH +
                    " bytes to " + DESTINATION + " on channel Id " + CHANNEL_ID);

            final Publication publication = aeron.addPublication(DESTINATION, CHANNEL_ID, 0);
            final RateReporter reporter = new RateReporter(TimeUnit.SECONDS.toNanos(1), ExampleUtil::printRate);

            // report the rate we are sending
            executor.execute(reporter);

            for (long i = 0; i < NUMBER_OF_MESSAGES; i++)
            {
                buffer.putLong(0, i);

                while (!publication.offer(buffer, 0, buffer.capacity()))
                {
                    Thread.yield();
                }

                reporter.onMessage(1, buffer.capacity());
            }

            System.out.println("Done streaming.");

            if (0 < LINGER_TIMEOUT)
            {
                System.out.println("Lingering for " + LINGER_TIMEOUT + " milliseconds...");
                Thread.sleep(LINGER_TIMEOUT);
            }

            reporter.done();
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
