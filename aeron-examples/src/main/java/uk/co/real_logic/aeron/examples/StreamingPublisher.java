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
import uk.co.real_logic.aeron.common.CloseHelper;
import uk.co.real_logic.aeron.common.RateReporter;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Publisher that sends as fast as possible a given number of messages at a given size
 */
public class StreamingPublisher
{
    private static final int STREAM_ID = ExampleConfiguration.STREAM_ID;
    private static final String CHANNEL = ExampleConfiguration.CHANNEL;
    private static final int MESSAGE_LENGTH = ExampleConfiguration.MESSAGE_LENGTH;
    private static final long NUMBER_OF_MESSAGES = ExampleConfiguration.NUMBER_OF_MESSAGES;
    private static final long LINGER_TIMEOUT_MS = ExampleConfiguration.LINGER_TIMEOUT_MS;
    private static final boolean EMBEDDED_MEDIA_DRIVER = ExampleConfiguration.EMBEDDED_MEDIA_DRIVER;

    private static final AtomicBuffer ATOMIC_BUFFER = new AtomicBuffer(ByteBuffer.allocateDirect(MESSAGE_LENGTH));

    public static void main(final String[] args) throws Exception
    {
        System.out.println("Streaming " + NUMBER_OF_MESSAGES + " messages of size " + MESSAGE_LENGTH +
                           " bytes to " + CHANNEL + " on stream Id " + STREAM_ID);

        final MediaDriver driver = EMBEDDED_MEDIA_DRIVER ? MediaDriver.launch() : null;

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        final Aeron.Context context = new Aeron.Context();

        try (final Aeron aeron = Aeron.connect(context, executor);
             final Publication publication = aeron.addPublication(CHANNEL, STREAM_ID, 0))
        {
            final RateReporter reporter = new RateReporter(TimeUnit.SECONDS.toNanos(1), ExampleUtil::printRate);

            // report the rate we are sending
            executor.execute(reporter);

            for (long i = 0; i < NUMBER_OF_MESSAGES; i++)
            {
                ATOMIC_BUFFER.putLong(0, i);

                while (!publication.offer(ATOMIC_BUFFER, 0, ATOMIC_BUFFER.capacity()))
                {
                    Thread.yield();
                }

                reporter.onMessage(1, ATOMIC_BUFFER.capacity());
            }

            System.out.println("Done streaming.");

            if (0 < LINGER_TIMEOUT_MS)
            {
                System.out.println("Lingering for " + LINGER_TIMEOUT_MS + " milliseconds...");
                Thread.sleep(LINGER_TIMEOUT_MS);
            }

            reporter.halt();
        }

        executor.shutdown();
        CloseHelper.quietClose(driver);
    }
}
