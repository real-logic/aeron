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

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.common.BusySpinIdleStrategy;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.aeron.common.IdleStrategy;
import uk.co.real_logic.aeron.common.RateReporter;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.aeron.common.concurrent.console.ContinueBarrier;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Publisher that sends as fast as possible a given number of messages at a given length.
 */
public class StreamingPublisher
{
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final String CHANNEL = SampleConfiguration.CHANNEL;
    private static final int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;
    private static final long NUMBER_OF_MESSAGES = SampleConfiguration.NUMBER_OF_MESSAGES;
    private static final long LINGER_TIMEOUT_MS = SampleConfiguration.LINGER_TIMEOUT_MS;
    private static final boolean EMBEDDED_MEDIA_DRIVER = SampleConfiguration.EMBEDDED_MEDIA_DRIVER;

    private static final UnsafeBuffer ATOMIC_BUFFER = new UnsafeBuffer(ByteBuffer.allocateDirect(MESSAGE_LENGTH));
    private static final IdleStrategy OFFER_IDLE_STRATEGY = new BusySpinIdleStrategy();

    private static volatile boolean printingActive = true;

    public static void main(final String[] args) throws Exception
    {
        if (MESSAGE_LENGTH < BitUtil.SIZE_OF_LONG)
        {
            throw new IllegalArgumentException(String.format("Message length must be at least %d bytes", MESSAGE_LENGTH));
        }

        SamplesUtil.useSharedMemoryOnLinux();

        final MediaDriver driver = EMBEDDED_MEDIA_DRIVER ? MediaDriver.launch() : null;

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        final Aeron.Context context = new Aeron.Context();

        try (final Aeron aeron = Aeron.connect(context, executor);
             final Publication publication = aeron.addPublication(CHANNEL, STREAM_ID))
        {
            final RateReporter reporter = new RateReporter(TimeUnit.SECONDS.toNanos(1), StreamingPublisher::printRate);
            executor.execute(reporter);

            final ContinueBarrier barrier = new ContinueBarrier("Execute again?");

            do
            {
                printingActive = true;

                System.out.format(
                    "\nStreaming %,d messages of size %d bytes to %s on stream Id %d\n",
                    NUMBER_OF_MESSAGES, MESSAGE_LENGTH, CHANNEL, STREAM_ID);

                for (long i = 0; i < NUMBER_OF_MESSAGES; i++)
                {
                    ATOMIC_BUFFER.putLong(0, i);

                    while (!publication.offer(ATOMIC_BUFFER, 0, ATOMIC_BUFFER.capacity()))
                    {
                        OFFER_IDLE_STRATEGY.idle(0);
                    }

                    reporter.onMessage(1, ATOMIC_BUFFER.capacity());
                }

                System.out.println("Done streaming.");

                if (0 < LINGER_TIMEOUT_MS)
                {
                    System.out.println("Lingering for " + LINGER_TIMEOUT_MS + " milliseconds...");
                    Thread.sleep(LINGER_TIMEOUT_MS);
                }

                printingActive = false;
            }
            while (barrier.await());

            reporter.halt();
        }

        executor.shutdown();
        CloseHelper.quietClose(driver);
    }

    public static void printRate(
        final double messagesPerSec, final double bytesPerSec, final long totalMessages, final long totalBytes)
    {
        if (printingActive)
        {
            System.out.format(
                "%.02g msgs/sec, %.02g bytes/sec, totals %d messages %d MB\n",
                messagesPerSec, bytesPerSec, totalMessages, totalBytes / (1024 * 1024));
        }
    }
}
