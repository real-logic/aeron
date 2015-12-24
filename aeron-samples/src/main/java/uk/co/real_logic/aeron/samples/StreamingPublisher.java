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

import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_LONG;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.driver.RateReporter;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.concurrent.BusySpinIdleStrategy;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.console.ContinueBarrier;

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
    private static final boolean RANDOM_MESSAGE_LENGTH = SampleConfiguration.RANDOM_MESSAGE_LENGTH;
    private static final UnsafeBuffer ATOMIC_BUFFER = new UnsafeBuffer(ByteBuffer.allocateDirect(MESSAGE_LENGTH));
    private static final BusySpinIdleStrategy OFFER_IDLE_STRATEGY = new BusySpinIdleStrategy();
    private static final IntSupplier LENGTH_GENERATOR = composeLengthGenerator(RANDOM_MESSAGE_LENGTH, MESSAGE_LENGTH);

    private static volatile boolean printingActive = true;

    public static void main(final String[] args) throws Exception
    {
        if (MESSAGE_LENGTH < SIZE_OF_LONG)
        {
            throw new IllegalArgumentException(String.format("Message length must be at least %d bytes", SIZE_OF_LONG));
        }

        final MediaDriver driver = EMBEDDED_MEDIA_DRIVER ? MediaDriver.launchEmbedded() : null;
        final Aeron.Context context = new Aeron.Context();

        if (EMBEDDED_MEDIA_DRIVER)
        {
            context.aeronDirectoryName(driver.aeronDirectoryName());
        }

        final RateReporter reporter = new RateReporter(TimeUnit.SECONDS.toNanos(1), StreamingPublisher::printRate);
        final ExecutorService executor = Executors.newFixedThreadPool(1);

        executor.execute(reporter);

        // Connect to media driver and add publication to send messages on the configured channel and stream ID.
        // The Aeron and Publication classes implement AutoCloseable, and will automatically
        // clean up resources when this try block is finished.
        try (final Aeron aeron = Aeron.connect(context);
             final Publication publication = aeron.addPublication(CHANNEL, STREAM_ID))
        {
            final ContinueBarrier barrier = new ContinueBarrier("Execute again?");

            do
            {
                printingActive = true;

                System.out.format(
                    "\nStreaming %,d messages of%s size %d bytes to %s on stream Id %d\n",
                    NUMBER_OF_MESSAGES,
                    (RANDOM_MESSAGE_LENGTH) ? " random" : "",
                    MESSAGE_LENGTH,
                    CHANNEL,
                    STREAM_ID);

                long backPressureCount = 0;

                for (long i = 0; i < NUMBER_OF_MESSAGES; i++)
                {
                    final int length = LENGTH_GENERATOR.getAsInt();

                    ATOMIC_BUFFER.putLong(0, i);
                    OFFER_IDLE_STRATEGY.reset();
                    while (publication.offer(ATOMIC_BUFFER, 0, length) < 0L)
                    {
                        // The offer failed, which is usually due to the publication
                        // being temporarily blocked.  Retry the offer after a short
                        // spin/yield/sleep, depending on the chosen IdleStrategy.
                        backPressureCount++;
                        OFFER_IDLE_STRATEGY.idle();
                    }

                    reporter.onMessage(1, length);
                }

                System.out.println("Done streaming. Back pressure ratio " + ((double)backPressureCount / NUMBER_OF_MESSAGES));

                if (0 < LINGER_TIMEOUT_MS)
                {
                    System.out.println("Lingering for " + LINGER_TIMEOUT_MS + " milliseconds...");
                    Thread.sleep(LINGER_TIMEOUT_MS);
                }

                printingActive = false;
            }
            while (barrier.await());
        }

        reporter.halt();
        executor.shutdown();
        CloseHelper.quietClose(driver);
    }

    public static void printRate(
        final double messagesPerSec, final double bytesPerSec, final long totalFragments, final long totalBytes)
    {
        if (printingActive)
        {
            System.out.format(
                "%.02g msgs/sec, %.02g bytes/sec, totals %d messages %d MB\n",
                messagesPerSec, bytesPerSec, totalFragments, totalBytes / (1024 * 1024));
        }
    }

    private static IntSupplier composeLengthGenerator(final boolean random, final int max)
    {
        if (random)
        {
            return () -> ThreadLocalRandom.current().nextInt(SIZE_OF_LONG, max);
        }
        else
        {
            return () -> max;
        }
    }
}
