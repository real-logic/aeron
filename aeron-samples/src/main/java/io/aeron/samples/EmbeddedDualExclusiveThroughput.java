/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.samples;

import io.aeron.*;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.console.ContinueBarrier;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.samples.SamplesUtil.rateReporterHandler;
import static org.agrona.SystemUtil.loadPropertiesFiles;

/**
 * Throughput test with dual {@link ExclusivePublication}s using {@link ExclusivePublication#tryClaim(int, BufferClaim)}
 * for testing two sources into a single {@link Subscription} over UDP transport.
 */
public class EmbeddedDualExclusiveThroughput
{
    private static final long NUMBER_OF_MESSAGES = SampleConfiguration.NUMBER_OF_MESSAGES;
    private static final long LINGER_TIMEOUT_MS = SampleConfiguration.LINGER_TIMEOUT_MS;
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final String CHANNEL = SampleConfiguration.CHANNEL;

    private static final UnsafeBuffer OFFER_BUFFER = new UnsafeBuffer(
        BufferUtil.allocateDirectAligned(MESSAGE_LENGTH, BitUtil.CACHE_LINE_LENGTH));

    private static volatile boolean printingActive = true;

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     * @throws InterruptedException if the thread sleep delay is interrupted.
     */
    @SuppressWarnings("MethodLength")
    public static void main(final String[] args) throws InterruptedException
    {
        loadPropertiesFiles(args);

        final RateReporter reporter = new RateReporter(
            TimeUnit.SECONDS.toNanos(1), EmbeddedDualExclusiveThroughput::printRate);
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        final AtomicBoolean running = new AtomicBoolean(true);
        final AvailableImageHandler handler =
            (image) -> System.out.println("source connection=" + image.sourceIdentity());

        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media(CommonContext.UDP_MEDIA)
            .controlMode(CommonContext.MDC_CONTROL_MODE_MANUAL);

        final String sourceUriOne = builder.controlEndpoint("localhost:20550").tags("1").build();
        final String sourceUriTwo = builder.controlEndpoint("localhost:20551").tags("2").build();

        try (MediaDriver mediaDriver = MediaDriver.launch();
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
            Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID, handler, null);
            ExclusivePublication publicationOne = aeron.addExclusivePublication(sourceUriOne, STREAM_ID);
            ExclusivePublication publicationTwo = aeron.addExclusivePublication(sourceUriTwo, STREAM_ID))
        {
            publicationOne.addDestination(CHANNEL);
            publicationTwo.addDestination(CHANNEL);

            while (subscription.imageCount() < 2)
            {
                Thread.yield();
            }

            executor.execute(reporter);
            executor.execute(() -> SamplesUtil.subscriberLoop(
                rateReporterHandler(reporter), FRAGMENT_COUNT_LIMIT, running).accept(subscription));

            final ContinueBarrier barrier = new ContinueBarrier("Execute again?");
            final IdleStrategy idleStrategy = SampleConfiguration.newIdleStrategy();

            do
            {
                System.out.format(
                    "%nStreaming %,d messages of payload length %d bytes to %s on stream id %d%n",
                    NUMBER_OF_MESSAGES * 2, MESSAGE_LENGTH, CHANNEL, STREAM_ID);

                printingActive = true;

                long backPressureCountOne = 0;
                long backPressureCountTwo = 0;

                for (long a = 0, b = 0; a < NUMBER_OF_MESSAGES || b < NUMBER_OF_MESSAGES;)
                {
                    idleStrategy.reset();
                    boolean failedOne = false;
                    boolean failedTwo = false;

                    if (a < NUMBER_OF_MESSAGES)
                    {
                        if (publicationOne.offer(OFFER_BUFFER, 0, MESSAGE_LENGTH, null) > 0)
                        {
                            a++;
                        }
                        else
                        {
                            backPressureCountOne++;
                            failedOne = true;
                        }
                    }

                    if (b < NUMBER_OF_MESSAGES)
                    {
                        if (publicationTwo.offer(OFFER_BUFFER, 0, MESSAGE_LENGTH, null) > 0)
                        {
                            b++;
                        }
                        else
                        {
                            backPressureCountTwo++;
                            failedTwo = true;
                        }
                    }

                    if (failedOne || failedTwo)
                    {
                        idleStrategy.idle();
                    }
                }

                System.out.println("Done streaming." +
                    " backPressureRatioOne=" + ((double)backPressureCountOne / NUMBER_OF_MESSAGES) +
                    " backPressureRatioTwo=" + ((double)backPressureCountTwo / NUMBER_OF_MESSAGES));

                if (LINGER_TIMEOUT_MS > 0)
                {
                    System.out.println("Lingering for " + LINGER_TIMEOUT_MS + " milliseconds...");
                    Thread.sleep(LINGER_TIMEOUT_MS);
                }

                printingActive = false;
            }
            while (barrier.await());

            running.set(false);
            reporter.halt();
            executor.shutdown();
        }
    }

    private static void printRate(
        final double messagesPerSec, final double bytesPerSec, final long totalFragments, final long totalBytes)
    {
        if (printingActive)
        {
            System.out.format(
                "%.04g msgs/sec, %.04g bytes/sec, totals %d messages %d MB payloads%n",
                messagesPerSec, bytesPerSec, totalFragments, totalBytes / (1024 * 1024));
        }
    }
}
