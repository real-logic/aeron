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

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
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
 * Throughput test using {@link ExclusivePublication#offer(DirectBuffer, int, int)} over UDP transport by spying via
 * IPC.
 */
public class EmbeddedExclusiveSpiedThroughput
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
     * @throws InterruptedException if interrupted during linger.
     */
    public static void main(final String[] args) throws InterruptedException
    {
        loadPropertiesFiles(args);

        final RateReporter reporter = new RateReporter(
            TimeUnit.SECONDS.toNanos(1), EmbeddedExclusiveSpiedThroughput::printRate);
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        final AtomicBoolean running = new AtomicBoolean(true);

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .spiesSimulateConnection(true);

        try (MediaDriver mediaDriver = MediaDriver.launch(ctx);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
            Subscription subscription = aeron.addSubscription(CommonContext.SPY_PREFIX + CHANNEL, STREAM_ID);
            ExclusivePublication publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID))
        {
            executor.execute(reporter);
            executor.execute(() -> SamplesUtil.subscriberLoop(
                rateReporterHandler(reporter), FRAGMENT_COUNT_LIMIT, running).accept(subscription));

            final ContinueBarrier barrier = new ContinueBarrier("Execute again?");
            final IdleStrategy idleStrategy = SampleConfiguration.newIdleStrategy();

            do
            {
                System.out.format(
                    "%nStreaming %,d messages of payload length %d bytes to %s on stream id %d%n",
                    NUMBER_OF_MESSAGES, MESSAGE_LENGTH, CHANNEL, STREAM_ID);

                printingActive = true;

                long backPressureCount = 0;
                for (long i = 0; i < NUMBER_OF_MESSAGES; i++)
                {
                    OFFER_BUFFER.putLong(0, i);

                    idleStrategy.reset();
                    while (publication.offer(OFFER_BUFFER, 0, MESSAGE_LENGTH, null) < 0)
                    {
                        backPressureCount++;
                        idleStrategy.idle();
                    }
                }

                System.out.println(
                    "Done streaming. backPressureRatio=" + ((double)backPressureCount / NUMBER_OF_MESSAGES));

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
