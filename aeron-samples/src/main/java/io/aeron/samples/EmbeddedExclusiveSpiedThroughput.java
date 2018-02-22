/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.samples;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.console.ContinueBarrier;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.samples.SamplesUtil.rateReporterHandler;
import static org.agrona.SystemUtil.loadPropertiesFiles;

public class EmbeddedExclusiveSpiedThroughput
{
    private static final long NUMBER_OF_MESSAGES = SampleConfiguration.NUMBER_OF_MESSAGES;
    private static final long LINGER_TIMEOUT_MS = SampleConfiguration.LINGER_TIMEOUT_MS;
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final String CHANNEL = SampleConfiguration.CHANNEL;

    private static final UnsafeBuffer ATOMIC_BUFFER = new UnsafeBuffer(
        BufferUtil.allocateDirectAligned(MESSAGE_LENGTH, BitUtil.CACHE_LINE_LENGTH));
    private static final BusySpinIdleStrategy OFFER_IDLE_STRATEGY = new BusySpinIdleStrategy();

    private static volatile boolean printingActive = true;

    public static void main(final String[] args) throws Exception
    {
        loadPropertiesFiles(args);

        final RateReporter reporter = new RateReporter(
            TimeUnit.SECONDS.toNanos(1), EmbeddedExclusiveSpiedThroughput::printRate);
        final FragmentHandler rateReporterHandler = rateReporterHandler(reporter);
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        final AtomicBoolean running = new AtomicBoolean(true);

        final MediaDriver.Context ctx = new MediaDriver.Context().spiesSimulateConnection(true);

        try (MediaDriver ignore = MediaDriver.launch(ctx);
            Aeron aeron = Aeron.connect();
            ExclusivePublication publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID);
            Subscription subscription = aeron.addSubscription(CommonContext.SPY_PREFIX + CHANNEL, STREAM_ID))
        {
            executor.execute(reporter);
            executor.execute(() -> SamplesUtil.subscriberLoop(
                rateReporterHandler, FRAGMENT_COUNT_LIMIT, running).accept(subscription));

            final ContinueBarrier barrier = new ContinueBarrier("Execute again?");

            do
            {
                System.out.format(
                    "%nStreaming %,d messages of payload length %d bytes to %s on stream Id %d%n",
                    NUMBER_OF_MESSAGES, MESSAGE_LENGTH, CHANNEL, STREAM_ID);

                printingActive = true;

                long backPressureCount = 0;
                for (long i = 0; i < NUMBER_OF_MESSAGES; i++)
                {
                    ATOMIC_BUFFER.putLong(0, i);

                    OFFER_IDLE_STRATEGY.reset();
                    while (publication.offer(ATOMIC_BUFFER, 0, ATOMIC_BUFFER.capacity()) < 0)
                    {
                        OFFER_IDLE_STRATEGY.idle();
                        backPressureCount++;
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

    public static void printRate(
        final double messagesPerSec, final double bytesPerSec, final long totalFragments, final long totalBytes)
    {
        if (printingActive)
        {
            System.out.format(
                "%.02g msgs/sec, %.02g bytes/sec, totals %d messages %d MB payloads%n",
                messagesPerSec, bytesPerSec, totalFragments, totalBytes / (1024 * 1024));
        }
    }
}
