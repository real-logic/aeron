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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.HdrHistogram.Histogram;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.console.ContinueBarrier;

import static org.agrona.SystemUtil.loadPropertiesFiles;

public class EmbeddedPingPong
{
    private static final int PING_STREAM_ID = SampleConfiguration.PING_STREAM_ID;
    private static final int PONG_STREAM_ID = SampleConfiguration.PONG_STREAM_ID;
    private static final long NUMBER_OF_MESSAGES = SampleConfiguration.NUMBER_OF_MESSAGES;
    private static final long WARMUP_NUMBER_OF_MESSAGES = SampleConfiguration.WARMUP_NUMBER_OF_MESSAGES;
    private static final int WARMUP_NUMBER_OF_ITERATIONS = SampleConfiguration.WARMUP_NUMBER_OF_ITERATIONS;
    private static final int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final int FRAME_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final String PING_CHANNEL = SampleConfiguration.PING_CHANNEL;
    private static final String PONG_CHANNEL = SampleConfiguration.PONG_CHANNEL;

    private static final UnsafeBuffer OFFER_BUFFER = new UnsafeBuffer(
        BufferUtil.allocateDirectAligned(MESSAGE_LENGTH, BitUtil.CACHE_LINE_LENGTH));
    private static final Histogram HISTOGRAM = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
    private static final CountDownLatch PONG_IMAGE_LATCH = new CountDownLatch(1);
    private static final BusySpinIdleStrategy PING_HANDLER_IDLE_STRATEGY = new BusySpinIdleStrategy();
    private static final BusySpinIdleStrategy PONG_HANDLER_IDLE_STRATEGY = new BusySpinIdleStrategy();
    private static final AtomicBoolean RUNNING = new AtomicBoolean(true);

    public static void main(final String[] args) throws Exception
    {
        loadPropertiesFiles(args);

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .threadingMode(ThreadingMode.DEDICATED)
            .conductorIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1))
            .receiverIdleStrategy(new NoOpIdleStrategy())
            .senderIdleStrategy(new NoOpIdleStrategy());

        try (MediaDriver ignored = MediaDriver.launch(ctx))
        {
            final Thread pongThread = startPong(ignored.aeronDirectoryName());
            pongThread.start();

            runPing(ignored.aeronDirectoryName());
            RUNNING.set(false);
            pongThread.join();

            System.out.println("Shutdown Driver...");
        }
    }

    private static void runPing(final String embeddedDirName) throws InterruptedException
    {
        final Aeron.Context ctx = new Aeron.Context()
            .availableImageHandler(EmbeddedPingPong::availablePongImageHandler)
            .aeronDirectoryName(embeddedDirName);

        System.out.println("Publishing Ping at " + PING_CHANNEL + " on stream Id " + PING_STREAM_ID);
        System.out.println("Subscribing Pong at " + PONG_CHANNEL + " on stream Id " + PONG_STREAM_ID);
        System.out.println("Message payload length of " + MESSAGE_LENGTH + " bytes");

        final FragmentAssembler dataHandler = new FragmentAssembler(EmbeddedPingPong::pongHandler);

        try (Aeron aeron = Aeron.connect(ctx);
            Publication pingPublication = aeron.addPublication(PING_CHANNEL, PING_STREAM_ID);
            Subscription pongSubscription = aeron.addSubscription(PONG_CHANNEL, PONG_STREAM_ID))
        {
            System.out.println("Waiting for new image from Pong...");

            PONG_IMAGE_LATCH.await();

            System.out.println(
                "Warming up... " + WARMUP_NUMBER_OF_ITERATIONS +
                " iterations of " + WARMUP_NUMBER_OF_MESSAGES + " messages");

            for (int i = 0; i < WARMUP_NUMBER_OF_ITERATIONS; i++)
            {
                roundTripMessages(dataHandler, pingPublication, pongSubscription, WARMUP_NUMBER_OF_MESSAGES);
            }

            Thread.sleep(100);
            final ContinueBarrier barrier = new ContinueBarrier("Execute again?");

            do
            {
                HISTOGRAM.reset();
                System.out.println("Pinging " + NUMBER_OF_MESSAGES + " messages");

                roundTripMessages(dataHandler, pingPublication, pongSubscription, NUMBER_OF_MESSAGES);

                System.out.println("Histogram of RTT latencies in microseconds.");
                HISTOGRAM.outputPercentileDistribution(System.out, 1000.0);
            }
            while (barrier.await());
        }
    }

    private static Thread startPong(final String embeddedDirName)
    {
        return new Thread(() ->
        {
            System.out.println("Subscribing Ping at " + PING_CHANNEL + " on stream Id " + PING_STREAM_ID);
            System.out.println("Publishing Pong at " + PONG_CHANNEL + " on stream Id " + PONG_STREAM_ID);

            final Aeron.Context ctx = new Aeron.Context().aeronDirectoryName(embeddedDirName);

            try (Aeron aeron = Aeron.connect(ctx);
                Publication pongPublication = aeron.addPublication(PONG_CHANNEL, PONG_STREAM_ID);
                Subscription pingSubscription = aeron.addSubscription(PING_CHANNEL, PING_STREAM_ID))
            {
                final FragmentAssembler dataHandler = new FragmentAssembler(
                    (buffer, offset, length, header) -> pingHandler(pongPublication, buffer, offset, length));

                while (RUNNING.get())
                {
                    PING_HANDLER_IDLE_STRATEGY.idle(pingSubscription.poll(dataHandler, FRAME_COUNT_LIMIT));
                }

                System.out.println("Shutting down...");
            }
        });
    }

    private static void roundTripMessages(
        final FragmentHandler fragmentHandler,
        final Publication pingPublication,
        final Subscription pongSubscription,
        final long numMessages)
    {
        while (!pongSubscription.isConnected())
        {
            Thread.yield();
        }

        final Image image = pongSubscription.imageAtIndex(0);

        for (long i = 0; i < numMessages; i++)
        {
            long offeredPosition;

            do
            {
                OFFER_BUFFER.putLong(0, System.nanoTime());
            }
            while ((offeredPosition = pingPublication.offer(OFFER_BUFFER, 0, MESSAGE_LENGTH)) < 0L);

            PONG_HANDLER_IDLE_STRATEGY.reset();
            do
            {
                while (image.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT) <= 0)
                {
                    PONG_HANDLER_IDLE_STRATEGY.idle();
                }
            }
            while (image.position() < offeredPosition);
        }
    }

    @SuppressWarnings("unused")
    private static void pongHandler(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final long pingTimestamp = buffer.getLong(offset);
        final long rttNs = System.nanoTime() - pingTimestamp;

        HISTOGRAM.recordValue(rttNs);
    }

    private static void availablePongImageHandler(final Image image)
    {
        final Subscription subscription = image.subscription();
        if (PONG_STREAM_ID == subscription.streamId() && PONG_CHANNEL.equals(subscription.channel()))
        {
            PONG_IMAGE_LATCH.countDown();
        }
    }

    public static void pingHandler(
        final Publication pongPublication, final DirectBuffer buffer, final int offset, final int length)
    {
        if (pongPublication.offer(buffer, offset, length) > 0L)
        {
            return;
        }

        PING_HANDLER_IDLE_STRATEGY.reset();

        while (pongPublication.offer(buffer, offset, length) < 0L)
        {
            PING_HANDLER_IDLE_STRATEGY.idle();
        }
    }
}
