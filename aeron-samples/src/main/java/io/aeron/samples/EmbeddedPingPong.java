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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.*;
import org.HdrHistogram.Histogram;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.*;
import org.agrona.console.ContinueBarrier;

import static org.agrona.SystemUtil.loadPropertiesFiles;

/**
 * Latency test using a ping-pong approach to measure RTT and store all results in a {@link Histogram}.
 */
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
    private static final boolean EXCLUSIVE_PUBLICATIONS = SampleConfiguration.EXCLUSIVE_PUBLICATIONS;

    private static final UnsafeBuffer OFFER_BUFFER = new UnsafeBuffer(
        BufferUtil.allocateDirectAligned(MESSAGE_LENGTH, BitUtil.CACHE_LINE_LENGTH));
    private static final Histogram HISTOGRAM = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
    private static final CountDownLatch PONG_IMAGE_LATCH = new CountDownLatch(1);
    private static final IdleStrategy PING_HANDLER_IDLE_STRATEGY = SampleConfiguration.newIdleStrategy();
    private static final IdleStrategy PONG_HANDLER_IDLE_STRATEGY = SampleConfiguration.newIdleStrategy();
    private static final AtomicBoolean RUNNING = new AtomicBoolean(true);

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     * @throws InterruptedException if the thread is interrupted.
     */
    public static void main(final String[] args) throws InterruptedException
    {
        loadPropertiesFiles(args);

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .threadingMode(ThreadingMode.DEDICATED)
            .conductorIdleStrategy(new BackoffIdleStrategy(1, 1, 1000, 1000))
            .receiverIdleStrategy(NoOpIdleStrategy.INSTANCE)
            .senderIdleStrategy(NoOpIdleStrategy.INSTANCE);

        try (MediaDriver mediaDriver = MediaDriver.launch(ctx);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName())))
        {
            final Thread pongThread = startPong(aeron);
            pongThread.start();

            runPing(aeron);
            RUNNING.set(false);
            pongThread.join();

            System.out.println("Shutdown Driver...");
        }
    }

    private static void runPing(final Aeron aeron) throws InterruptedException
    {
        System.out.println("Publishing Ping at " + PING_CHANNEL + " on stream id " + PING_STREAM_ID);
        System.out.println("Subscribing Pong at " + PONG_CHANNEL + " on stream id " + PONG_STREAM_ID);
        System.out.println("Message payload length of " + MESSAGE_LENGTH + " bytes");
        System.out.println("Using exclusive publications: " + EXCLUSIVE_PUBLICATIONS);

        final FragmentAssembler dataHandler = new FragmentAssembler(EmbeddedPingPong::pongHandler);

        try (Subscription pongSubscription = aeron.addSubscription(
            PONG_CHANNEL, PONG_STREAM_ID, EmbeddedPingPong::availablePongImageHandler, null);
            Publication pingPublication = EXCLUSIVE_PUBLICATIONS ?
                aeron.addExclusivePublication(PING_CHANNEL, PING_STREAM_ID) :
                aeron.addPublication(PING_CHANNEL, PING_STREAM_ID))
        {
            System.out.println("Waiting for new image from Pong...");
            PONG_IMAGE_LATCH.await();

            System.out.format("Warming up... %d iterations of %,d messages%n",
                WARMUP_NUMBER_OF_ITERATIONS, WARMUP_NUMBER_OF_MESSAGES);

            for (int i = 0; i < WARMUP_NUMBER_OF_ITERATIONS; i++)
            {
                roundTripMessages(dataHandler, pingPublication, pongSubscription, WARMUP_NUMBER_OF_MESSAGES);
                Thread.yield();
            }

            Thread.sleep(100);
            final ContinueBarrier barrier = new ContinueBarrier("Execute again?");

            do
            {
                HISTOGRAM.reset();
                System.out.format("Pinging %,d messages%n", NUMBER_OF_MESSAGES);

                roundTripMessages(dataHandler, pingPublication, pongSubscription, NUMBER_OF_MESSAGES);

                System.out.println("Histogram of RTT latencies in microseconds.");
                HISTOGRAM.outputPercentileDistribution(System.out, 1000.0);
            }
            while (barrier.await());
        }
    }

    private static Thread startPong(final Aeron aeron)
    {
        return new Thread(() ->
        {
            System.out.println("Subscribing Ping at " + PING_CHANNEL + " on stream id " + PING_STREAM_ID);
            System.out.println("Publishing Pong at " + PONG_CHANNEL + " on stream id " + PONG_STREAM_ID);

            try (Subscription pingSubscription = aeron.addSubscription(PING_CHANNEL, PING_STREAM_ID);
                Publication pongPublication = EXCLUSIVE_PUBLICATIONS ?
                    aeron.addExclusivePublication(PONG_CHANNEL, PONG_STREAM_ID) :
                    aeron.addPublication(PONG_CHANNEL, PONG_STREAM_ID))
            {
                final BufferClaim bufferClaim = new BufferClaim();
                final FragmentHandler fragmentHandler = (buffer, offset, length, header) ->
                    pingHandler(bufferClaim, pongPublication, buffer, offset, length, header);

                while (RUNNING.get())
                {
                    PING_HANDLER_IDLE_STRATEGY.idle(pingSubscription.poll(fragmentHandler, FRAME_COUNT_LIMIT));
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
            while ((offeredPosition = pingPublication.offer(OFFER_BUFFER, 0, MESSAGE_LENGTH, null)) < 0L);

            while (image.position() < offeredPosition)
            {
                final int fragments = image.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);
                PONG_HANDLER_IDLE_STRATEGY.idle(fragments);
            }
        }
    }

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

    static void pingHandler(
        final BufferClaim bufferClaim,
        final Publication pongPublication,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        PING_HANDLER_IDLE_STRATEGY.reset();
        while (pongPublication.tryClaim(length, bufferClaim) <= 0)
        {
            PING_HANDLER_IDLE_STRATEGY.idle();
        }

        bufferClaim
            .flags(header.flags())
            .putBytes(buffer, offset, length)
            .commit();
    }
}
