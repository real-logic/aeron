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

import io.aeron.*;
import io.aeron.driver.MediaDriver;
import org.HdrHistogram.Histogram;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.console.ContinueBarrier;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Ping component of Ping-Pong latency test recorded to a histogram to capture full distribution..
 * <p>
 * Initiates messages sent to {@link Pong} and records times.
 * @see Pong
 */
public class Ping
{
    private static final int PING_STREAM_ID = SampleConfiguration.PING_STREAM_ID;
    private static final int PONG_STREAM_ID = SampleConfiguration.PONG_STREAM_ID;
    private static final long NUMBER_OF_MESSAGES = SampleConfiguration.NUMBER_OF_MESSAGES;
    private static final long WARMUP_NUMBER_OF_MESSAGES = SampleConfiguration.WARMUP_NUMBER_OF_MESSAGES;
    private static final int WARMUP_NUMBER_OF_ITERATIONS = SampleConfiguration.WARMUP_NUMBER_OF_ITERATIONS;
    private static final int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final boolean EMBEDDED_MEDIA_DRIVER = SampleConfiguration.EMBEDDED_MEDIA_DRIVER;
    private static final String PING_CHANNEL = SampleConfiguration.PING_CHANNEL;
    private static final String PONG_CHANNEL = SampleConfiguration.PONG_CHANNEL;

    private static final UnsafeBuffer OFFER_BUFFER = new UnsafeBuffer(
        BufferUtil.allocateDirectAligned(MESSAGE_LENGTH, BitUtil.CACHE_LINE_LENGTH));
    private static final Histogram HISTOGRAM = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
    private static final CountDownLatch LATCH = new CountDownLatch(1);
    private static final IdleStrategy POLLING_IDLE_STRATEGY = new BusySpinIdleStrategy();

    public static void main(final String[] args) throws Exception
    {
        final MediaDriver driver = EMBEDDED_MEDIA_DRIVER ? MediaDriver.launchEmbedded() : null;
        final Aeron.Context ctx = new Aeron.Context().availableImageHandler(Ping::availablePongImageHandler);
        final FragmentHandler fragmentHandler = new FragmentAssembler(Ping::pongHandler);

        if (EMBEDDED_MEDIA_DRIVER)
        {
            ctx.aeronDirectoryName(driver.aeronDirectoryName());
        }

        System.out.println("Publishing Ping at " + PING_CHANNEL + " on stream Id " + PING_STREAM_ID);
        System.out.println("Subscribing Pong at " + PONG_CHANNEL + " on stream Id " + PONG_STREAM_ID);
        System.out.println("Message length of " + MESSAGE_LENGTH + " bytes");

        try (Aeron aeron = Aeron.connect(ctx))
        {
            System.out.println(
                "Warming up... " + WARMUP_NUMBER_OF_ITERATIONS +
                " iterations of " + WARMUP_NUMBER_OF_MESSAGES + " messages");

            try (Publication publication = aeron.addPublication(PING_CHANNEL, PING_STREAM_ID);
                Subscription subscription = aeron.addSubscription(PONG_CHANNEL, PONG_STREAM_ID))
            {
                LATCH.await();

                for (int i = 0; i < WARMUP_NUMBER_OF_ITERATIONS; i++)
                {
                    roundTripMessages(fragmentHandler, publication, subscription, WARMUP_NUMBER_OF_MESSAGES);
                }

                Thread.sleep(100);
                final ContinueBarrier barrier = new ContinueBarrier("Execute again?");

                do
                {
                    HISTOGRAM.reset();
                    System.out.println("Pinging " + NUMBER_OF_MESSAGES + " messages");

                    roundTripMessages(fragmentHandler, publication, subscription, NUMBER_OF_MESSAGES);
                    System.out.println("Histogram of RTT latencies in microseconds.");

                    HISTOGRAM.outputPercentileDistribution(System.out, 1000.0);
                }
                while (barrier.await());
            }
        }

        CloseHelper.quietClose(driver);
    }

    private static void roundTripMessages(
        final FragmentHandler fragmentHandler,
        final Publication publication,
        final Subscription subscription,
        final long count)
    {
        while (!subscription.isConnected())
        {
            Thread.yield();
        }

        final Image image = subscription.imageAtIndex(0);

        for (long i = 0; i < count; i++)
        {
            long offeredPosition;

            do
            {
                OFFER_BUFFER.putLong(0, System.nanoTime());
            }
            while ((offeredPosition = publication.offer(OFFER_BUFFER, 0, MESSAGE_LENGTH)) < 0L);

            POLLING_IDLE_STRATEGY.reset();

            do
            {
                while (image.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT) <= 0)
                {
                    POLLING_IDLE_STRATEGY.idle();
                }
            }
            while (image.position() < offeredPosition);
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
        System.out.format(
            "Available image: channel=%s streamId=%d session=%d%n",
            subscription.channel(), subscription.streamId(), image.sessionId());

        if (PONG_STREAM_ID == subscription.streamId() && PONG_CHANNEL.equals(subscription.channel()))
        {
            LATCH.countDown();
        }
    }
}
