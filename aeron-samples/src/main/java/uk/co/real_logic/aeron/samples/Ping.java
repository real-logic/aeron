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

import org.HdrHistogram.Histogram;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.BusySpinIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.aeron.common.concurrent.console.ContinueBarrier;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Ping component of Ping-Pong.
 *
 * Initiates and records times.
 */
public class Ping
{
    private static final int PING_STREAM_ID = SampleConfiguration.PING_STREAM_ID;
    private static final int PONG_STREAM_ID = SampleConfiguration.PONG_STREAM_ID;
    private static final String PING_CHANNEL = SampleConfiguration.PING_CHANNEL;
    private static final String PONG_CHANNEL = SampleConfiguration.PONG_CHANNEL;
    private static final int NUMBER_OF_MESSAGES = SampleConfiguration.NUMBER_OF_MESSAGES;
    private static final int WARMUP_NUMBER_OF_MESSAGES = SampleConfiguration.WARMUP_NUMBER_OF_MESSAGES;
    private static final int WARMUP_NUMBER_OF_ITERATIONS = SampleConfiguration.WARMUP_NUMBER_OF_ITERATIONS;
    private static final int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final boolean EMBEDDED_MEDIA_DRIVER = SampleConfiguration.EMBEDDED_MEDIA_DRIVER;

    private static final UnsafeBuffer ATOMIC_BUFFER = new UnsafeBuffer(ByteBuffer.allocateDirect(MESSAGE_LENGTH));
    private static final Histogram HISTOGRAM = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
    private static final CountDownLatch PONG_CONNECTION_LATCH = new CountDownLatch(1);

    public static void main(final String[] args) throws Exception
    {
        final MediaDriver driver = EMBEDDED_MEDIA_DRIVER ? MediaDriver.launch() : null;
        final Aeron.Context ctx = new Aeron.Context()
            .newConnectionHandler(Ping::newPongConnectionHandler);

        System.out.println("Publishing Ping at " + PING_CHANNEL + " on stream Id " + PING_STREAM_ID);
        System.out.println("Subscribing Pong at " + PONG_CHANNEL + " on stream Id " + PONG_STREAM_ID);
        System.out.println("Message size of " + MESSAGE_LENGTH + " bytes");

        final DataHandler dataHandler = new FragmentAssemblyAdapter(Ping::pongHandler);

        try (final Aeron aeron = Aeron.connect(ctx);
             final Publication pingPublication = aeron.addPublication(PING_CHANNEL, PING_STREAM_ID);
             final Subscription pongSubscription = aeron.addSubscription(PONG_CHANNEL, PONG_STREAM_ID, dataHandler))
        {
            System.out.println("Waiting for new connection from Pong...");

            PONG_CONNECTION_LATCH.await();

            System.out.println(
                "Warming up... " + WARMUP_NUMBER_OF_ITERATIONS + " iterations of " + WARMUP_NUMBER_OF_MESSAGES + " messages");

            for (int i = 0; i < WARMUP_NUMBER_OF_ITERATIONS; i++)
            {
                sendPingAndReceivePong(pingPublication, pongSubscription, WARMUP_NUMBER_OF_MESSAGES);
            }

            final ContinueBarrier barrier = new ContinueBarrier("Execute again?");

            do
            {
                HISTOGRAM.reset();
                System.out.println("Pinging " + NUMBER_OF_MESSAGES + " messages");

                sendPingAndReceivePong(pingPublication, pongSubscription, NUMBER_OF_MESSAGES);

                System.out.println("Histogram of RTT latencies in microseconds.");
                HISTOGRAM.outputPercentileDistribution(System.out, 1000.0);
            }
            while (barrier.await());
        }

        CloseHelper.quietClose(driver);
    }

    private static void sendPingAndReceivePong(
        final Publication pingPublication, final Subscription pongSubscription, final int numMessages)
    {
        final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

        for (int i = 0; i < numMessages; i++)
        {
            do
            {
                ATOMIC_BUFFER.putLong(0, System.nanoTime());
            }
            while (!pingPublication.offer(ATOMIC_BUFFER, 0, MESSAGE_LENGTH));

            while (pongSubscription.poll(FRAGMENT_COUNT_LIMIT) <= 0)
            {
                idleStrategy.idle(0);
            }
        }
    }

    private static void pongHandler(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final long pingTimestamp = buffer.getLong(offset);
        final long rttNs = System.nanoTime() - pingTimestamp;

        HISTOGRAM.recordValue(rttNs);
    }

    private static void newPongConnectionHandler(
        final String channel, final int streamId, final int sessionId, final String sourceInfo)
    {
        if (channel.equals(PONG_CHANNEL) && PONG_STREAM_ID == streamId)
        {
            PONG_CONNECTION_LATCH.countDown();
        }
    }
}
