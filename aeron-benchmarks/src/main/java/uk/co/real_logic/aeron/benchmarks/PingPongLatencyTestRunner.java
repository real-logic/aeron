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
package uk.co.real_logic.aeron.benchmarks;

import org.HdrHistogram.Histogram;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.DataHandler;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.BackoffIdleStrategy;
import uk.co.real_logic.aeron.common.BitUtil;
import uk.co.real_logic.aeron.common.RateReporter;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static uk.co.real_logic.aeron.benchmarks.BenchmarkUtil.*;

/**
 * Goal: test for latency of ping-pong timings.
 *
 * For infinite amount of time: Send a messages
 */
// TODO: parameterise by backoff strategies
public class PingPongLatencyTestRunner
{
    private static final int CAPACITY = BitUtil.SIZE_OF_LONG;
    private static final AtomicBuffer PUBLISHING_BUFFER = new AtomicBuffer(ByteBuffer.allocateDirect(CAPACITY));
    private static final int MAX_BATCH_SIZE = 10;
    private static final int NUMBER_OF_MESSAGES = 100_000_000;

    private static long sentTime;


    public static void main(String[] args) throws Exception
    {
        BenchmarkUtil.useSharedMemoryOnLinux();

        final DataHandler handler = (buffer, offset, length, sessionId, flags) -> sentTime = buffer.getLong(offset);
        final Histogram histogram = new Histogram(10_000_000, 5);

        try (final MediaDriver driver = MediaDriver.launch();
             final Aeron publishingClient = Aeron.connect(new Aeron.Context());
             final Aeron consumingClient = Aeron.connect(new Aeron.Context());
             final Publication publication = publishingClient.addPublication(CHANNEL, STREAM_ID);
             final Subscription subscription = consumingClient.addSubscription(CHANNEL, STREAM_ID, handler))
        {
            publish(publication);

            subscribe(subscription, histogram);

            while (true)
            {
                try
                {
                    Thread.sleep(Long.MAX_VALUE);
                }
                catch (Throwable e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void subscribe(final Subscription subscription, final Histogram histogram)
    {
        new Thread("Subscriber")
        {
            public void run()
            {
                final BackoffIdleStrategy idleStrategy = new BackoffIdleStrategy(
                        100,
                        100,
                        TimeUnit.MICROSECONDS.toNanos(1),
                        TimeUnit.MICROSECONDS.toNanos(100));

                try
                {
                    int i = 0;

                    while (true)
                    {
                        final int fragmentsRead = subscription.poll(1);
                        if (fragmentsRead == 1) {
                            final long readTime = System.nanoTime();
                            long transportTime = readTime - sentTime;
                            histogram.recordValue(transportTime / 1000);

                            if ((i % 1_000_000) == 0)
                            {
                                System.out.printf(
                                        "Worst: %d, 99.9: %d, 99: %d, 50: %d\n",
                                        histogram.getMaxValue(),
                                        histogram.getValueAtPercentile(99.9),
                                        histogram.getValueAtPercentile(99),
                                        histogram.getValueAtPercentile(50));
                            }

                            i++;
                        }

                        idleStrategy.idle(fragmentsRead);
                    }
                }
                catch (final Throwable ex)
                {
                    ex.printStackTrace();
                }
            }
        }.start();
    }

    private static void publish(final Publication publication)
    {
        new Thread("Publisher")
        {
            public void run()
            {
                for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
                {
                    PUBLISHING_BUFFER.putLong(0, System.nanoTime());

                    while (!publication.offer(PUBLISHING_BUFFER, 0, CAPACITY))
                    {
                        Thread.yield();
                    }

                    Thread.yield();
                }
            }
        }.start();
    }
}
