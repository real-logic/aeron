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

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.DataHandler;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.RateReporter;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.nio.ByteBuffer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static uk.co.real_logic.aeron.benchmarks.BenchmarkUtil.CHANNEL;
import static uk.co.real_logic.aeron.benchmarks.BenchmarkUtil.STREAM_ID;
import static uk.co.real_logic.aeron.benchmarks.BenchmarkUtil.subscriberLoop;

/**
 * Goal: test for critical path allocation.
 *
 * For infinite amount of time: Send a messages
 */
public class AllocationSoakTestRunner
{
    private static final int CAPACITY = 256;
    private static final AtomicBuffer PUBLISHING_BUFFER = new AtomicBuffer(ByteBuffer.allocateDirect(CAPACITY));
    private static final int FRAME_COUNT_LIMIT = 10;

    public static void main(String[] args) throws Exception
    {
        BenchmarkUtil.useSharedMemoryOnLinux();

        final RateReporter reporter = new RateReporter(SECONDS.toNanos(1), AllocationSoakTestRunner::printRateOnPause);
        final DataHandler handler = (buffer, offset, length, sessionId, flags) -> reporter.onMessage(1, length);

        try (final MediaDriver driver = MediaDriver.launch();
             final Aeron publishingClient = Aeron.connect(new Aeron.Context());
             final Aeron consumingClient = Aeron.connect(new Aeron.Context());
             final Publication publication = publishingClient.addPublication(CHANNEL, STREAM_ID, 0);
             final Subscription subscription = consumingClient.addSubscription(CHANNEL, STREAM_ID, handler))
        {
            publish(reporter, publication);

            subscribe(subscription);

            reporter.run();
        }
    }

    private static void subscribe(final Subscription subscription)
    {
        new Thread("Subscriber")
        {
            public void run()
            {
                subscriberLoop(FRAME_COUNT_LIMIT).accept(subscription);
            }
        }.start();
    }

    private static void publish(final RateReporter reporter, final Publication publication)
    {
        new Thread("Publisher")
        {
            public void run()
            {
                for (int i = 0; true; i++)
                {
                    PUBLISHING_BUFFER.putLong(0, i);

                    while (!publication.offer(PUBLISHING_BUFFER, 0, CAPACITY))
                    {
                        Thread.yield();
                    }

                    reporter.onMessage(1, CAPACITY);

                    Thread.yield();
                }
            }
        }.start();
    }

    public static void printRateOnPause(
        final double messagesPerSec, final double bytesPerSec, final long totalMessages, final long totalBytes)
    {
        if (messagesPerSec == 0.0 || bytesPerSec == 0.0)
        {
            System.out.println(String.format("%.02g msgs/sec, %.02g bytes/sec", messagesPerSec, bytesPerSec));
        }
    }

}
