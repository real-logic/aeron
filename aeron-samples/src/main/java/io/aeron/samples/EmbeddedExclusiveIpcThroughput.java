/*
 * Copyright 2014-2019 Real Logic Ltd.
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
import io.aeron.driver.*;
import io.aeron.logbuffer.*;
import org.agrona.*;
import org.agrona.concurrent.*;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.SystemUtil.loadPropertiesFiles;
import static org.agrona.UnsafeAccess.UNSAFE;

public class EmbeddedExclusiveIpcThroughput
{
    public static final int BURST_LENGTH = 1_000_000;
    public static final int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;
    public static final int MESSAGE_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    public static final String CHANNEL = CommonContext.IPC_CHANNEL;
    public static final int STREAM_ID = SampleConfiguration.STREAM_ID;

    public static void main(final String[] args) throws Exception
    {
        loadPropertiesFiles(args);

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(new NoOpIdleStrategy());

        try (MediaDriver ignore = MediaDriver.launch(ctx);
            Aeron aeron = Aeron.connect();
            ExclusivePublication publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID);
            Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID))
        {
            final Subscriber subscriber = new Subscriber(running, subscription);
            final Thread subscriberThread = new Thread(subscriber);
            subscriberThread.setName("subscriber");
            final Thread publisherThread = new Thread(new Publisher(running, publication));
            publisherThread.setName("publisher");
            final Thread rateReporterThread = new Thread(new RateReporter(running, subscriber));
            rateReporterThread.setName("rate-reporter");

            rateReporterThread.start();
            subscriberThread.start();
            publisherThread.start();

            subscriberThread.join();
            publisherThread.join();
            rateReporterThread.join();
        }
    }

    public static final class RateReporter implements Runnable
    {
        private final AtomicBoolean running;
        private final Subscriber subscriber;

        public RateReporter(final AtomicBoolean running, final Subscriber subscriber)
        {
            this.running = running;
            this.subscriber = subscriber;
        }

        public void run()
        {
            long lastTimeStampMs = System.currentTimeMillis();
            long lastTotalBytes = subscriber.totalBytes();

            while (running.get())
            {
                LockSupport.parkNanos(1_000_000_000);

                final long newTimeStampMs = System.currentTimeMillis();
                final long newTotalBytes = subscriber.totalBytes();

                final long durationMs = newTimeStampMs - lastTimeStampMs;
                final long bytesTransferred = newTotalBytes - lastTotalBytes;

                System.out.format(
                    "Duration %dms - %,d messages - %,d payload bytes%n",
                    durationMs, bytesTransferred / MESSAGE_LENGTH, bytesTransferred);

                lastTimeStampMs = newTimeStampMs;
                lastTotalBytes = newTotalBytes;
            }
        }
    }

    public static final class Publisher implements Runnable
    {
        private final AtomicBoolean running;
        private final ExclusivePublication publication;

        public Publisher(final AtomicBoolean running, final ExclusivePublication publication)
        {
            this.running = running;
            this.publication = publication;
        }

        public void run()
        {
            final ExclusivePublication publication = this.publication;
            final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(
                publication.maxMessageLength(), CACHE_LINE_LENGTH);
            final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);
            long backPressureCount = 0;
            long totalMessageCount = 0;

            outputResults:
            while (running.get())
            {
                for (int i = 0; i < BURST_LENGTH; i++)
                {
                    while (publication.offer(buffer, 0, MESSAGE_LENGTH) <= 0)
                    {
                        ++backPressureCount;
                        if (!running.get())
                        {
                            break outputResults;
                        }
                    }

                    ++totalMessageCount;
                }
            }

            final double backPressureRatio = backPressureCount / (double)totalMessageCount;
            System.out.format("Publisher back pressure ratio: %f%n", backPressureRatio);
        }
    }

    public static final class Subscriber implements Runnable, FragmentHandler
    {
        private static final long TOTAL_BYTES_OFFSET;

        static
        {
            try
            {
                TOTAL_BYTES_OFFSET = UNSAFE.objectFieldOffset(Subscriber.class.getDeclaredField("totalBytes"));
            }
            catch (final Exception ex)
            {
                throw new RuntimeException(ex);
            }
        }

        private final AtomicBoolean running;
        private final Subscription subscription;

        private volatile long totalBytes = 0;

        public Subscriber(final AtomicBoolean running, final Subscription subscription)
        {
            this.running = running;
            this.subscription = subscription;
        }

        public long totalBytes()
        {
            return totalBytes;
        }

        public void run()
        {
            while (!subscription.isConnected())
            {
                Thread.yield();
            }

            final Image image = subscription.imageAtIndex(0);

            long failedPolls = 0;
            long successfulPolls = 0;

            while (running.get())
            {
                final int fragmentsRead = image.poll(this, MESSAGE_COUNT_LIMIT);
                if (0 == fragmentsRead)
                {
                    ++failedPolls;
                }
                else
                {
                    ++successfulPolls;
                }
            }

            final double failureRatio = failedPolls / (double)(successfulPolls + failedPolls);
            System.out.format("Subscriber poll failure ratio: %f%n", failureRatio);
        }

        public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            UNSAFE.putOrderedLong(this, TOTAL_BYTES_OFFSET, totalBytes + length);
        }
    }
}
