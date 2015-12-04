/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.driver.MediaDriver.Context;
import uk.co.real_logic.aeron.driver.ThreadingMode;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.BusySpinIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

/**
 * Note that the secondary results for this benchmark are the important ones. Namely we are interested in the bytes transfer rate
 * but also in the number of fragments delivered and the success/fail ratio for the subscriber. Note that the benchmark gives a
 * slight head start to publishers which are allowed to continue roaming while the measurement actions are run on the subscriber
 * thread. This is probably fine because the consumer is significantly faster than the producer in this use case.
 *
 * @author nitsanw
 *
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class AeronPubSubThroughput
{
    @Param("1000")
    public int burstSize;

    @Param("256")
    public int messageLength;

    @Param("1")
    public int publisherCount;

    @Param("ipc")
    public String channel = CommonContext.IPC_CHANNEL;

    public static final int MESSAGE_COUNT_LIMIT = 256;
    public static final int STREAM_ID = 10;
    private Image image;

    private Aeron aeron;
    private Publication publication;
    private Subscription subscription;
    private List<Thread> publisherThreads = new ArrayList<Thread>();
    private AtomicBoolean running;
    private final IdleStrategy idle = new BusySpinIdleStrategy();
    private Context ctx;

    @SuppressWarnings("resource")
    @Setup(Level.Trial)
    public void setup() throws Exception
    {
        ctx = new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .sharedIdleStrategy(new NoOpIdleStrategy()).dirsDeleteOnStart(true);

        MediaDriver.launch(ctx);
        aeron = Aeron.connect();
        final String uri;
        switch (channel)
        {
        case "ipc":
            uri = CommonContext.IPC_CHANNEL;
            break;
        case "udp":
            uri = "udp://localhost:40123";
            break;
        default:
            uri = channel;
        }
        publication = aeron.addPublication(uri, STREAM_ID);
        subscription = aeron.addSubscription(uri, STREAM_ID);
        running = new AtomicBoolean(true);
        for (int i = 0; i < publisherCount; i++)
        {
            final Thread publisherThread = new Thread(new Publisher(running, publication, burstSize, messageLength));

            publisherThread.setName("publisher");

            publisherThread.start();
            publisherThreads.add(publisherThread);
        }
        while (subscription.imageCount() == 0)
        {
            // wait for an image to be ready
            Thread.yield();
        }

        image = subscription.images().get(0);

    }

    @TearDown(Level.Trial)
    public void tearDown() throws InterruptedException
    {
        running.set(false);
        for (Thread publisherThread : publisherThreads)
        {
            publisherThread.join();
        }
        // driver.close();
        aeron.close();
        publication.close();
        subscription.close();
        ctx.deleteAeronDirectory();
    }

    public static final class Publisher implements Runnable
    {
        private final AtomicBoolean running;
        private final Publication publication;
        private final int burstSize;
        private final int messageLength;

        public Publisher(final AtomicBoolean running, final Publication publication, final int burstSize, final int messageLength)
        {
            super();
            this.running = running;
            this.publication = publication;
            this.burstSize = burstSize;
            this.messageLength = messageLength;
        }

        public void run()
        {
            final Publication publication = this.publication;
            final int burstSize = this.burstSize;
            final int messageLength = this.messageLength;
            final AtomicBoolean running = this.running;

            final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(publication.maxMessageLength()));

            while (running.get())
            {
                for (int i = 0; i < burstSize; i++)
                {
                    while (publication.offer(buffer, 0, messageLength) <= 0)
                    {
                        if (!running.get())
                        {
                            return;
                        }
                    }
                }
            }
        }

    }

    @AuxCounters
    @State(Scope.Thread)
    public static class SubscriberCounters implements FragmentHandler
    {
        public long failed;
        public long made;
        public long bytes;
        public long fragments;

        @Setup(Level.Iteration)
        public void resetCounters()
        {
            fragments = bytes = made = failed = 0;
        }

        public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            bytes += length;
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void backoffYield(SubscriberCounters m)
    {
        final int fragmentsRead = image.poll(m, MESSAGE_COUNT_LIMIT);
        if (0 == fragmentsRead)
        {
            Thread.yield();
            m.failed++;
        }
        else
        {
            m.made++;
            m.fragments += fragmentsRead;
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void backoffSpin(SubscriberCounters m)
    {
        final int fragmentsRead = image.poll(m, MESSAGE_COUNT_LIMIT);
        if (0 == fragmentsRead)
        {
            m.failed++;
        }
        else
        {
            m.made++;
            m.fragments += fragmentsRead;
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void backoffBusySpinStrategy(SubscriberCounters m)
    {
        final int fragmentsRead = image.poll(m, MESSAGE_COUNT_LIMIT);
        idle.idle(fragmentsRead);
        if (0 == fragmentsRead)
        {
            m.failed++;
        }
        else
        {
            m.made++;
            m.fragments += fragmentsRead;
        }
    }
}
