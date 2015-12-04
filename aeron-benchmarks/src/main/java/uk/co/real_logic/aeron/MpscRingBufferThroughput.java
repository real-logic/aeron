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

import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.BusySpinIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.MessageHandler;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.agrona.concurrent.ringbuffer.RingBufferDescriptor;

/**
 * Note that the secondary results for this benchmark are the important ones. Namely we are interested in the bytes transfer rate
 * but also in the number of messages delivered and the success/fail ratio for the consumer. Note that the benchmark gives a
 * slight head start to producers which are allowed to continue roaming while the measurement actions are run on the consumer
 * thread. This is probably fine because the consumer is significantly faster than the producer in this use case.
 *
 * @author nitsanw
 *
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class MpscRingBufferThroughput
{
    @Param("1000")
    public int burstSize;

    @Param("256")
    public int messageLength;

    @Param("17")
    public int pow2size;

    @Param("1")
    public int producerCount;

    ManyToOneRingBuffer ringBuffer;
    public static final int MESSAGE_COUNT_LIMIT = 256;

    private List<Thread> producerThreads = new ArrayList<Thread>();
    private AtomicBoolean running;
    private final IdleStrategy idle = new BusySpinIdleStrategy();

    @Setup(Level.Trial)
    public void setup() throws Exception
    {
        ringBuffer = new ManyToOneRingBuffer(
                new UnsafeBuffer(ByteBuffer.allocateDirect((1 << pow2size) + RingBufferDescriptor.TRAILER_LENGTH)));
        running = new AtomicBoolean(true);
        for (int i = 0; i < producerCount; i++)
        {
            final Thread pThread = new Thread(new Producer(running, ringBuffer, burstSize, messageLength));

            pThread.setName("publisher");

            pThread.start();
            producerThreads.add(pThread);
        }

    }

    @TearDown(Level.Trial)
    public void tearDown() throws InterruptedException
    {
        running.set(false);
        for (Thread publisherThread : producerThreads)
        {
            publisherThread.join();
        }
    }

    public static final class Producer implements Runnable
    {
        private final AtomicBoolean running;
        private final ManyToOneRingBuffer ringBuffer;
        private final int burstSize;
        private final int messageLength;

        public Producer(final AtomicBoolean running, final ManyToOneRingBuffer ringBuffer, final int burstSize,
                final int messageLength)
        {
            super();
            this.running = running;
            this.ringBuffer = ringBuffer;
            this.burstSize = burstSize;
            this.messageLength = messageLength;
        }

        public void run()
        {
            final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
            final int burstSize = this.burstSize;
            final int messageLength = this.messageLength;
            final AtomicBoolean running = this.running;

            final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(messageLength));

            while (running.get())
            {
                for (int i = 0; i < burstSize; i++)
                {
                    while (!ringBuffer.write(1, buffer, 0, messageLength))
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
    public static class ConsumerCounters implements MessageHandler
    {
        public long failed;
        public long made;
        public long bytes;
        public long messages;

        @Setup(Level.Iteration)
        public void resetCounters()
        {
            messages = bytes = made = failed = 0;
        }

        @Override
        public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
        {
            bytes += length;
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void backoffYield(ConsumerCounters m)
    {
        final int fragmentsRead = ringBuffer.read(m, MESSAGE_COUNT_LIMIT);
        if (0 == fragmentsRead)
        {
            Thread.yield();
            m.failed++;
        }
        else
        {
            m.made++;
            m.messages += fragmentsRead;
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void backoffSpin(ConsumerCounters m)
    {
        final int fragmentsRead = ringBuffer.read(m, MESSAGE_COUNT_LIMIT);
        if (0 == fragmentsRead)
        {
            m.failed++;
        }
        else
        {
            m.made++;
            m.messages += fragmentsRead;
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void backoffBusySpinStrategy(ConsumerCounters m)
    {
        final int fragmentsRead = ringBuffer.read(m, MESSAGE_COUNT_LIMIT);
        idle.idle(fragmentsRead);
        if (0 == fragmentsRead)
        {
            m.failed++;
        }
        else
        {
            m.made++;
            m.messages += fragmentsRead;
        }
    }
}
