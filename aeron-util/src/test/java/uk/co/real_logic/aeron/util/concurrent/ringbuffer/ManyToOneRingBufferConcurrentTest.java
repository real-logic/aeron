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
package uk.co.real_logic.aeron.util.concurrent.ringbuffer;

import org.junit.Test;
import uk.co.real_logic.aeron.util.BitUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.CyclicBarrier;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ManyToOneRingBufferConcurrentTest
{
    public static final int EVENT_TYPE_ID = 7;

    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect((256) + RingBufferDescriptor.TRAILER_SIZE);
    private final AtomicBuffer atomicBuffer = new AtomicBuffer(byteBuffer);
    private final RingBuffer ringBuffer = new ManyToOneRingBuffer(atomicBuffer);

    @Test
    public void shouldProvideCorrelationIds() throws Exception
    {
        final int reps = 10 * 1000 * 1000;
        final int numThreads = 2;
        final CyclicBarrier barrier = new CyclicBarrier(numThreads);
        final Thread[] threads = new Thread[numThreads];

        for (int i = 0; i < numThreads; i++)
        {
            threads[i] = new Thread(
                () ->
                {
                    try
                    {
                        barrier.await();
                    }
                    catch (final Exception ignore)
                    {
                    }

                    for (int r = 0; r < reps; r++)
                    {
                        ringBuffer.nextCorrelationId();
                    }
                }
            );

            threads[i].start();
        }

        for (final Thread t : threads)
        {
            t.join();
        }

        assertThat(Long.valueOf(ringBuffer.nextCorrelationId()), is(Long.valueOf(reps * numThreads)));
    }

    @Test
    public void shouldExchangeEvents() throws Exception
    {
        final int reps = 10 * 1000 * 1000;
        final int numProducers = 2;
        final CyclicBarrier barrier = new CyclicBarrier(numProducers);
        final Thread[] threads = new Thread[numProducers];

        for (int i = 0; i < numProducers; i++)
        {
            threads[i] = new Thread(new Producer(i, barrier, reps));
            threads[i].start();
        }

        final int[] counts = new int[numProducers];

        final EventHandler handler =
            (eventTypeId, buffer, index, length) ->
            {
                final int producerId = buffer.getInt(index);
                final int iteration = buffer.getInt(index + BitUtil.SIZE_OF_INT);

                final int count = counts[producerId];
                assertThat(Integer.valueOf(iteration), is(Integer.valueOf(count)));

                counts[producerId]++;
            };

        int eventCount = 0;
        while (eventCount < (reps * numProducers))
        {
            final int readCount = ringBuffer.read(handler);
            if (0 == readCount)
            {
                Thread.yield();
            }

            eventCount += readCount;
        }

        assertThat(Integer.valueOf(eventCount), is(Integer.valueOf(reps * numProducers)));
    }

    private class Producer implements Runnable
    {
        private final int producerId;
        private final CyclicBarrier barrier;
        private final int reps;

        public Producer(final int producerId, final CyclicBarrier barrier, final int reps)
        {
            this.producerId = producerId;
            this.barrier = barrier;
            this.reps = reps;
        }

        public void run()
        {
            try
            {
                barrier.await();
            }
            catch (final Exception ignore)
            {
            }

            final int length = BitUtil.SIZE_OF_INT * 2;
            final int repsValueOffset = BitUtil.SIZE_OF_INT;
            final AtomicBuffer srcBuffer = new AtomicBuffer(new byte[1024]);

            srcBuffer.putInt(0, producerId);

            for (int i = 0; i < reps; i++)
            {
                srcBuffer.putInt(repsValueOffset, i);

                while (!ringBuffer.write(EVENT_TYPE_ID, srcBuffer, 0, length))
                {
                    Thread.yield();
                }
            }
        }
    }
}
