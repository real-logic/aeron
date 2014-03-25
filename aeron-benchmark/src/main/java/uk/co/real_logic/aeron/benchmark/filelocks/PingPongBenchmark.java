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
package uk.co.real_logic.aeron.benchmark.filelocks;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class PingPongBenchmark
{
    private static final int RUN_COUNT = 1_000_000;
    //private static final int RUN_COUNT = 10_000;

    private static final int NANOTIME_ESTIMATE_COUNT = 1000;

    private static final int PING_OFFSET = 0;
    private static final int PONG_OFFSET = 1;

    private final Lock lock;
    private final Condition pinged;
    private final Condition ponged;
    private final ByteBuffer data;

    public PingPongBenchmark(final Lock lock,
                             final Condition pinged,
                             final Condition ponged,
                             final ByteBuffer data) throws IOException
    {
        this.lock = lock;
        this.pinged = pinged;
        this.ponged = ponged;
        this.data = data;
    }

    public Runnable pinger()
    {
        return new Pinger();
    }

    public Runnable ponger()
    {
        return new Ponger();
    }

    private abstract class Runner implements Runnable
    {

        public void runLoop()
        {
            try
            {
                String name = Thread.currentThread().getName();
                for (int i = 1; i < RUN_COUNT + 1; i++)
                {
                    System.out.println(name + i);
                    runIteration(i);
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        protected abstract void runIteration(final int i) throws InterruptedException;
    }

    public final class Pinger extends Runner
    {
        private final long[] times = new long[RUN_COUNT + 1];

        @Override
        public void run()
        {
            Thread.currentThread().setName("Ping Thread");
            runLoop();
            times[RUN_COUNT] = System.nanoTime();
            printTimes(times);
        }

        @Override
        protected void runIteration(final int i) throws InterruptedException
        {
            times[i] = System.nanoTime();
            lock.lock();
            try
            {
                data.putInt(PING_OFFSET, i);
                pinged.signal();
            }
            finally
            {
                lock.unlock();
            }

            lock.lock();
            try
            {
                while (data.getInt(PONG_OFFSET) != i)
                {
                    ponged.await();
                }
            }
            finally
            {
                lock.unlock();
            }
        }
    }

    public final class Ponger extends Runner
    {
        @Override
        public void run()
        {
            Thread.currentThread().setName("Pong Thread");
            runLoop();
        }

        @Override
        protected void runIteration(final int i) throws InterruptedException
        {
            lock.lock();
            try
            {
                while (data.getInt(PING_OFFSET) != i)
                {
                    System.out.println("start loop");
                    pinged.await();
                }
            }
            finally
            {
                lock.unlock();
            }

            lock.lock();
            try
            {
                data.putInt(PONG_OFFSET, i);
                ponged.signal();
            }
            finally
            {
                lock.unlock();
            }
        }
    }

    private static void printTimes(final long[] times)
    {

        long costOfNanoTime = estimateCostOfNanoTime();
        try (PrintStream out = new PrintStream(new FileOutputStream("timings.log")))
        {
            long startTime = times[0];
            for (int i = 1; i < RUN_COUNT + 1; i++)
            {
                long iterationTime = (times[i] - startTime) - costOfNanoTime;
                out.println(iterationTime);
                startTime = times[i];
            }
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
    }

    private static long estimateCostOfNanoTime()
    {
        long[] sink = new long[NANOTIME_ESTIMATE_COUNT];
        for (int i = 0; i < NANOTIME_ESTIMATE_COUNT; i++)
        {
            sink[i] = System.nanoTime();
        }

        // Printout to avoid DCE
        System.out.println("Ignore this output: " + sink[new Random().nextInt(NANOTIME_ESTIMATE_COUNT)]);

        long totalTime = sink[NANOTIME_ESTIMATE_COUNT - 1] - sink[0];
        return totalTime / NANOTIME_ESTIMATE_COUNT;
    }

}
