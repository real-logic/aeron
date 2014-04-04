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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class FileLockBenchmark
{

    public static final String TMPFS_FILE = "/dev/shm/filelock_benchmark";
    public static final String PING_FILE = "/dev/shm/ping_condition_file";
    public static final String PONG_FILE = "/dev/shm/pong_condition_file";

    private static final int RUN_COUNT = 1_000_000;

    private static final int PING_OFFSET = 0;
    private static final int PONG_OFFSET = 1;

    private static final Signaller PINGED;
    private static final Signaller PONGED;
    private static final ByteBuffer DATA;

    static
    {
        PINGED = new FileLockSignaller(PING_FILE);
        PONGED = new FileLockSignaller(PONG_FILE);
        try
        {
            final RandomAccessFile randomAccessFile = new RandomAccessFile(TMPFS_FILE, "rw");
            FileChannel channel = randomAccessFile.getChannel();
            DATA = channel.map(READ_WRITE, 0, 10);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static final int NANOTIME_ESTIMATE_COUNT = 1000;

    private abstract static class Runner implements Runnable
    {

        public void runLoop()
        {
            try
            {
                for (int i = 1; i < RUN_COUNT + 1; i++)
                {
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

    private static final class Pinger extends Runner
    {
        private final long[] times = new long[RUN_COUNT + 1];

        public void run()
        {
            Thread.currentThread().setName("Ping Thread");
            runLoop();
            times[RUN_COUNT] = System.nanoTime();
            printTimes(times);
        }

        protected void runIteration(final int i) throws InterruptedException
        {
            times[i] = System.nanoTime();
            PINGED.start();
            try
            {
                DATA.putInt(PING_OFFSET, i);
            }
            finally
            {
                PINGED.signal();
            }

            while (DATA.getInt(PONG_OFFSET) != i)
            {
                PONGED.await();
            }
        }
    }

    private static final class Ponger extends Runner
    {
        public void run()
        {
            Thread.currentThread().setName("Pong Thread");
            runLoop();
        }

        protected void runIteration(final int i) throws InterruptedException
        {
            while (DATA.getInt(PING_OFFSET) != i)
            {
                PINGED.await();
            }

            PONGED.start();
            try
            {
                DATA.putInt(PONG_OFFSET, i);
            }
            finally
            {
                PONGED.signal();
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

    public static void main(String[] args) throws IOException, InterruptedException
    {
        boolean isPinger = args.length == 1 && Boolean.parseBoolean(args[0]);
        Runnable toRun = isPinger ? new Pinger() : new Ponger();
        toRun.run();
    }

}
