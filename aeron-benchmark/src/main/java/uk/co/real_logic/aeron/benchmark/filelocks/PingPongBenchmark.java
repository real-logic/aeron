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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class PingPongBenchmark
{
    // private static final int RUN_COUNT = 1_000_000;
    private static final int RUN_COUNT = 500;

    private static final int PING = 1;
    private static final int PONG = 2;

    private static final int WARM_UP_RUNS = 10_000;

    public static final String NON_TMPFS_FILE = "/tmp/filelock_benchmark";
    public static final String TMPFS_FILE = "/dev/shm/filelock_benchmark";

    private final Lock lock;
    private final Condition pinged;
    private final Condition ponged;
    private final RandomAccessFile randomAccessFile;

    public PingPongBenchmark(final Lock lock, final Condition pinged, final Condition ponged, final RandomAccessFile randomAccessFile) throws IOException
    {
        this.lock = lock;
        this.pinged = pinged;
        this.ponged = ponged;
        this.randomAccessFile = randomAccessFile;
    }

    public void runAsPinger()
    {
        Thread.currentThread().setName("pinger");
        final long[] times = new long[RUN_COUNT + 1];
        withRuns(i ->
        {
            times[i] = System.nanoTime();
            ping(i);
        });
        times[RUN_COUNT] = System.nanoTime();
        printTimes(times);
    }

    public void runAsPonger()
    {
        Thread.currentThread().setName("ponger");
        startOddToAvoidPinger();
        withRuns(this::pong);
    }

    private void startOddToAvoidPinger()
    {
        try
        {
            randomAccessFile.seek(1);
            lock.lock();
            pinged.await();
            lock.unlock();
        } catch (IOException | InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    private void withRuns(Block block)
    {
        try
        {
            try
            {
                for (int i = 0; i < RUN_COUNT; i++)
                {
                    lock.lock();
                    try
                    {
                        block.run(i);
                    } finally
                    {
                        lock.unlock();
                    }
                }
            } finally
            {
                randomAccessFile.close();
            }
        } catch (Throwable e)
        {
            e.printStackTrace();
        }
    }

    interface Block
    {
        void run(final int i) throws IOException, InterruptedException;
    }

    public void ping(final int i) throws IOException, InterruptedException
    {
        randomAccessFile.write(PING);
        pinged.signal();

        if (i < RUN_COUNT - 1)
        {
            ponged.await();
            int pong = randomAccessFile.read();
            checkEqual(pong, PONG, randomAccessFile.getFilePointer());
        }
    }

    public void pong(final int i) throws IOException, InterruptedException
    {
        randomAccessFile.write(PONG);
        ponged.signal();

        if (i < RUN_COUNT - 1)
        {
            pinged.await();
            int ping = randomAccessFile.read();
            checkEqual(ping, PING, randomAccessFile.getFilePointer());
        }
    }

    private static void printTimes(final long[] times)
    {
        try (PrintStream out = new PrintStream(new FileOutputStream("timings.log")))
        {
            long startTime = times[0];
            for (int i = 1; i < RUN_COUNT + 1; i++)
            {
                if (i > WARM_UP_RUNS)
                    out.println(times[i] - startTime);
                startTime = times[i];
            }
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
    }

    private static void checkEqual(final int left, final int right, final long filePointer)
    {
        if (left != right)
        {
            throw new IllegalStateException("values not equal: " + left + ", " + right + " @ " + filePointer);
        }
    }

}
