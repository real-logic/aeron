package uk.co.real_logic.aeron.benchmark.filelocks;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class FileLockBenchmark
{
    private static final int RUN_COUNT = 1_000_000;

    private static final int PING = 1;
    private static final int PONG = 2;

    private static final long PING_LOCK_OFFSET = 0;
    private static final long PONG_LOCK_OFFSET = 1;
    private static final long BUFFER_START = 2;
    private static final int WARM_UP_RUNS = 10_000;

    private static String NON_TMPFS_FILE = "/tmp/filelock_benchmark";
    private static String TMPFS_FILE = "/dev/shm/filelock_benchmark";

    private final Lock lock;
    private final Condition pinged;
    private final Condition ponged;
    private final RandomAccessFile randomAccessFile;
    private final FileChannel channel;

    public FileLockBenchmark(Lock lock, Condition pinged, Condition ponged) throws IOException
    {
        this.lock = lock;
        this.pinged = pinged;
        this.ponged = ponged;
        File file = new File(TMPFS_FILE);
        randomAccessFile = new RandomAccessFile(file, "rw");
        channel = randomAccessFile.getChannel();
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
                channel.close();
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
        pinged.signalAll();

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
        ponged.signalAll();

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
