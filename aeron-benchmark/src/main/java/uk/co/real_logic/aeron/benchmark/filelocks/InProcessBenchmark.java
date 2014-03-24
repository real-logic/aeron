package uk.co.real_logic.aeron.benchmark.filelocks;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class InProcessBenchmark
{

    public static void main(String[] args) throws IOException
    {
        /*boolean goFirst = Boolean.parseBoolean(args[0]);
        */

        final Lock lock = new ReentrantLock();
        final Condition pinged = lock.newCondition();
        final Condition ponged = lock.newCondition();
        FileLockBenchmark pinger = new FileLockBenchmark(lock, pinged, ponged);
        FileLockBenchmark ponger = new FileLockBenchmark(lock, pinged, ponged);
        new Thread(pinger::runAsPinger).start();
        ponger.runAsPonger();
        System.out.println(new File(".").getAbsolutePath());
    }

}
