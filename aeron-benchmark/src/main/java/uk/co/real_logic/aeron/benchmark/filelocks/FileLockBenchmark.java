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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Condition;

public class FileLockBenchmark
{
    private static final long MUTEX_OFFSET = 0;
    private static final long PING_LOCK_OFFSET = 2;
    private static final long PONG_LOCK_OFFSET = 4;

    public static void main(String[] args) throws IOException, InterruptedException
    {
        boolean isPinger = args.length == 1 && Boolean.parseBoolean(args[0]);

        final File file = new File(PingPongBenchmark.TMPFS_FILE);
        final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        FileChannel channel = randomAccessFile.getChannel();

        final FileLockBasedLock lock = new FileLockBasedLock(channel, MUTEX_OFFSET);
        final Condition pinged = new FileLockCondition(lock, channel, PING_LOCK_OFFSET);
        final Condition ponged = new FileLockCondition(lock, channel, PONG_LOCK_OFFSET);

        final PingPongBenchmark benchmark = new PingPongBenchmark(lock, pinged, ponged, randomAccessFile);
        if (isPinger)
        {
            benchmark.runAsPinger();
        }
        else
        {
            benchmark.runAsPonger();
        }
    }

}
