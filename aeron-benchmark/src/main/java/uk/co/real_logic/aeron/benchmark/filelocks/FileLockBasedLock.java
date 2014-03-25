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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class FileLockBasedLock implements Lock
{

    private final FileChannel channel;
    private final long position;
    private FileLock lock;

    public FileLockBasedLock(final FileChannel channel, final long position)
    {
        this.channel = channel;
        this.position = position;
    }

    @Override
    public void lock()
    {
        try
        {
            lock = channel.lock(position, 1, true);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void unlock()
    {
        try
        {
            lock.release();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public boolean tryLock()
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public boolean tryLock(final long time, final TimeUnit unit) throws InterruptedException
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public Condition newCondition()
    {
        throw new UnsupportedOperationException("unimplemented");
    }
}
