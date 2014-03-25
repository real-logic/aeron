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
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

public class FileLockCondition implements Condition
{

    private final FileLockBasedLock mutex;
    private final FileLockBasedLock semaphore;

    public FileLockCondition(final FileLockBasedLock mutex, final String path) throws FileNotFoundException
    {
        this.mutex = mutex;
        semaphore = new FileLockBasedLock(path);
    }

    @Override
    public void await() throws InterruptedException
    {
        mutex.unlock();

        semaphore.lock();

        mutex.lock();
    }

    @Override
    public void signal()
    {
        semaphore.unlock();
    }

    @Override
    public void awaitUninterruptibly()
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public long awaitNanos(final long nanosTimeout) throws InterruptedException
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public boolean await(final long time, final TimeUnit unit) throws InterruptedException
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public boolean awaitUntil(final Date deadline) throws InterruptedException
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public void signalAll()
    {
        throw new UnsupportedOperationException("unimplemented");
    }
}
