package io.aeron.driver;


import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

import java.nio.charset.Charset;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * A thread safe extension of the {@link CountersManager} to allow same process read and write access to the same
 * counter buffer. Note that cross process access is not catered for.
 *
 * TODO: move to Agrona when settled.
 */
public class ConcurrentCountersManager extends CountersManager
{
    private final ReentrantLock lock = new ReentrantLock();

    public ConcurrentCountersManager(final AtomicBuffer metaDataBuffer, final AtomicBuffer valuesBuffer)
    {
        super(metaDataBuffer, valuesBuffer);
    }

    public ConcurrentCountersManager(
        final AtomicBuffer metaDataBuffer,
        final AtomicBuffer valuesBuffer,
        final Charset labelCharset)
    {
        super(metaDataBuffer, valuesBuffer, labelCharset);
    }

    public int allocate(final String label)
    {
        lock.lock();
        try
        {
            return super.allocate(label);
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public int allocate(final String label, final int typeId, final Consumer<MutableDirectBuffer> keyFunc)
    {
        lock.lock();
        try
        {
            return super.allocate(label, typeId, keyFunc);
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public AtomicCounter newCounter(final String label)
    {
        lock.lock();
        try
        {
            return super.newCounter(label);
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public AtomicCounter newCounter(final String label, final int typeId, final Consumer<MutableDirectBuffer> keyFunc)
    {
        lock.lock();
        try
        {
            return super.newCounter(label, typeId, keyFunc);
        }
        finally
        {
            lock.unlock();
        }
    }

    public void free(final int counterId)
    {
        lock.lock();
        try
        {
            super.free(counterId);
        }
        finally
        {
            lock.unlock();
        }
    }
}
