/*
 * Copyright 2014-2017 Real Logic Ltd.
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
package io.aeron.status;

import org.agrona.UnsafeAccess;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.status.CountersReader;

import static org.agrona.BitUtil.SIZE_OF_LONG;

public class ReadableCounter implements AutoCloseable
{
    private final CountersReader countersReader;
    private final byte[] buffer;
    private final int counterId;
    private final long addressOffset;
    private volatile boolean isClosed = false;

    public ReadableCounter(final CountersReader countersReader, final int counterId)
    {
        this.countersReader = countersReader;
        this.counterId = counterId;

        final AtomicBuffer valuesBuffer = countersReader.valuesBuffer();

        final int counterOffset = CountersReader.counterOffset(counterId);
        valuesBuffer.boundsCheck(counterOffset, SIZE_OF_LONG);
        this.buffer = valuesBuffer.byteArray();
        this.addressOffset = valuesBuffer.addressOffset() + counterOffset;
    }

    /**
     * Return the counter Id.
     *
     * @return counter Id.
     */
    public int counterId()
    {
        return counterId;
    }

    /**
     * Return the state of the counter.
     *
     * @see CountersReader#RECORD_ALLOCATED
     * @see CountersReader#RECORD_LINGERING
     * @see CountersReader#RECORD_RECLAIMED
     * @see CountersReader#RECORD_UNUSED
     *
     * @return state for the counter.
     */
    public int state()
    {
        return countersReader.getCounterState(counterId);
    }

    /**
     * Return the counter label.
     *
     * @return the counter label.
     */
    public String label()
    {
        return countersReader.getCounterLabel(counterId);
    }

    /**
     * Get the latest value for the counter with volatile semantics.
     *
     * @return the latest value for the counter.
     */
    public long get()
    {
        return UnsafeAccess.UNSAFE.getLongVolatile(buffer, addressOffset);
    }

    /**
     * Get the value of the counter using weak ordering semantics. This is the same a standard read of a field.
     *
     * @return the  value for the counter.
     */
    public long getWeak()
    {
        return UnsafeAccess.UNSAFE.getLong(buffer, addressOffset);
    }

    /**
     * Close the counter.
     */
    public void close()
    {
        isClosed = true;
    }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    public boolean isClosed()
    {
        return isClosed;
    }

    /**
     * Forcibly close the counter.
     */
    void forceClose()
    {
        isClosed = true;
    }
}
