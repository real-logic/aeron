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
package io.aeron;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.status.AtomicCounter;

public class Counter extends AtomicCounter
{
    protected final long registrationId;
    protected final ClientConductor clientConductor;
    protected volatile boolean isClosed = false;

    public Counter(final ClientConductor clientConductor,
                   final AtomicBuffer buffer,
                   final long registrationId,
                   final int counterId)
    {
        super(buffer, counterId);

        this.clientConductor = clientConductor;
        this.registrationId = registrationId;
    }

    /**
     * Return the registration id used to register this Counter with the media driver.
     *
     * @return registration id
     */
    public long registrationId()
    {
        return registrationId;
    }

    /**
     * Close the Counter.
     * <p>
     * This method is idempotent.
     */
    public void close()
    {
        clientConductor.clientLock().lock();
        try
        {
            if (!isClosed)
            {
                isClosed = true;

                clientConductor.releaseCounter(this);
            }
        }
        finally
        {
            clientConductor.clientLock().unlock();
        }
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
     * Forcibly close the Counter and release resources
     */
    void forceClose()
    {
        if (!isClosed)
        {
            isClosed = true;
            clientConductor.asyncReleaseCounter(this);
        }
    }
}
