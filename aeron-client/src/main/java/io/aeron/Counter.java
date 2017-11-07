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

import org.agrona.concurrent.status.Position;

public class Counter
{
    protected final long registrationId;
    protected final ClientConductor clientConductor;
    protected final Position position;
    protected volatile boolean isClosed = false;

    public Counter(final ClientConductor clientConductor, final long registrationId, final Position position)
    {
        this.clientConductor = clientConductor;
        this.registrationId = registrationId;
        this.position = position;
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
     * Return the Position providing access to the underlying counter.
     *
     * @return atomic counter
     */
    public Position position()
    {
        return position;
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
