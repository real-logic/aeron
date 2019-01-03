/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.driver;

import org.agrona.concurrent.status.AtomicCounter;

/**
 * Tracks a aeron client interest in a counter.
 */
public class CounterLink implements DriverManagedResource
{
    private final long registrationId;
    private final AtomicCounter counter;
    private final AeronClient client;
    private boolean reachedEndOfLife = false;

    public CounterLink(final AtomicCounter counter, final long registrationId, final AeronClient client)
    {
        this.registrationId = registrationId;
        this.counter = counter;
        this.client = client;
    }

    public void close()
    {
        counter.close();
    }

    public long registrationId()
    {
        return registrationId;
    }

    public int counterId()
    {
        return counter.id();
    }

    public void onTimeEvent(final long timeNs, final long timeMs, final DriverConductor conductor)
    {
        if (client.hasTimedOut())
        {
            reachedEndOfLife = true;
        }
    }

    public boolean hasReachedEndOfLife()
    {
        return reachedEndOfLife;
    }
}
