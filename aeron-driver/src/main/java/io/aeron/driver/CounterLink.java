/*
 * Copyright 2014-2024 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
 * Tracks an aeron client interest in a counter.
 */
final class CounterLink implements DriverManagedResource
{
    private final long registrationId;
    private final AtomicCounter counter;
    private final AeronClient client;
    private boolean reachedEndOfLife = false;

    CounterLink(final AtomicCounter counter, final long registrationId, final AeronClient client)
    {
        this.registrationId = registrationId;
        this.counter = counter;
        this.client = client;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        counter.close();
    }

    /**
     * {@inheritDoc}
     */
    public void onTimeEvent(final long timeNs, final long timeMs, final DriverConductor conductor)
    {
        if (null != client && client.hasTimedOut())
        {
            reachedEndOfLife = true;
            conductor.unavailableCounter(registrationId, counterId());
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean hasReachedEndOfLife()
    {
        return reachedEndOfLife;
    }

    int counterId()
    {
        return counter.id();
    }

    long registrationId()
    {
        return registrationId;
    }
}
