/*
 * Copyright 2014-2020 Real Logic Limited.
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

import java.util.concurrent.TimeUnit;

/**
 * Aeron client library tracker.
 */
public class AeronClient implements DriverManagedResource
{
    private final long clientId;
    private final long clientLivenessTimeoutMs;
    private final AtomicCounter clientTimeouts;
    private final AtomicCounter heartbeatTimestamp;
    private boolean reachedEndOfLife = false;
    private boolean closedByCommand = false;

    public AeronClient(
        final long clientId,
        final long clientLivenessTimeoutNs,
        final long nowMs,
        final AtomicCounter clientTimeouts,
        final AtomicCounter heartbeatTimestamp)
    {
        this.clientId = clientId;
        this.clientLivenessTimeoutMs = Math.max(1, TimeUnit.NANOSECONDS.toMillis(clientLivenessTimeoutNs));
        this.clientTimeouts = clientTimeouts;
        this.heartbeatTimestamp = heartbeatTimestamp;

        heartbeatTimestamp.setOrdered(nowMs);
    }

    public void close()
    {
        if (!heartbeatTimestamp.isClosed())
        {
            heartbeatTimestamp.close();
        }
    }

    public long clientId()
    {
        return clientId;
    }

    public void timeOfLastKeepaliveMs(final long nowMs)
    {
        heartbeatTimestamp.setOrdered(nowMs);
    }

    public boolean hasTimedOut()
    {
        return reachedEndOfLife;
    }

    public void onTimeEvent(final long timeNs, final long timeMs, final DriverConductor conductor)
    {
        if (timeMs > (heartbeatTimestamp.get() + clientLivenessTimeoutMs))
        {
            reachedEndOfLife = true;

            if (!closedByCommand)
            {
                clientTimeouts.incrementOrdered();
                conductor.clientTimeout(clientId);
            }
        }
    }

    public boolean hasReachedEndOfLife()
    {
        return reachedEndOfLife;
    }

    void onClosedByCommand()
    {
        closedByCommand = true;
        heartbeatTimestamp.set(0);
    }
}
