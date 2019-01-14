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

import java.util.concurrent.TimeUnit;

/**
 * Aeron client library tracker.
 */
public class AeronClient implements DriverManagedResource
{
    private final long clientId;
    private final long clientLivenessTimeoutMs;
    private final AtomicCounter clientTimeouts;
    private final AtomicCounter heartbeatStatus;
    private long timeOfLastKeepaliveMs;
    private boolean reachedEndOfLife = false;

    public AeronClient(
        final long clientId,
        final long clientLivenessTimeoutNs,
        final long nowMs,
        final AtomicCounter clientTimeouts,
        final AtomicCounter heartbeatStatus)
    {
        this.clientId = clientId;
        this.clientLivenessTimeoutMs = Math.max(1, TimeUnit.NANOSECONDS.toMillis(clientLivenessTimeoutNs));
        this.timeOfLastKeepaliveMs = nowMs;
        this.clientTimeouts = clientTimeouts;
        this.heartbeatStatus = heartbeatStatus;

        heartbeatStatus.setOrdered(nowMs);
    }

    public void close()
    {
        if (!heartbeatStatus.isClosed())
        {
            heartbeatStatus.close();
        }
    }

    public long clientId()
    {
        return clientId;
    }

    public void timeOfLastKeepaliveMs(final long nowMs)
    {
        timeOfLastKeepaliveMs = nowMs;
        heartbeatStatus.setOrdered(nowMs);
    }

    public boolean hasTimedOut()
    {
        return reachedEndOfLife;
    }

    public void onTimeEvent(final long timeNs, final long timeMs, final DriverConductor conductor)
    {
        if (timeMs > (timeOfLastKeepaliveMs + clientLivenessTimeoutMs))
        {
            reachedEndOfLife = true;
            clientTimeouts.incrementOrdered();
            conductor.clientTimeout(clientId);
        }
    }

    public boolean hasReachedEndOfLife()
    {
        return reachedEndOfLife;
    }
}
