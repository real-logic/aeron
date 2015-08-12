/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver;

import static uk.co.real_logic.aeron.driver.Configuration.CLIENT_LIVENESS_TIMEOUT_NS;

/**
 * Aeron client library tracker.
 */
public class AeronClient implements DriverManagedResourceProvider
{
    private final long clientId;
    private long timeOfLastKeepalive;
    private DriverManagedResource driverManagedResource;

    public AeronClient(final long clientId, final long now)
    {
        this.clientId = clientId;
        this.timeOfLastKeepalive = now;
        this.driverManagedResource = new AeronClientManagedResource();
    }

    public long clientId()
    {
        return clientId;
    }

    public long timeOfLastKeepalive()
    {
        return timeOfLastKeepalive;
    }

    public void timeOfLastKeepalive(final long now)
    {
        timeOfLastKeepalive = now;
    }

    public boolean hasTimedOut(final long now)
    {
        return now > (timeOfLastKeepalive + CLIENT_LIVENESS_TIMEOUT_NS);
    }

    public DriverManagedResource managedResource()
    {
        return driverManagedResource;
    }

    private class AeronClientManagedResource implements DriverManagedResource
    {
        private boolean reachedEndOfLife = false;

        public void onTimeEvent(long time, DriverConductor conductor)
        {
            if (time > (timeOfLastKeepalive + CLIENT_LIVENESS_TIMEOUT_NS))
            {
                reachedEndOfLife = true;
            }
        }

        public boolean hasReachedEndOfLife()
        {
            return reachedEndOfLife;
        }

        public void timeOfLastStateChange(long time)
        {
            timeOfLastKeepalive = time;
        }

        public long timeOfLastStateChange()
        {
            return timeOfLastKeepalive;
        }

        public void delete()
        {
            // nothing to do
        }
    }
}
