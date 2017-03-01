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
package io.aeron.driver;

/**
 * Tracks a aeron client interest registration in a {@link NetworkPublication} or {@link IpcPublication}.
 */
public class PublicationLink implements DriverManagedResource
{
    private final long registrationId;
    private final DriverManagedResource publication;
    private final AeronClient client;
    private boolean reachedEndOfLife = false;

    public PublicationLink(final long registrationId, final DriverManagedResource publication, final AeronClient client)
    {
        this.registrationId = registrationId;
        this.publication = publication;
        this.client = client;
        this.publication.incRef();
    }

    public void close()
    {
        publication.decRef();
    }

    public long registrationId()
    {
        return registrationId;
    }

    public void onTimeEvent(final long timeNs, final DriverConductor conductor)
    {
        if (client.hasTimedOut(timeNs))
        {
            reachedEndOfLife = true;
        }
    }

    public boolean hasReachedEndOfLife()
    {
        return reachedEndOfLife;
    }

    public void timeOfLastStateChange(final long time)
    {
        // not set this way
    }

    public long timeOfLastStateChange()
    {
        return client.timeOfLastKeepalive();
    }

    public void delete()
    {
        close();
    }
}
