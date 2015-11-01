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

import uk.co.real_logic.agrona.concurrent.AtomicCounter;

/**
 * Tracks a aeron client interest registration in a {@link NetworkPublication}.
 */
public class PublicationLink implements DriverManagedResource
{
    private final long registrationId;
    private final long unblockTimeoutNs;
    private final DriverManagedResource publication;
    private final AeronClient client;
    private final AtomicCounter unblockedPublications;

    private long lastConsumerPosition;
    private long timeOfLastConsumerPositionChange;
    private boolean reachedEndOfLife = false;

    public PublicationLink(
        final long registrationId,
        final DriverManagedResource publication,
        final AeronClient client,
        final long now,
        final long unblockTimeoutNs,
        final SystemCounters systemCounters)
    {
        this.registrationId = registrationId;
        this.publication = publication;
        this.client = client;
        this.publication.incRef();
        this.lastConsumerPosition = publication.consumerPosition();
        this.timeOfLastConsumerPositionChange = now;
        this.unblockTimeoutNs = unblockTimeoutNs;
        this.unblockedPublications = systemCounters.unblockedPublications();
    }

    public void close()
    {
        publication.decRef();
    }

    public long registrationId()
    {
        return registrationId;
    }

    public void onTimeEvent(final long time, final DriverConductor conductor)
    {
        if (client.hasTimedOut(time))
        {
            reachedEndOfLife = true;
        }

        final long consumerPosition = publication.consumerPosition();
        if (consumerPosition == lastConsumerPosition)
        {
            if (publication.producerPosition() > consumerPosition &&
                time > (timeOfLastConsumerPositionChange + unblockTimeoutNs))
            {
                if (publication.unblockAtConsumerPosition())
                {
                    unblockedPublications.orderedIncrement();
                }
            }
        }
        else
        {
            timeOfLastConsumerPositionChange = time;
            lastConsumerPosition = consumerPosition;
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
