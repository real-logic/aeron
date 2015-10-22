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

/**
 * Tracks a aeron client interest registration in a {@link NetworkPublication}.
 */
public class PublicationLink implements DriverManagedResource
{
    private final long registrationId;
    private final long unblockTimeoutNs;
    private final DriverManagedResource publication;
    private final AeronClient client;
    private final SystemCounters systemCounters;

    private boolean reachedEndOfLife = false;
    private long previousConsumerPosition;
    private long previousProducerPosition;
    private long timeOfLastConsumerPositionChange;

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
        this.previousConsumerPosition = publication.consumerPosition();
        this.previousProducerPosition = publication.producerPosition();
        this.timeOfLastConsumerPositionChange = now;
        this.unblockTimeoutNs = unblockTimeoutNs;
        this.systemCounters = systemCounters;
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
        final long producerPosition = publication.producerPosition();

        if (consumerPosition == previousConsumerPosition &&
            producerPosition == previousProducerPosition &&
            publication.producerPosition() > consumerPosition)
        {
            if (time > (timeOfLastConsumerPositionChange + unblockTimeoutNs))
            {
                if (publication.unblockAtConsumerPosition())
                {
                    systemCounters.unblocks().orderedIncrement();
                }
            }
        }
        else
        {
            // want movement of either consumer or producer to reset time?
            timeOfLastConsumerPositionChange = time;
        }

        previousConsumerPosition = consumerPosition;
        previousProducerPosition = producerPosition;
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
