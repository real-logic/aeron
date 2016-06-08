/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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

import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import org.agrona.concurrent.status.ReadablePosition;

import java.util.IdentityHashMap;
import java.util.Map;

import static io.aeron.driver.Configuration.CLIENT_LIVENESS_TIMEOUT_NS;

/**
 * Subscription registration from a client used for liveness tracking
 */
public class SubscriptionLink implements DriverManagedResource
{
    private final long registrationId;
    private final int streamId;
    private final ReceiveChannelEndpoint channelEndpoint;
    private final AeronClient aeronClient;
    private final Map<PublicationImage, ReadablePosition> positionByImageMap = new IdentityHashMap<>();
    private final DirectPublication directPublication;
    private final ReadablePosition directPublicationSubscriberPosition;
    private final UdpChannel spiedChannel;

    private NetworkPublication spiedPublication = null;
    private ReadablePosition spiedPosition = null;

    private boolean reachedEndOfLife = false;

    public SubscriptionLink(
        final long registrationId,
        final ReceiveChannelEndpoint channelEndpoint,
        final int streamId,
        final AeronClient aeronClient)
    {
        this.registrationId = registrationId;
        this.channelEndpoint = channelEndpoint;
        this.streamId = streamId;
        this.aeronClient = aeronClient;
        this.directPublication = null;
        this.directPublicationSubscriberPosition = null;
        this.spiedChannel = null;
    }

    public SubscriptionLink(
        final long registrationId,
        final int streamId,
        final DirectPublication directPublication,
        final ReadablePosition subscriberPosition,
        final AeronClient aeronClient)
    {
        this.registrationId = registrationId;
        this.channelEndpoint = null; // will prevent matches between PublicationImages and DirectPublications
        this.streamId = streamId;
        this.aeronClient = aeronClient;
        this.directPublication = directPublication;
        directPublication.incRef();
        this.directPublicationSubscriberPosition = subscriberPosition;
        this.spiedChannel = null;
    }

    public SubscriptionLink(
        final long registrationId,
        final UdpChannel spiedChannel,
        final int streamId,
        final AeronClient aeronClient)
    {
        this.registrationId = registrationId;
        this.channelEndpoint = null;
        this.streamId = streamId;
        this.aeronClient = aeronClient;
        this.directPublication = null;
        this.directPublicationSubscriberPosition = null;
        this.spiedChannel = spiedChannel;
    }

    public long registrationId()
    {
        return registrationId;
    }

    public ReceiveChannelEndpoint channelEndpoint()
    {
        return channelEndpoint;
    }

    public int streamId()
    {
        return streamId;
    }

    public UdpChannel spiedChannel()
    {
        return spiedChannel;
    }

    public boolean matches(final ReceiveChannelEndpoint channelEndpoint, final int streamId)
    {
        return channelEndpoint == this.channelEndpoint && streamId == this.streamId;
    }

    public boolean matches(final SendChannelEndpoint channelEndpoint, final int streamId)
    {
        boolean result = false;

        if (null != spiedChannel)
        {
            result =
                channelEndpoint.udpChannel().canonicalForm().equals(spiedChannel.canonicalForm()) &&
                    streamId == this.streamId();
        }

        return result;
    }

    public void addImage(final PublicationImage image, final ReadablePosition position)
    {
        positionByImageMap.put(image, position);
    }

    public void removeImage(final PublicationImage image)
    {
        positionByImageMap.remove(image);
    }

    public void addSpiedPublication(final NetworkPublication publication, final ReadablePosition position)
    {
        spiedPublication = publication;
        spiedPosition = position;
    }

    public void removeSpiedPublication(final NetworkPublication publication)
    {
        spiedPublication = null;
        spiedPosition = null;
    }

    public void close()
    {
        positionByImageMap.forEach(PublicationImage::removeSubscriber);

        if (null != directPublication)
        {
            directPublication.removeSubscription(directPublicationSubscriberPosition);
            directPublication.decRef();
        }
        else if (null != spiedPublication)
        {
            spiedPublication.removeSpyPosition(spiedPosition);
        }
    }

    public void onTimeEvent(final long time, final DriverConductor conductor)
    {
        if (time > (aeronClient.timeOfLastKeepalive() + CLIENT_LIVENESS_TIMEOUT_NS))
        {
            reachedEndOfLife = true;
            conductor.cleanupSubscriptionLink(SubscriptionLink.this);
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
        return aeronClient.timeOfLastKeepalive();
    }

    public void delete()
    {
        close();
    }
}
