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
import io.aeron.driver.media.UdpChannel;
import org.agrona.concurrent.status.ReadablePosition;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Subscription registration from a client used for liveness tracking
 */
public class SubscriptionLink implements DriverManagedResource
{
    private final long registrationId;
    private final long clientLivenessTimeoutNs;
    private final int streamId;
    private final boolean isReliable;
    private final String channelUri;
    private final ReceiveChannelEndpoint channelEndpoint;
    private final AeronClient aeronClient;
    private final Map<PublicationImage, ReadablePosition> positionByImageMap = new IdentityHashMap<>();
    private final IpcPublication ipcPublication;
    private final ReadablePosition ipcPublicationSubscriberPosition;
    private final UdpChannel spiedChannel;

    private NetworkPublication spiedPublication = null;
    private ReadablePosition spiedPosition = null;

    private boolean reachedEndOfLife = false;

    public SubscriptionLink(
        final long registrationId,
        final ReceiveChannelEndpoint channelEndpoint,
        final int streamId,
        final String channelUri,
        final AeronClient aeronClient,
        final long clientLivenessTimeoutNs,
        final boolean isReliable)
    {
        this.registrationId = registrationId;
        this.channelEndpoint = channelEndpoint;
        this.streamId = streamId;
        this.channelUri = channelUri;
        this.aeronClient = aeronClient;
        this.ipcPublication = null;
        this.ipcPublicationSubscriberPosition = null;
        this.spiedChannel = null;
        this.clientLivenessTimeoutNs = clientLivenessTimeoutNs;
        this.isReliable = isReliable;
    }

    public SubscriptionLink(
        final long registrationId,
        final int streamId,
        final String channelUri,
        final IpcPublication ipcPublication,
        final ReadablePosition subscriberPosition,
        final AeronClient aeronClient,
        final long clientLivenessTimeoutNs)
    {
        this.registrationId = registrationId;
        this.channelEndpoint = null; // will prevent matches between PublicationImages and IpcPublications
        this.streamId = streamId;
        this.channelUri = channelUri;
        this.aeronClient = aeronClient;
        this.ipcPublication = ipcPublication;
        ipcPublication.incRef();
        this.ipcPublicationSubscriberPosition = subscriberPosition;
        this.spiedChannel = null;
        this.clientLivenessTimeoutNs = clientLivenessTimeoutNs;
        this.isReliable = true;
    }

    public SubscriptionLink(
        final long registrationId,
        final UdpChannel spiedChannel,
        final int streamId,
        final String channelUri,
        final AeronClient aeronClient,
        final long clientLivenessTimeoutNs)
    {
        this.registrationId = registrationId;
        this.channelEndpoint = null;
        this.streamId = streamId;
        this.channelUri = channelUri;
        this.aeronClient = aeronClient;
        this.ipcPublication = null;
        this.ipcPublicationSubscriberPosition = null;
        this.spiedChannel = spiedChannel;
        this.clientLivenessTimeoutNs = clientLivenessTimeoutNs;
        this.isReliable = true;
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

    public String channelUri()
    {
        return channelUri;
    }

    public boolean isReliable()
    {
        return isReliable;
    }

    public boolean matches(final ReceiveChannelEndpoint channelEndpoint, final int streamId)
    {
        return channelEndpoint == this.channelEndpoint && streamId == this.streamId;
    }

    public boolean matches(final NetworkPublication publication)
    {
        boolean result = false;

        if (null != spiedChannel)
        {
            result = streamId == publication.streamId() &&
                publication.sendChannelEndpoint().udpChannel().canonicalForm().equals(spiedChannel.canonicalForm());
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

    public void removeSpiedPublication()
    {
        spiedPublication = null;
        spiedPosition = null;
    }

    public void close()
    {
        positionByImageMap.forEach(PublicationImage::removeSubscriber);

        if (null != ipcPublication)
        {
            ipcPublication.removeSubscription(ipcPublicationSubscriberPosition);
            ipcPublication.decRef();
        }
        else if (null != spiedPublication)
        {
            spiedPublication.removeSpyPosition(spiedPosition);
        }
    }

    public void onTimeEvent(final long time, final DriverConductor conductor)
    {
        if (time > (aeronClient.timeOfLastKeepalive() + clientLivenessTimeoutNs))
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
