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
public abstract class SubscriptionLink implements DriverManagedResource
{
    protected final long registrationId;
    protected final long clientLivenessTimeoutNs;
    protected final int streamId;
    protected final String channelUri;
    protected final AeronClient aeronClient;

    protected boolean reachedEndOfLife = false;

    protected SubscriptionLink(
        final long registrationId,
        final int streamId,
        final String channelUri,
        final AeronClient aeronClient,
        final long clientLivenessTimeoutNs)
    {
        this.registrationId = registrationId;
        this.streamId = streamId;
        this.channelUri = channelUri;
        this.aeronClient = aeronClient;
        this.clientLivenessTimeoutNs = clientLivenessTimeoutNs;
    }

    public long registrationId()
    {
        return registrationId;
    }

    public int streamId()
    {
        return streamId;
    }

    public String channelUri()
    {
        return channelUri;
    }

    public ReceiveChannelEndpoint channelEndpoint()
    {
        return null;
    }

    public boolean isReliable()
    {
        return true;
    }

    public boolean matches(final NetworkPublication publication)
    {
        return false;
    }
    public boolean matches(final ReceiveChannelEndpoint channelEndpoint, final int streamId)
    {
        return false;
    }

    public abstract void addSource(Object source, ReadablePosition position);

    public abstract void removeSource(Object source);

    public abstract void close();

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

class NetworkSubscriptionLink extends SubscriptionLink
{
    private final boolean isReliable;
    private final ReceiveChannelEndpoint channelEndpoint;
    private final Map<PublicationImage, ReadablePosition> positionByImageMap = new IdentityHashMap<>();

    NetworkSubscriptionLink(
        final long registrationId,
        final ReceiveChannelEndpoint channelEndpoint,
        final int streamId,
        final String channelUri,
        final AeronClient aeronClient,
        final long clientLivenessTimeoutNs,
        final boolean isReliable)
    {
        super(registrationId, streamId, channelUri, aeronClient, clientLivenessTimeoutNs);

        this.isReliable = isReliable;
        this.channelEndpoint = channelEndpoint;
    }

    public boolean isReliable()
    {
        return isReliable;
    }

    public ReceiveChannelEndpoint channelEndpoint()
    {
        return channelEndpoint;
    }

    public boolean matches(final ReceiveChannelEndpoint channelEndpoint, final int streamId)
    {
        return channelEndpoint == this.channelEndpoint && streamId == this.streamId;
    }

    public void addSource(final Object source, final ReadablePosition position)
    {
        positionByImageMap.put((PublicationImage)source, position);
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    public void removeSource(final Object source)
    {
        positionByImageMap.remove(source);
    }

    public void close()
    {
        positionByImageMap.forEach(PublicationImage::removeSubscriber);
    }
}

class IpcSubscriptionLink extends SubscriptionLink
{
    private IpcPublication publication;
    private ReadablePosition position;

    IpcSubscriptionLink(
        final long registrationId,
        final int streamId,
        final String channelUri,
        final AeronClient aeronClient,
        final long clientLivenessTimeoutNs)
    {
        super(registrationId, streamId, channelUri, aeronClient, clientLivenessTimeoutNs);
    }

    public void addSource(final Object source, final ReadablePosition position)
    {
        this.publication = (IpcPublication)source;
        this.position = position;

        publication.incRef();
    }

    public void removeSource(final Object source)
    {
        publication = null;
        position = null;
    }

    public void close()
    {
        if (null != publication)
        {
            publication.removeSubscription(position);
            publication.decRef();
        }
    }
}

class SpySubscriptionLink extends SubscriptionLink
{
    private final UdpChannel udpChannel;
    private NetworkPublication publication = null;
    private ReadablePosition position = null;

    SpySubscriptionLink(
        final long registrationId,
        final UdpChannel spiedChannel,
        final int streamId,
        final AeronClient aeronClient,
        final long clientLivenessTimeoutNs)
    {
        super(registrationId, streamId, spiedChannel.originalUriString(), aeronClient, clientLivenessTimeoutNs);
        this.udpChannel = spiedChannel;
    }

    public void addSource(final Object source, final ReadablePosition position)
    {
        this.publication = (NetworkPublication)source;
        this.position = position;
    }

    public void removeSource(final Object source)
    {
        publication = null;
        position = null;
    }

    public boolean matches(final NetworkPublication publication)
    {
        return streamId == publication.streamId() &&
            publication.sendChannelEndpoint().udpChannel().canonicalForm().equals(udpChannel.canonicalForm());
    }

    public void close()
    {
        if (null != publication)
        {
            publication.removeSpyPosition(position);
        }
    }
}