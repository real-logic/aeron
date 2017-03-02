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
    protected final String uri;
    protected final AeronClient aeronClient;
    protected final Map<Subscribable, ReadablePosition> positionBySubscribableMap = new IdentityHashMap<>();

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
        this.uri = channelUri;
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

    public String uri()
    {
        return uri;
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

    public boolean matches(final int streamId)
    {
        return false;
    }

    public boolean isLinked(final Subscribable subscribable)
    {
        return positionBySubscribableMap.containsKey(subscribable);
    }

    public void link(final Subscribable subscribable, final ReadablePosition position)
    {
        positionBySubscribableMap.put(subscribable, position);
    }

    public void unlink(final Subscribable subscribable)
    {
        positionBySubscribableMap.remove(subscribable);
    }

    public void close()
    {
        positionBySubscribableMap.forEach(Subscribable::removeSubscriber);
    }

    public void onTimeEvent(final long timeNs, final DriverConductor conductor)
    {
        if (timeNs > (aeronClient.timeOfLastKeepalive() + clientLivenessTimeoutNs))
        {
            reachedEndOfLife = true;
            conductor.cleanupSubscriptionLink(this);
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
}

class IpcSubscriptionLink extends SubscriptionLink
{
    IpcSubscriptionLink(
        final long registrationId,
        final int streamId,
        final String channelUri,
        final AeronClient aeronClient,
        final long clientLivenessTimeoutNs)
    {
        super(registrationId, streamId, channelUri, aeronClient, clientLivenessTimeoutNs);
    }

    public boolean matches(final int streamId)
    {
        return streamId() == streamId;
    }
}

class SpySubscriptionLink extends SubscriptionLink
{
    private final UdpChannel udpChannel;

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

    public boolean matches(final NetworkPublication publication)
    {
        return streamId == publication.streamId() &&
            udpChannel.canonicalForm().equals(publication.sendChannelEndpoint().udpChannel().canonicalForm());
    }
}