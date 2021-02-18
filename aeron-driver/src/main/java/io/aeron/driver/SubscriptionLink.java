/*
 * Copyright 2014-2021 Real Logic Limited.
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

import io.aeron.CommonContext;
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
    final long registrationId;
    final int streamId;
    final int sessionId;
    final boolean hasSessionId;
    final boolean isSparse;
    final boolean isTether;
    boolean reachedEndOfLife = false;
    final CommonContext.InferableBoolean group;
    final String channel;
    final AeronClient aeronClient;
    final IdentityHashMap<Subscribable, ReadablePosition> positionBySubscribableMap;

    SubscriptionLink(
        final long registrationId,
        final int streamId,
        final String channel,
        final AeronClient aeronClient,
        final SubscriptionParams params)
    {
        this.registrationId = registrationId;
        this.streamId = streamId;
        this.channel = channel;
        this.aeronClient = aeronClient;
        this.hasSessionId = params.hasSessionId;
        this.sessionId = params.sessionId;
        this.isSparse = params.isSparse;
        this.isTether = params.isTether;
        this.group = params.group;

        positionBySubscribableMap = new IdentityHashMap<>(hasSessionId ? 1 : 8);
    }

    /**
     * Channel URI the subscription is on.
     *
     * @return channel URI the subscription is on.
     */
    public final String channel()
    {
        return channel;
    }

    /**
     * Stream id the subscription is on.
     *
     * @return stream id the subscription is on.
     */
    public final int streamId()
    {
        return streamId;
    }

    /**
     * Registration id of the subscription.
     *
     * @return registration id of the subscription.
     */
    public final long registrationId()
    {
        return registrationId;
    }

    AeronClient aeronClient()
    {
        return aeronClient;
    }

    final int sessionId()
    {
        return sessionId;
    }

    ReceiveChannelEndpoint channelEndpoint()
    {
        return null;
    }

    boolean isReliable()
    {
        return true;
    }

    boolean isRejoin()
    {
        return true;
    }

    boolean isTether()
    {
        return isTether;
    }

    boolean isSparse()
    {
        return isSparse;
    }

    CommonContext.InferableBoolean group()
    {
        return group;
    }

    boolean hasSessionId()
    {
        return hasSessionId;
    }

    boolean matches(final NetworkPublication publication)
    {
        return false;
    }

    boolean matches(final PublicationImage image)
    {
        return false;
    }

    boolean matches(final IpcPublication publication)
    {
        return false;
    }

    boolean matches(final ReceiveChannelEndpoint channelEndpoint, final int streamId, final SubscriptionParams params)
    {
        return false;
    }

    boolean matches(final ReceiveChannelEndpoint channelEndpoint, final int streamId, final int sessionId)
    {
        return false;
    }

    boolean isLinked(final Subscribable subscribable)
    {
        return positionBySubscribableMap.containsKey(subscribable);
    }

    void link(final Subscribable subscribable, final ReadablePosition position)
    {
        positionBySubscribableMap.put(subscribable, position);
    }

    void unlink(final Subscribable subscribable)
    {
        positionBySubscribableMap.remove(subscribable);
    }

    boolean isWildcardOrSessionIdMatch(final int sessionId)
    {
        return !hasSessionId || this.sessionId == sessionId;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        for (final Map.Entry<Subscribable, ReadablePosition> entry : positionBySubscribableMap.entrySet())
        {
            final Subscribable subscribable = entry.getKey();
            final ReadablePosition position = entry.getValue();
            subscribable.removeSubscriber(this, position);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void onTimeEvent(final long timeNs, final long timeMs, final DriverConductor conductor)
    {
        if (aeronClient.hasTimedOut())
        {
            reachedEndOfLife = true;
            conductor.cleanupSubscriptionLink(this);
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean hasReachedEndOfLife()
    {
        return reachedEndOfLife;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return this.getClass().getName() + "{" +
            "registrationId=" + registrationId +
            ", streamId=" + streamId +
            ", sessionId=" + sessionId +
            ", hasSessionId=" + hasSessionId +
            ", isReliable=" + isReliable() +
            ", isSparse=" + isSparse() +
            ", isTether=" + isTether() +
            ", isRejoin=" + isRejoin() +
            ", reachedEndOfLife=" + reachedEndOfLife +
            ", group=" + group +
            ", channel='" + channel + '\'' +
            ", aeronClient=" + aeronClient +
            ", positionBySubscribableMap=" + positionBySubscribableMap +
            '}';
    }
}

class NetworkSubscriptionLink extends SubscriptionLink
{
    private final boolean isReliable;
    private final boolean isRejoin;
    private final ReceiveChannelEndpoint channelEndpoint;

    NetworkSubscriptionLink(
        final long registrationId,
        final ReceiveChannelEndpoint channelEndpoint,
        final int streamId,
        final String channelUri,
        final AeronClient aeronClient,
        final SubscriptionParams params)
    {
        super(registrationId, streamId, channelUri, aeronClient, params);

        this.isReliable = params.isReliable;
        this.isRejoin = params.isRejoin;
        this.channelEndpoint = channelEndpoint;
    }

    boolean isReliable()
    {
        return isReliable;
    }

    boolean isRejoin()
    {
        return isRejoin;
    }

    ReceiveChannelEndpoint channelEndpoint()
    {
        return channelEndpoint;
    }

    boolean matches(final PublicationImage image)
    {
        return image.channelEndpoint() == this.channelEndpoint &&
            image.streamId() == this.streamId &&
            isWildcardOrSessionIdMatch(image.sessionId());
    }

    boolean matches(final ReceiveChannelEndpoint channelEndpoint, final int streamId, final SubscriptionParams params)
    {
        final boolean isExactWildcardOrSessionIdMatch =
            hasSessionId == params.hasSessionId && (!hasSessionId || this.sessionId == params.sessionId);

        return channelEndpoint == this.channelEndpoint &&
            streamId == this.streamId &&
            isExactWildcardOrSessionIdMatch;
    }

    boolean matches(final ReceiveChannelEndpoint channelEndpoint, final int streamId, final int sessionId)
    {
        return channelEndpoint == this.channelEndpoint &&
            streamId == this.streamId &&
            isWildcardOrSessionIdMatch(sessionId);
    }
}

class IpcSubscriptionLink extends SubscriptionLink
{
    IpcSubscriptionLink(
        final long registrationId,
        final int streamId,
        final String channelUri,
        final AeronClient aeronClient,
        final SubscriptionParams params)
    {
        super(registrationId, streamId, channelUri, aeronClient, params);
    }

    boolean matches(final IpcPublication publication)
    {
        return publication.streamId() == streamId && isWildcardOrSessionIdMatch(publication.sessionId());
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
        final SubscriptionParams params)
    {
        super(registrationId, streamId, spiedChannel.originalUriString(), aeronClient, params);

        this.udpChannel = spiedChannel;
    }

    boolean matches(final NetworkPublication publication)
    {
        final UdpChannel publicationChannel = publication.channelEndpoint().udpChannel();
        final boolean isSameChannelTag = udpChannel.hasTag() && udpChannel.tag() == publicationChannel.tag();

        return streamId == publication.streamId() && (isSameChannelTag ||
            (isWildcardOrSessionIdMatch(publication.sessionId()) &&
            udpChannel.canonicalForm().equals(publicationChannel.canonicalForm())));
    }
}

class UntetheredSubscription
{
    enum State
    {
        ACTIVE,
        LINGER,
        RESTING
    }

    State state = State.ACTIVE;
    long timeOfLastUpdateNs;
    final SubscriptionLink subscriptionLink;
    final ReadablePosition position;

    UntetheredSubscription(final SubscriptionLink subscriptionLink, final ReadablePosition position, final long timeNs)
    {
        this.subscriptionLink = subscriptionLink;
        this.position = position;
        this.timeOfLastUpdateNs = timeNs;
    }

    void state(final State newState, final long nowNs, final int streamId, final int sessionId)
    {
        stateChange(state, newState, subscriptionLink.registrationId, streamId, sessionId, nowNs);
        state = newState;
        timeOfLastUpdateNs = nowNs;
    }

    void stateChange(
        final State oldState,
        final State newState,
        final long subscriptionId,
        final int streamId,
        final int sessionId,
        final long nowNs)
    {
//        System.out.println(nowNs + ": subscriptionId=" + subscriptionId + ", streamId=" + streamId +
//            ", sessionId=" + sessionId + ", " + oldState + " -> " + newState);
    }
}
