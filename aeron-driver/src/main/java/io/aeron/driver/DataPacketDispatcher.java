/*
 * Copyright 2014-2018 Real Logic Ltd.
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

import io.aeron.driver.exceptions.UnknownSubscriptionException;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.SetupFlyweight;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;

import static io.aeron.driver.DataPacketDispatcher.SessionState.*;

/**
 * Handling of dispatching data packets to {@link PublicationImage}s streams.
 * <p>
 * All methods should be called from the {@link Receiver} thread.
 */
public class DataPacketDispatcher
{
    enum SessionState
    {
        ACTIVE,
        PENDING_SETUP_FRAME,
        INIT_IN_PROGRESS,
        ON_COOL_DOWN,
        NO_INTEREST
    }

    static class SessionInterest
    {
        SessionState state;
        PublicationImage image;

        SessionInterest(final SessionState state)
        {
            this.state = state;
        }
    }

    static class StreamInterest
    {
        boolean isForAllSessions;
        Int2ObjectHashMap<SessionInterest> sessionInterestByIdMap;
        IntHashSet subscribedSessionIds;

        StreamInterest(final boolean isForAllSessions)
        {
            this.isForAllSessions = isForAllSessions;
            sessionInterestByIdMap = new Int2ObjectHashMap<>();
            subscribedSessionIds = new IntHashSet();
        }
    }

    private final Int2ObjectHashMap<StreamInterest> streamInterestByIdMap = new Int2ObjectHashMap<>();
    private final DriverConductorProxy conductorProxy;
    private final Receiver receiver;

    public DataPacketDispatcher(final DriverConductorProxy conductorProxy, final Receiver receiver)
    {
        this.conductorProxy = conductorProxy;
        this.receiver = receiver;
    }

    public void addSubscription(final int streamId)
    {
        final StreamInterest streamInterest = streamInterestByIdMap.get(streamId);

        if (null == streamInterest)
        {
            streamInterestByIdMap.put(streamId, new StreamInterest(true));
        }
        else if (!streamInterest.isForAllSessions)
        {
            streamInterest.isForAllSessions = true;

            for (final int sessionId : streamInterest.sessionInterestByIdMap.keySet())
            {
                final SessionInterest sessionInterest = streamInterest.sessionInterestByIdMap.get(sessionId);
                if (NO_INTEREST == sessionInterest.state)
                {
                    streamInterest.sessionInterestByIdMap.remove(sessionId);
                }
            }
        }
    }

    public void addSubscription(final int streamId, final int sessionId)
    {
        StreamInterest streamInterest = streamInterestByIdMap.get(streamId);

        if (null == streamInterest)
        {
            streamInterest = new StreamInterest(false);
            streamInterestByIdMap.put(streamId, streamInterest);
        }

        streamInterest.subscribedSessionIds.add(sessionId);

        final SessionInterest sessionInterest = streamInterest.sessionInterestByIdMap.get(sessionId);
        if (null != sessionInterest && NO_INTEREST == sessionInterest.state)
        {
            streamInterest.sessionInterestByIdMap.remove(sessionId);
        }
    }

    public void removeSubscription(final int streamId)
    {
        final StreamInterest streamInterest = streamInterestByIdMap.get(streamId);
        if (null == streamInterest)
        {
            throw new UnknownSubscriptionException("No subscription registered on stream " + streamId);
        }

        for (final int sessionId : streamInterest.sessionInterestByIdMap.keySet())
        {
            final SessionInterest sessionInterest = streamInterest.sessionInterestByIdMap.get(sessionId);

            if (!streamInterest.subscribedSessionIds.contains(sessionId))
            {
                if (null != sessionInterest.image)
                {
                    sessionInterest.image.ifActiveGoInactive();
                }

                streamInterest.sessionInterestByIdMap.remove(sessionId);
            }
        }

        streamInterest.isForAllSessions = false;

        if (streamInterest.subscribedSessionIds.isEmpty())
        {
            streamInterestByIdMap.remove(streamId);
        }
    }

    public void removeSubscription(final int streamId, final int sessionId)
    {
        final StreamInterest streamInterest = streamInterestByIdMap.get(streamId);
        if (null == streamInterest)
        {
            throw new UnknownSubscriptionException("No subscription registered on stream " + streamId);
        }

        final SessionInterest sessionInterest = streamInterest.sessionInterestByIdMap.remove(sessionId);
        if (null != sessionInterest && null != sessionInterest.image)
        {
            sessionInterest.image.ifActiveGoInactive();
        }

        streamInterest.subscribedSessionIds.remove(sessionId);

        if (!streamInterest.isForAllSessions && streamInterest.subscribedSessionIds.isEmpty())
        {
            streamInterestByIdMap.remove(streamId);
        }
    }

    public void addPublicationImage(final PublicationImage image)
    {
        final int sessionId = image.sessionId();
        final int streamId = image.streamId();

        final StreamInterest streamInterest = streamInterestByIdMap.get(streamId);
        SessionInterest sessionInterest = streamInterest.sessionInterestByIdMap.get(sessionId);

        if (null == sessionInterest)
        {
            sessionInterest = new SessionInterest(ACTIVE);
            streamInterest.sessionInterestByIdMap.put(sessionId, sessionInterest);
        }
        else
        {
            sessionInterest.state = ACTIVE;
        }

        sessionInterest.image = image;

        image.activate();
    }

    public void removePublicationImage(final PublicationImage image)
    {
        final int sessionId = image.sessionId();
        final int streamId = image.streamId();

        final StreamInterest streamInterest = streamInterestByIdMap.get(streamId);
        if (null != streamInterest)
        {
            final SessionInterest sessionInterest = streamInterest.sessionInterestByIdMap.get(sessionId);
            if (null != sessionInterest && null != sessionInterest.image)
            {
                if (sessionInterest.image.correlationId() == image.correlationId())
                {
                    sessionInterest.state = ON_COOL_DOWN;
                    sessionInterest.image = null;
                }
            }
        }

        image.ifActiveGoInactive();
    }

    public void removePendingSetup(final int sessionId, final int streamId)
    {
        final StreamInterest streamInterest = streamInterestByIdMap.get(streamId);
        if (null != streamInterest)
        {
            final SessionInterest sessionInterest = streamInterest.sessionInterestByIdMap.get(sessionId);
            if (null != sessionInterest && PENDING_SETUP_FRAME == sessionInterest.state)
            {
                streamInterest.sessionInterestByIdMap.remove(sessionId);
            }
        }
    }

    public void removeCoolDown(final int sessionId, final int streamId)
    {
        final StreamInterest streamInterest = streamInterestByIdMap.get(streamId);
        if (null != streamInterest)
        {
            final SessionInterest sessionInterest = streamInterest.sessionInterestByIdMap.get(sessionId);
            if (null != sessionInterest && ON_COOL_DOWN == sessionInterest.state)
            {
                streamInterest.sessionInterestByIdMap.remove(sessionId);
            }
        }
    }

    public int onDataPacket(
        final ReceiveChannelEndpoint channelEndpoint,
        final DataHeaderFlyweight header,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress,
        final int transportIndex)
    {
        final int streamId = header.streamId();
        final StreamInterest streamInterest = streamInterestByIdMap.get(streamId);

        if (null != streamInterest)
        {
            final int sessionId = header.sessionId();
            final int termId = header.termId();
            final SessionInterest sessionInterest = streamInterest.sessionInterestByIdMap.get(sessionId);

            if (null != sessionInterest)
            {
                if (null != sessionInterest.image)
                {
                    return sessionInterest.image.insertPacket(
                        termId, header.termOffset(), buffer, length, transportIndex, srcAddress);
                }
            }
            else if (!DataHeaderFlyweight.isEndOfStream(buffer))
            {
                if (streamInterest.isForAllSessions || streamInterest.subscribedSessionIds.contains(sessionId))
                {
                    streamInterest.sessionInterestByIdMap.put(sessionId, new SessionInterest(PENDING_SETUP_FRAME));
                    elicitSetupMessageFromSource(channelEndpoint, transportIndex, srcAddress, streamId, sessionId);
                }
                else
                {
                    streamInterest.sessionInterestByIdMap.put(sessionId, new SessionInterest(NO_INTEREST));
                }
            }
        }

        return 0;
    }

    public void onSetupMessage(
        final ReceiveChannelEndpoint channelEndpoint,
        final SetupFlyweight header,
        final InetSocketAddress srcAddress,
        final int transportIndex)
    {
        final int streamId = header.streamId();
        final StreamInterest streamInterest = streamInterestByIdMap.get(streamId);

        if (null != streamInterest)
        {
            final int sessionId = header.sessionId();
            final int initialTermId = header.initialTermId();
            final int activeTermId = header.activeTermId();
            final SessionInterest sessionInterest = streamInterest.sessionInterestByIdMap.get(sessionId);

            if (null != sessionInterest)
            {
                if (null == sessionInterest.image && (PENDING_SETUP_FRAME == sessionInterest.state))
                {
                    sessionInterest.state = INIT_IN_PROGRESS;

                    createPublicationImage(
                        channelEndpoint,
                        transportIndex,
                        srcAddress,
                        streamId,
                        sessionId,
                        initialTermId,
                        activeTermId,
                        header.termOffset(),
                        header.termLength(),
                        header.mtuLength(),
                        header.ttl());
                }
                else if (null != sessionInterest.image)
                {
                    sessionInterest.image.addControlAddressIfUnknown(transportIndex, srcAddress);
                }
            }
            else if (streamInterest.isForAllSessions || streamInterest.subscribedSessionIds.contains(sessionId))
            {
                streamInterest.sessionInterestByIdMap.put(sessionId, new SessionInterest(INIT_IN_PROGRESS));
                createPublicationImage(
                    channelEndpoint,
                    transportIndex,
                    srcAddress,
                    streamId,
                    sessionId,
                    initialTermId,
                    activeTermId,
                    header.termOffset(),
                    header.termLength(),
                    header.mtuLength(),
                    header.ttl());
            }
            else
            {
                streamInterest.sessionInterestByIdMap.put(sessionId, new SessionInterest(NO_INTEREST));
            }
        }
    }

    public void onRttMeasurement(
        final ReceiveChannelEndpoint channelEndpoint,
        final RttMeasurementFlyweight header,
        final InetSocketAddress srcAddress,
        final int transportIndex)
    {
        final int streamId = header.streamId();
        final StreamInterest streamInterest = streamInterestByIdMap.get(streamId);

        if (null != streamInterest)
        {
            final int sessionId = header.sessionId();
            final SessionInterest sessionInterest = streamInterest.sessionInterestByIdMap.get(sessionId);

            if (null != sessionInterest && null != sessionInterest.image)
            {
                if (RttMeasurementFlyweight.REPLY_FLAG == (header.flags() & RttMeasurementFlyweight.REPLY_FLAG))
                {
                    // TODO: check rate limit

                    final InetSocketAddress controlAddress = channelEndpoint.isMulticast(transportIndex) ?
                        channelEndpoint.udpChannel(transportIndex).remoteControl() : srcAddress;

                    channelEndpoint.sendRttMeasurement(
                        transportIndex, controlAddress, sessionId, streamId, header.echoTimestampNs(), 0, false);
                }
                else
                {
                    sessionInterest.image.onRttMeasurement(header, transportIndex, srcAddress);
                }
            }
        }
    }

    public boolean shouldElicitSetupMessage()
    {
        return !streamInterestByIdMap.isEmpty();
    }

    private void elicitSetupMessageFromSource(
        final ReceiveChannelEndpoint channelEndpoint,
        final int transportIndex,
        final InetSocketAddress srcAddress,
        final int streamId,
        final int sessionId)
    {
        final InetSocketAddress controlAddress = channelEndpoint.isMulticast(transportIndex) ?
            channelEndpoint.udpChannel(transportIndex).remoteControl() : srcAddress;

        channelEndpoint.sendSetupElicitingStatusMessage(transportIndex, controlAddress, sessionId, streamId);
        receiver.addPendingSetupMessage(sessionId, streamId, transportIndex, channelEndpoint, false, controlAddress);
    }

    private void createPublicationImage(
        final ReceiveChannelEndpoint channelEndpoint,
        final int transportIndex,
        final InetSocketAddress srcAddress,
        final int streamId,
        final int sessionId,
        final int initialTermId,
        final int activeTermId,
        final int termOffset,
        final int termLength,
        final int mtuLength,
        final int setupTtl)
    {
        final InetSocketAddress controlAddress = channelEndpoint.isMulticast(transportIndex) ?
            channelEndpoint.udpChannel(transportIndex).remoteControl() : srcAddress;

        if (channelEndpoint.isMulticast(transportIndex) && channelEndpoint.multicastTtl(transportIndex) < setupTtl)
        {
            channelEndpoint.possibleTtlAsymmetryEncountered();
        }

        conductorProxy.createPublicationImage(
            sessionId,
            streamId,
            initialTermId,
            activeTermId,
            termOffset,
            termLength,
            mtuLength,
            transportIndex,
            controlAddress,
            srcAddress,
            channelEndpoint);
    }
}
