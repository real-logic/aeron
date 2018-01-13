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
public class DataPacketDispatcher implements DataPacketHandler, SetupMessageHandler
{
    public enum SessionState
    {
        ADDED,
        PENDING_SETUP_FRAME,
        INIT_IN_PROGRESS,
        ON_COOL_DOWN,
        REJECTED
    }

    private static class SessionIdState
    {
        SessionState state;
        PublicationImage image;

        SessionIdState(final SessionState state)
        {
            this.state = state;
        }
    }

    private static class StreamIdState
    {
        boolean acceptAll;
        Int2ObjectHashMap<SessionIdState> sessionIdMap;
        IntHashSet reservedSessionIds;

        StreamIdState(final boolean acceptAll)
        {
            this.acceptAll = acceptAll;
            sessionIdMap = new Int2ObjectHashMap<>();
            reservedSessionIds = new IntHashSet();
        }
    }

    private final Int2ObjectHashMap<StreamIdState> sessionsByStreamIdMap = new Int2ObjectHashMap<>();
    private final DriverConductorProxy conductorProxy;
    private final Receiver receiver;

    public DataPacketDispatcher(final DriverConductorProxy conductorProxy, final Receiver receiver)
    {
        this.conductorProxy = conductorProxy;
        this.receiver = receiver;
    }

    public void addSubscription(final int streamId)
    {
        final StreamIdState streamIdState = sessionsByStreamIdMap.get(streamId);

        if (null == streamIdState)
        {
            sessionsByStreamIdMap.put(streamId, new StreamIdState(true));
        }
        else if (!streamIdState.acceptAll)
        {
            streamIdState.acceptAll = true;

            for (final int sessionId : streamIdState.sessionIdMap.keySet())
            {
                final SessionIdState sessionIdState = streamIdState.sessionIdMap.get(sessionId);
                if (sessionIdState.state == REJECTED)
                {
                    streamIdState.sessionIdMap.remove(sessionId);
                }
            }
        }
    }

    public void addSubscription(final int streamId, final int sessionId)
    {
        StreamIdState streamIdState = sessionsByStreamIdMap.get(streamId);

        if (null == streamIdState)
        {
            streamIdState = new StreamIdState(false);
            streamIdState.reservedSessionIds.add(sessionId);
            sessionsByStreamIdMap.put(streamId, streamIdState);
        }
        else
        {
            streamIdState.reservedSessionIds.add(sessionId);
        }
    }

    public void removeSubscription(final int streamId)
    {
        final StreamIdState streamIdState = sessionsByStreamIdMap.get(streamId);
        if (null == streamIdState)
        {
            throw new UnknownSubscriptionException("No subscription registered on stream " + streamId);
        }

        for (final int sessionId : streamIdState.sessionIdMap.keySet())
        {
            final SessionIdState sessionIdState = streamIdState.sessionIdMap.get(sessionId);

            if (null != sessionIdState.image)
            {
                sessionIdState.image.ifActiveGoInactive();
                streamIdState.sessionIdMap.remove(sessionId);
            }
        }

        if (streamIdState.sessionIdMap.isEmpty())
        {
            sessionsByStreamIdMap.remove(streamId);
        }
    }

    public void removeSubscription(final int streamId, final int sessionId)
    {
        final StreamIdState streamIdState = sessionsByStreamIdMap.get(streamId);
        if (null == streamIdState)
        {
            throw new UnknownSubscriptionException("No subscription registered on stream " + streamId);
        }

        streamIdState.sessionIdMap.remove(sessionId);
        streamIdState.reservedSessionIds.remove(sessionId);

        if (streamIdState.sessionIdMap.isEmpty())
        {
            sessionsByStreamIdMap.remove(streamId);
        }
    }

    public void addPublicationImage(final PublicationImage image)
    {
        final int sessionId = image.sessionId();
        final int streamId = image.streamId();

        final StreamIdState streamIdState = sessionsByStreamIdMap.get(streamId);
        SessionIdState sessionIdState = streamIdState.sessionIdMap.get(sessionId);

        if (null == sessionIdState)
        {
            sessionIdState = new SessionIdState(ADDED);
            streamIdState.sessionIdMap.put(sessionId, sessionIdState);
        }
        else
        {
            sessionIdState.state = ADDED;
        }

        sessionIdState.image = image;

        image.activate();
    }

    public void removePublicationImage(final PublicationImage image)
    {
        final int sessionId = image.sessionId();
        final int streamId = image.streamId();

        final StreamIdState streamIdState = sessionsByStreamIdMap.get(streamId);
        if (null != streamIdState)
        {
            final SessionIdState sessionIdState = streamIdState.sessionIdMap.get(sessionId);
            if (null != sessionIdState && null != sessionIdState.image)
            {
                if (sessionIdState.image.correlationId() == image.correlationId())
                {
                    sessionIdState.state = ON_COOL_DOWN;
                    sessionIdState.image = null;
                }
            }
        }

        image.ifActiveGoInactive();
    }

    public void removePendingSetup(final int sessionId, final int streamId)
    {
        final StreamIdState streamIdState = sessionsByStreamIdMap.get(streamId);
        if (null != streamIdState)
        {
            final SessionIdState sessionIdState = streamIdState.sessionIdMap.get(sessionId);
            if (null != sessionIdState && PENDING_SETUP_FRAME == sessionIdState.state)
            {
                streamIdState.sessionIdMap.remove(sessionId);
            }
        }
    }

    public void removeCoolDown(final int sessionId, final int streamId)
    {
        final StreamIdState streamIdState = sessionsByStreamIdMap.get(streamId);
        if (null != streamIdState)
        {
            final SessionIdState sessionIdState = streamIdState.sessionIdMap.get(sessionId);
            if (null != sessionIdState && ON_COOL_DOWN == sessionIdState.state)
            {
                streamIdState.sessionIdMap.remove(sessionId);
            }
        }
    }

    public int onDataPacket(
        final ReceiveChannelEndpoint channelEndpoint,
        final DataHeaderFlyweight header,
        final UnsafeBuffer buffer,
        final int length,
        final InetSocketAddress srcAddress)
    {
        final int streamId = header.streamId();
        final StreamIdState streamIdState = sessionsByStreamIdMap.get(streamId);

        if (null != streamIdState)
        {
            final int sessionId = header.sessionId();
            final int termId = header.termId();
            final SessionIdState sessionIdState = streamIdState.sessionIdMap.get(sessionId);

            if (null != sessionIdState)
            {
                if (null != sessionIdState.image)
                {
                    return sessionIdState.image.insertPacket(termId, header.termOffset(), buffer, length);
                }
            }
            else if (!DataHeaderFlyweight.isEndOfStream(buffer) &&
                (streamIdState.acceptAll || streamIdState.reservedSessionIds.contains(sessionId)))
            {
                streamIdState.sessionIdMap.put(sessionId, new SessionIdState(PENDING_SETUP_FRAME));
                elicitSetupMessageFromSource(channelEndpoint, srcAddress, streamId, sessionId);
            }
            else if (!streamIdState.acceptAll && !DataHeaderFlyweight.isEndOfStream(buffer))
            {
                streamIdState.sessionIdMap.put(sessionId, new SessionIdState(REJECTED));
            }
        }

        return 0;
    }

    public void onSetupMessage(
        final ReceiveChannelEndpoint channelEndpoint,
        final SetupFlyweight header,
        final UnsafeBuffer buffer,
        final InetSocketAddress srcAddress)
    {
        final int streamId = header.streamId();
        final StreamIdState streamIdState = sessionsByStreamIdMap.get(streamId);

        if (null != streamIdState)
        {
            final int sessionId = header.sessionId();
            final int initialTermId = header.initialTermId();
            final int activeTermId = header.activeTermId();
            final SessionIdState sessionIdState = streamIdState.sessionIdMap.get(sessionId);

            if (null != sessionIdState)
            {
                if (null == sessionIdState.image && (PENDING_SETUP_FRAME == sessionIdState.state))
                {
                    sessionIdState.state = INIT_IN_PROGRESS;

                    createPublicationImage(
                        channelEndpoint,
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
            }
            else if (streamIdState.acceptAll || streamIdState.reservedSessionIds.contains(sessionId))
            {
                streamIdState.sessionIdMap.put(sessionId, new SessionIdState(INIT_IN_PROGRESS));
                createPublicationImage(
                    channelEndpoint,
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
                streamIdState.sessionIdMap.put(sessionId, new SessionIdState(REJECTED));
            }
        }
    }

    public void onRttMeasurement(
        final ReceiveChannelEndpoint channelEndpoint,
        final RttMeasurementFlyweight header,
        final InetSocketAddress srcAddress)
    {
        final int streamId = header.streamId();
        final StreamIdState streamIdState = sessionsByStreamIdMap.get(streamId);

        if (null != streamIdState)
        {
            final int sessionId = header.sessionId();
            final SessionIdState sessionIdState = streamIdState.sessionIdMap.get(sessionId);

            if (null != sessionIdState && null != sessionIdState.image)
            {
                if (RttMeasurementFlyweight.REPLY_FLAG == (header.flags() & RttMeasurementFlyweight.REPLY_FLAG))
                {
                    // TODO: check rate limit

                    final InetSocketAddress controlAddress =
                        channelEndpoint.isMulticast() ? channelEndpoint.udpChannel().remoteControl() : srcAddress;

                    channelEndpoint.sendRttMeasurement(
                        controlAddress, sessionId, streamId, header.echoTimestampNs(), 0, false);
                }
                else
                {
                    sessionIdState.image.onRttMeasurement(header, srcAddress);
                }
            }
        }
    }

    public boolean shouldElicitSetupMessage()
    {
        return !sessionsByStreamIdMap.isEmpty();
    }

    private void elicitSetupMessageFromSource(
        final ReceiveChannelEndpoint channelEndpoint,
        final InetSocketAddress srcAddress,
        final int streamId,
        final int sessionId)
    {
        final InetSocketAddress controlAddress =
            channelEndpoint.isMulticast() ? channelEndpoint.udpChannel().remoteControl() : srcAddress;

        channelEndpoint.sendSetupElicitingStatusMessage(controlAddress, sessionId, streamId);
        receiver.addPendingSetupMessage(sessionId, streamId, channelEndpoint, false, controlAddress);
    }

    private void createPublicationImage(
        final ReceiveChannelEndpoint channelEndpoint,
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
        final InetSocketAddress controlAddress =
            channelEndpoint.isMulticast() ? channelEndpoint.udpChannel().remoteControl() : srcAddress;

        if (channelEndpoint.isMulticast() && channelEndpoint.multicastTtl() < setupTtl)
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
            controlAddress,
            srcAddress,
            channelEndpoint);
    }
}
