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
import org.agrona.collections.BiInt2ObjectMap;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;

import static io.aeron.driver.DataPacketDispatcher.SessionState.INIT_IN_PROGRESS;
import static io.aeron.driver.DataPacketDispatcher.SessionState.ON_COOL_DOWN;
import static io.aeron.driver.DataPacketDispatcher.SessionState.PENDING_SETUP_FRAME;

/**
 * Handling of dispatching data packets to {@link PublicationImage}s streams.
 * <p>
 * All methods should be called from the {@link Receiver} thread.
 */
public class DataPacketDispatcher implements DataPacketHandler, SetupMessageHandler
{
    public enum SessionState
    {
        PENDING_SETUP_FRAME,
        INIT_IN_PROGRESS,
        ON_COOL_DOWN,
    }

    private final BiInt2ObjectMap<SessionState> ignoredSessionsMap = new BiInt2ObjectMap<>();
    private final Int2ObjectHashMap<Int2ObjectHashMap<PublicationImage>> sessionsByStreamIdMap =
        new Int2ObjectHashMap<>();
    private final DriverConductorProxy conductorProxy;
    private final Receiver receiver;

    public DataPacketDispatcher(final DriverConductorProxy conductorProxy, final Receiver receiver)
    {
        this.conductorProxy = conductorProxy;
        this.receiver = receiver;
    }

    public void addSubscription(final int streamId)
    {
        if (null == sessionsByStreamIdMap.get(streamId))
        {
            sessionsByStreamIdMap.put(streamId, new Int2ObjectHashMap<>());
        }
    }

    public void removeSubscription(final int streamId)
    {
        final Int2ObjectHashMap<PublicationImage> imageBySessionIdMap = sessionsByStreamIdMap.remove(streamId);
        if (null == imageBySessionIdMap)
        {
            throw new UnknownSubscriptionException("No subscription registered on stream " + streamId);
        }

        for (final PublicationImage image : imageBySessionIdMap.values())
        {
            image.ifActiveGoInactive();
        }
    }

    public void addPublicationImage(final PublicationImage image)
    {
        final int sessionId = image.sessionId();
        final int streamId = image.streamId();

        final Int2ObjectHashMap<PublicationImage> imageBySessionIdMap = sessionsByStreamIdMap.get(streamId);

        imageBySessionIdMap.put(sessionId, image);
        ignoredSessionsMap.remove(sessionId, streamId);

        image.activate();
    }

    public void removePublicationImage(final PublicationImage image)
    {
        final int sessionId = image.sessionId();
        final int streamId = image.streamId();

        final Int2ObjectHashMap<PublicationImage> imageBySessionIdMap = sessionsByStreamIdMap.get(streamId);
        if (null != imageBySessionIdMap)
        {
            final PublicationImage mappedImage = imageBySessionIdMap.get(sessionId);

            if (null != mappedImage && mappedImage.correlationId() == image.correlationId())
            {
                imageBySessionIdMap.remove(sessionId);
                ignoredSessionsMap.remove(sessionId, streamId);
            }
        }

        image.ifActiveGoInactive();
        ignoredSessionsMap.put(sessionId, streamId, ON_COOL_DOWN);
    }

    public void removePendingSetup(final int sessionId, final int streamId)
    {
        if (PENDING_SETUP_FRAME == ignoredSessionsMap.get(sessionId, streamId))
        {
            ignoredSessionsMap.remove(sessionId, streamId);
        }
    }

    public void removeCoolDown(final int sessionId, final int streamId)
    {
        if (ON_COOL_DOWN == ignoredSessionsMap.get(sessionId, streamId))
        {
            ignoredSessionsMap.remove(sessionId, streamId);
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
        final Int2ObjectHashMap<PublicationImage> imageBySessionIdMap = sessionsByStreamIdMap.get(streamId);

        if (null != imageBySessionIdMap)
        {
            final int sessionId = header.sessionId();
            final int termId = header.termId();
            final PublicationImage image = imageBySessionIdMap.get(sessionId);

            if (null != image)
            {
                return image.insertPacket(termId, header.termOffset(), buffer, length);
            }
            else if (null == ignoredSessionsMap.get(sessionId, streamId) && !DataHeaderFlyweight.isEndOfStream(buffer))
            {
                elicitSetupMessageFromSource(channelEndpoint, srcAddress, streamId, sessionId);
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
        final Int2ObjectHashMap<PublicationImage> imageBySessionIdMap = sessionsByStreamIdMap.get(streamId);

        if (null != imageBySessionIdMap)
        {
            final int sessionId = header.sessionId();
            final int initialTermId = header.initialTermId();
            final int activeTermId = header.activeTermId();
            final PublicationImage image = imageBySessionIdMap.get(sessionId);

            if (null == image && isNotAlreadyInProgressOrOnCoolDown(streamId, sessionId))
            {
                if (channelEndpoint.isMulticast() && channelEndpoint.multicastTtl() < header.ttl())
                {
                    channelEndpoint.possibleTtlAsymmetryEncountered();
                }

                createPublicationImage(
                    channelEndpoint,
                    srcAddress,
                    streamId,
                    sessionId,
                    initialTermId,
                    activeTermId,
                    header.termOffset(),
                    header.termLength(),
                    header.mtuLength());
            }
        }
    }

    public void onRttMeasurement(
        final ReceiveChannelEndpoint channelEndpoint,
        final RttMeasurementFlyweight header,
        final InetSocketAddress srcAddress)
    {
        final int streamId = header.streamId();
        final Int2ObjectHashMap<PublicationImage> imageBySessionIdMap = sessionsByStreamIdMap.get(streamId);

        if (null != imageBySessionIdMap)
        {
            final int sessionId = header.sessionId();
            final PublicationImage image = imageBySessionIdMap.get(sessionId);

            if (null != image)
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
                    image.onRttMeasurement(header, srcAddress);
                }
            }
        }
    }

    public boolean shouldElicitSetupMessage()
    {
        return !sessionsByStreamIdMap.isEmpty();
    }

    private boolean isNotAlreadyInProgressOrOnCoolDown(final int streamId, final int sessionId)
    {
        final SessionState state = ignoredSessionsMap.get(sessionId, streamId);

        return INIT_IN_PROGRESS != state && ON_COOL_DOWN != state;
    }

    private void elicitSetupMessageFromSource(
        final ReceiveChannelEndpoint channelEndpoint,
        final InetSocketAddress srcAddress,
        final int streamId,
        final int sessionId)
    {
        final InetSocketAddress controlAddress =
            channelEndpoint.isMulticast() ? channelEndpoint.udpChannel().remoteControl() : srcAddress;

        ignoredSessionsMap.put(sessionId, streamId, PENDING_SETUP_FRAME);

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
        final int mtuLength)
    {
        final InetSocketAddress controlAddress =
            channelEndpoint.isMulticast() ? channelEndpoint.udpChannel().remoteControl() : srcAddress;

        ignoredSessionsMap.put(sessionId, streamId, INIT_IN_PROGRESS);
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
