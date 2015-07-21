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

import uk.co.real_logic.aeron.driver.media.ReceiveChannelEndpoint;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.protocol.SetupFlyweight;
import uk.co.real_logic.aeron.driver.exceptions.UnknownSubscriptionException;
import uk.co.real_logic.agrona.collections.BiInt2ObjectMap;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;

/**
 * Handling of dispatching data packets to {@link NetworkedImage}s streams.
 *
 * All methods should be called via {@link Receiver} thread
 */
public class DataPacketDispatcher implements DataPacketHandler, SetupMessageHandler
{
    private static final Integer PENDING_SETUP_FRAME = 1;
    private static final Integer INIT_IN_PROGRESS = 2;
    private static final Integer ON_COOLDOWN = 3;

    private final BiInt2ObjectMap<Integer> ignoredSessionsMap = new BiInt2ObjectMap<>();
    private final Int2ObjectHashMap<Int2ObjectHashMap<NetworkedImage>> sessionsByStreamIdMap = new Int2ObjectHashMap<>();
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
        final Int2ObjectHashMap<NetworkedImage> imageBySessionIdMap = sessionsByStreamIdMap.remove(streamId);
        if (null == imageBySessionIdMap)
        {
            throw new UnknownSubscriptionException("No subscription registered on stream " + streamId);
        }

        imageBySessionIdMap.values().forEach(NetworkedImage::ifActiveGoInactive);
    }

    public void addImage(final NetworkedImage image)
    {
        final int sessionId = image.sessionId();
        final int streamId = image.streamId();

        final Int2ObjectHashMap<NetworkedImage> imageBySessionIdMap = sessionsByStreamIdMap.get(streamId);
        if (null == imageBySessionIdMap)
        {
            throw new IllegalStateException("No subscription registered on stream " + streamId);
        }

        imageBySessionIdMap.put(sessionId, image);
        ignoredSessionsMap.remove(sessionId, streamId);

        image.status(NetworkedImage.Status.ACTIVE);
    }

    public void removeImage(final NetworkedImage image)
    {
        final int sessionId = image.sessionId();
        final int streamId = image.streamId();

        final Int2ObjectHashMap<NetworkedImage> imageBySessionIdMap = sessionsByStreamIdMap.get(streamId);
        if (null != imageBySessionIdMap)
        {
            imageBySessionIdMap.remove(sessionId);
            ignoredSessionsMap.remove(sessionId, streamId);
        }

        image.ifActiveGoInactive();
    }

    public void removePendingSetup(final int sessionId, final int streamId)
    {
        if (PENDING_SETUP_FRAME.equals(ignoredSessionsMap.get(sessionId, streamId)))
        {
            ignoredSessionsMap.remove(sessionId, streamId);
        }
    }

    public void addCooldown(final int sessionId, final int streamId)
    {
        ignoredSessionsMap.put(sessionId, streamId, ON_COOLDOWN);
    }

    public void removeCooldown(final int sessionId, final int streamId)
    {
        if (ON_COOLDOWN.equals(ignoredSessionsMap.get(sessionId, streamId)))
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
        final Int2ObjectHashMap<NetworkedImage> imageBySessionIdMap = sessionsByStreamIdMap.get(streamId);

        if (null != imageBySessionIdMap)
        {
            final int sessionId = header.sessionId();
            final int termId = header.termId();
            final NetworkedImage image = imageBySessionIdMap.get(sessionId);

            if (null != image)
            {
                return image.insertPacket(termId, header.termOffset(), buffer, length);
            }
            else if (null == ignoredSessionsMap.get(sessionId, streamId))
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
        final int length,
        final InetSocketAddress srcAddress)
    {
        final int streamId = header.streamId();
        final Int2ObjectHashMap<NetworkedImage> imageBySessionIdMap = sessionsByStreamIdMap.get(streamId);

        if (null != imageBySessionIdMap)
        {
            final int sessionId = header.sessionId();
            final int initialTermId = header.initialTermId();
            final int activeTermId = header.activeTermId();
            final NetworkedImage image = imageBySessionIdMap.get(sessionId);

            if (null == image && isNotAlreadyInProgress(streamId, sessionId))
            {
                createImage(
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

    private boolean isNotAlreadyInProgress(final int streamId, final int sessionId)
    {
        return !INIT_IN_PROGRESS.equals(ignoredSessionsMap.get(sessionId, streamId));
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
        receiver.addPendingSetupMessage(sessionId, streamId, channelEndpoint);
    }

    private void createImage(
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
        conductorProxy.createImage(
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
