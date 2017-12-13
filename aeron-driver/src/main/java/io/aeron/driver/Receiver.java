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

import io.aeron.driver.cmd.ReceiverCmd;
import io.aeron.driver.media.DataTransportPoller;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import org.agrona.collections.ArrayListUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.function.Consumer;

import static io.aeron.driver.Configuration.PENDING_SETUPS_TIMEOUT_NS;
import static io.aeron.driver.status.SystemCounterDescriptor.BYTES_RECEIVED;

/**
 * Receiver agent for JVM based media driver, uses an event loop with command buffer
 */
public class Receiver implements Agent, Consumer<ReceiverCmd>
{
    private final DataTransportPoller dataTransportPoller;
    private final OneToOneConcurrentArrayQueue<ReceiverCmd> commandQueue;
    private final AtomicCounter totalBytesReceived;
    private final NanoClock clock;
    private final ArrayList<PublicationImage> publicationImages = new ArrayList<>();
    private final ArrayList<PendingSetupMessageFromSource> pendingSetupMessages = new ArrayList<>();
    private final DriverConductorProxy conductorProxy;

    public Receiver(final MediaDriver.Context ctx)
    {
        dataTransportPoller = ctx.dataTransportPoller();
        commandQueue = ctx.receiverCommandQueue();
        totalBytesReceived = ctx.systemCounters().get(BYTES_RECEIVED);
        clock = ctx.nanoClock();
        conductorProxy = ctx.driverConductorProxy();
    }

    public void onClose()
    {
        dataTransportPoller.close();
    }

    public String roleName()
    {
        return "receiver";
    }

    public int doWork()
    {
        int workCount = commandQueue.drain(this, Configuration.COMMAND_DRAIN_LIMIT);
        final int bytesReceived = dataTransportPoller.pollTransports();

        final long nowNs = clock.nanoTime();

        final ArrayList<PublicationImage> publicationImages = this.publicationImages;
        for (int lastIndex = publicationImages.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final PublicationImage image = publicationImages.get(i);
            if (image.hasActivityAndNotEndOfStream(nowNs))
            {
                workCount += image.sendPendingStatusMessage();
                workCount += image.processPendingLoss();
                workCount += image.initiateAnyRttMeasurements(nowNs);
            }
            else
            {
                image.removeFromDispatcher();
                ArrayListUtil.fastUnorderedRemove(publicationImages, i, lastIndex);
                lastIndex--;
            }
        }

        checkPendingSetupMessages(nowNs);

        totalBytesReceived.getAndAddOrdered(bytesReceived);

        return workCount + bytesReceived;
    }

    public void addPendingSetupMessage(
        final int sessionId,
        final int streamId,
        final ReceiveChannelEndpoint channelEndpoint,
        final boolean periodic,
        final InetSocketAddress controlAddress)
    {
        final PendingSetupMessageFromSource cmd = new PendingSetupMessageFromSource(
            sessionId, streamId, channelEndpoint, periodic, controlAddress);
        cmd.timeOfStatusMessageNs(clock.nanoTime());
        pendingSetupMessages.add(cmd);
    }

    public void onAddSubscription(final ReceiveChannelEndpoint channelEndpoint, final int streamId)
    {
        channelEndpoint.addSubscription(streamId);
    }

    public void onRemoveSubscription(final ReceiveChannelEndpoint channelEndpoint, final int streamId)
    {
        channelEndpoint.removeSubscription(streamId);
    }

    public void onNewPublicationImage(final ReceiveChannelEndpoint channelEndpoint, final PublicationImage image)
    {
        publicationImages.add(image);
        channelEndpoint.addPublicationImage(image);
    }

    public void onRegisterReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        channelEndpoint.openChannel(conductorProxy);
        channelEndpoint.registerForRead(dataTransportPoller);
        channelEndpoint.indicateActive();

        if (channelEndpoint.hasExplicitControl())
        {
            addPendingSetupMessage(0, 0, channelEndpoint, true, channelEndpoint.explicitControlAddress());
            channelEndpoint.sendSetupElicitingStatusMessage(channelEndpoint.explicitControlAddress(), 0, 0);
        }
    }

    public void onCloseReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        channelEndpoint.close();
    }

    public void onRemoveCoolDown(final ReceiveChannelEndpoint channelEndpoint, final int sessionId, final int streamId)
    {
        channelEndpoint.removeCoolDown(sessionId, streamId);
    }

    public void accept(final ReceiverCmd cmd)
    {
        cmd.execute(this);
    }

    private void checkPendingSetupMessages(final long nowNs)
    {
        final ArrayList<PendingSetupMessageFromSource> pendingSetupMessages = this.pendingSetupMessages;
        for (int lastIndex = pendingSetupMessages.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final PendingSetupMessageFromSource pending = pendingSetupMessages.get(i);

            if (nowNs > (pending.timeOfStatusMessageNs() + PENDING_SETUPS_TIMEOUT_NS))
            {
                if (!pending.isPeriodic())
                {
                    ArrayListUtil.fastUnorderedRemove(pendingSetupMessages, i, lastIndex);
                    lastIndex--;
                    pending.removeFromDataPacketDispatcher();
                }
                else if (pending.shouldElicitSetupMessage())
                {
                    pending.channelEndpoint().sendSetupElicitingStatusMessage(
                        pending.controlAddress(), pending.sessionId(), pending.streamId());
                    pending.timeOfStatusMessageNs(nowNs);
                }
            }
        }
    }
}
