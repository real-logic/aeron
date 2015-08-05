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

import uk.co.real_logic.aeron.driver.cmd.ReceiverCmd;
import uk.co.real_logic.aeron.driver.media.*;
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.NanoClock;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.util.ArrayList;
import java.util.function.Consumer;

/**
 * Receiver agent for JVM based media driver, uses an event loop with command buffer
 */
public class Receiver implements Agent, Consumer<ReceiverCmd>
{
    private final long statusMessageTimeout;
    private final DataTransportPoller transportPoller;
    private final OneToOneConcurrentArrayQueue<ReceiverCmd> commandQueue;
    private final AtomicCounter totalBytesReceived;
    private final NanoClock clock;
    private final ArrayList<NetworkedImage> images = new ArrayList<>();
    private final ArrayList<PendingSetupMessageFromSource> pendingSetupMessages = new ArrayList<>();

    public Receiver(final MediaDriver.Context ctx)
    {
        statusMessageTimeout = ctx.statusMessageTimeout();
        transportPoller = ctx.receiverTransportPoller();
        commandQueue = ctx.receiverCommandQueue();
        totalBytesReceived = ctx.systemCounters().bytesReceived();
        clock = ctx.nanoClock();
    }

    public String roleName()
    {
        return "receiver";
    }

    public int doWork() throws Exception
    {
        int workCount = commandQueue.drain(this);
        final int bytesReceived = transportPoller.pollTransports();

        final long now = clock.nanoTime();
        for (int i = images.size() - 1; i >= 0; i--)
        {
            final NetworkedImage image = images.get(i);
            if (!image.checkForActivity(now, Configuration.IMAGE_LIVENESS_TIMEOUT_NS))
            {
                image.removeFromDispatcher();
                images.remove(i);
            }
            else
            {
                workCount += image.sendPendingStatusMessage(now, statusMessageTimeout);
                workCount += image.sendPendingNak();
            }
        }

        timeoutPendingSetupMessages(now);

        totalBytesReceived.addOrdered(bytesReceived);

        return workCount + bytesReceived;
    }

    public void addPendingSetupMessage(final int sessionId, final int streamId, final ReceiveChannelEndpoint channelEndpoint)
    {
        final PendingSetupMessageFromSource cmd = new PendingSetupMessageFromSource(sessionId, streamId, channelEndpoint);
        cmd.timeOfStatusMessage(clock.nanoTime());
        pendingSetupMessages.add(cmd);
    }

    public void onAddSubscription(final ReceiveChannelEndpoint channelEndpoint, final int streamId)
    {
        channelEndpoint.dispatcher().addSubscription(streamId);
    }

    public void onRemoveSubscription(final ReceiveChannelEndpoint channelEndpoint, final int streamId)
    {
        channelEndpoint.dispatcher().removeSubscription(streamId);
    }

    public void onNewImage(final ReceiveChannelEndpoint channelEndpoint, final NetworkedImage image)
    {
        images.add(image);
        channelEndpoint.dispatcher().addImage(image);
    }

    public void onRegisterReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        channelEndpoint.openChannel();
        channelEndpoint.registerForRead(transportPoller);
        transportPoller.selectNowWithoutProcessing();
    }

    public void onCloseReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        channelEndpoint.close();
        transportPoller.selectNowWithoutProcessing();
    }

    public void onRemoveCoolDown(final ReceiveChannelEndpoint channelEndpoint, final int sessionId, final int streamId)
    {
        channelEndpoint.dispatcher().removeCoolDown(sessionId, streamId);
    }

    public void accept(final ReceiverCmd cmd)
    {
        cmd.execute(this);
    }

    private void timeoutPendingSetupMessages(final long now)
    {
        for (int i = pendingSetupMessages.size() - 1; i >= 0; i--)
        {
            final PendingSetupMessageFromSource cmd = pendingSetupMessages.get(i);

            if (now > (cmd.timeOfStatusMessage() + Configuration.PENDING_SETUPS_TIMEOUT_NS))
            {
                pendingSetupMessages.remove(i);
                cmd.channelEndpoint().dispatcher().removePendingSetup(cmd.sessionId(), cmd.streamId());
            }
        }
    }
}
