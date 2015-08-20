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
    private final DataTransportPoller dataTransportPoller;
    private final OneToOneConcurrentArrayQueue<ReceiverCmd> commandQueue;
    private final AtomicCounter totalBytesReceived;
    private final NanoClock clock;
    private final ArrayList<PublicationImage> publicationImages = new ArrayList<>();
    private final ArrayList<PendingSetupMessageFromSource> pendingSetupMessages = new ArrayList<>();

    public Receiver(final MediaDriver.Context ctx)
    {
        statusMessageTimeout = ctx.statusMessageTimeout();
        dataTransportPoller = ctx.receiverTransportPoller();
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
        final int bytesReceived = dataTransportPoller.pollTransports();

        final long now = clock.nanoTime();
        for (int i = publicationImages.size() - 1; i >= 0; i--)
        {
            final PublicationImage image = publicationImages.get(i);
            if (!image.checkForActivity(now))
            {
                image.removeFromDispatcher();
                publicationImages.remove(i);
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

    public void onNewPublicationImage(final ReceiveChannelEndpoint channelEndpoint, final PublicationImage image)
    {
        publicationImages.add(image);
        channelEndpoint.dispatcher().addPublicationImage(image);
    }

    public void onRegisterReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        channelEndpoint.openChannel();
        channelEndpoint.registerForRead(dataTransportPoller);
        dataTransportPoller.selectNowWithoutProcessing();
    }

    public void onCloseReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        channelEndpoint.close();
        dataTransportPoller.selectNowWithoutProcessing();
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
