/*
 * Copyright 2014 Real Logic Ltd.
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

import uk.co.real_logic.aeron.common.Agent;
import uk.co.real_logic.aeron.common.concurrent.AtomicCounter;
import uk.co.real_logic.aeron.common.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.aeron.driver.cmd.*;

import java.util.function.Consumer;

/**
 * Receiver service for JVM based media driver, uses an event loop with command buffer
 */
public class Receiver implements Agent
{
    private final TransportPoller transportPoller;
    private final OneToOneConcurrentArrayQueue<Object> commandQueue;
    private final Consumer<Object> onConductorCommandFunc;
    private final AtomicCounter totalBytesReceived;

    public Receiver(final MediaDriver.Context ctx)
    {
        this.transportPoller = ctx.receiverNioSelector();
        this.commandQueue = ctx.receiverCommandQueue();
        this.totalBytesReceived = ctx.systemCounters().bytesReceived();

        onConductorCommandFunc = this::onConductorCommand;
    }

    private void onConductorCommand(final Object obj)
    {
        if (obj instanceof NewConnectionCmd)
        {
            final NewConnectionCmd cmd = (NewConnectionCmd)obj;
            onNewConnection(cmd.channelEndpoint(), cmd.connection());
        }
        else if (obj instanceof AddSubscriptionCmd)
        {
            final AddSubscriptionCmd cmd = (AddSubscriptionCmd)obj;
            onAddSubscription(cmd.mediaSubscriptionEndpoint(), cmd.streamId());
        }
        else if (obj instanceof RemoveSubscriptionCmd)
        {
            final RemoveSubscriptionCmd cmd = (RemoveSubscriptionCmd)obj;
            onRemoveSubscription(cmd.receiveChannelEndpoint(), cmd.streamId());
        }
        else if (obj instanceof RegisterReceiveChannelEndpointCmd)
        {
            final RegisterReceiveChannelEndpointCmd cmd = (RegisterReceiveChannelEndpointCmd)obj;
            onRegisterMediaSubscriptionEndpoint(cmd.receiveChannelEndpoint());
        }
        else if (obj instanceof RemoveConnectionCmd)
        {
            final RemoveConnectionCmd cmd = (RemoveConnectionCmd)obj;
            onRemoveConnection(cmd.connection());
        }
        else if (obj instanceof RemovePendingSetupCmd)
        {
            final RemovePendingSetupCmd cmd = (RemovePendingSetupCmd)obj;
            onRemovePendingSetup(cmd.channelEndpoint(), cmd.sessionId(), cmd.streamId());
        }
        else if (obj instanceof CloseReceiveChannelEndpointCmd)
        {
            final CloseReceiveChannelEndpointCmd cmd = (CloseReceiveChannelEndpointCmd)obj;
            onCloseReceiveChannelEndpoint(cmd.receiveChannelEndpoint());
        }
        else if (obj instanceof CloseSubscriptionCmd)
        {
            final CloseSubscriptionCmd cmd = (CloseSubscriptionCmd)obj;
            onCloseSubscription(cmd.subscription());
        }
    }

    public int doWork() throws Exception
    {
        final int workCount = commandQueue.drain(onConductorCommandFunc);
        final int bytesReceived = transportPoller.pollTransports();

        totalBytesReceived.addOrdered(bytesReceived);

        return workCount + bytesReceived;
    }

    public String roleName()
    {
        return "receiver";
    }

    public void onAddSubscription(final ReceiveChannelEndpoint channelEndpoint, final int streamId)
    {
        channelEndpoint.dispatcher().addSubscription(streamId);
    }

    public void onRemoveSubscription(final ReceiveChannelEndpoint channelEndpoint, final int streamId)
    {
        channelEndpoint.dispatcher().onRemoveSubscription(streamId);
    }

    public void onNewConnection(final ReceiveChannelEndpoint channelEndpoint, final DriverConnection connection)
    {
        channelEndpoint.dispatcher().addConnection(connection);
    }

    public void onRemoveConnection(final DriverConnection connection)
    {
        connection.receiveChannelEndpoint()
                  .dispatcher()
                  .removeConnection(connection);
    }

    public void onRegisterMediaSubscriptionEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        channelEndpoint.registerForRead(transportPoller);
        transportPoller.selectNowWithoutProcessing();
    }

    public void onRemovePendingSetup(final ReceiveChannelEndpoint channelEndpoint, final int sessionId, final int streamId)
    {
        channelEndpoint.dispatcher().removePendingSetup(sessionId, streamId);
    }

    public void onCloseReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        channelEndpoint.close();
        transportPoller.selectNowWithoutProcessing();
    }

    public void onCloseSubscription(final DriverSubscription subscription)
    {
        subscription.close();
    }
}
