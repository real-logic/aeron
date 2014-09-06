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
import uk.co.real_logic.aeron.common.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.aeron.driver.cmd.*;

import java.util.function.Consumer;

/**
 * Receiver service for JVM based media driver, uses an event loop with command buffer
 */
public class Receiver extends Agent
{
    private final NioSelector nioSelector;
    private final OneToOneConcurrentArrayQueue<Object> commandQueue;
    private final Consumer<Object> onConductorCommandFunc;

    public Receiver(final MediaDriver.Context ctx)
    {
        super(ctx.receiverIdleStrategy(), ctx.exceptionConsumer(), ctx.systemCounters().driverExceptions());

        this.nioSelector = ctx.receiverNioSelector();
        this.commandQueue = ctx.receiverCommandQueue();
        onConductorCommandFunc = this::onConductorCommand;
    }

    private void onConductorCommand(final Object obj)
    {
        if (obj instanceof NewConnectionCmd)
        {
            onNewConnection((NewConnectionCmd) obj);
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
            onRemoveConnection((RemoveConnectionCmd) obj);
        }
        else if (obj instanceof RemovePendingSetupCmd)
        {
            onRemovePendingSetup((RemovePendingSetupCmd) obj);
        }
    }

    public int doWork() throws Exception
    {
        return commandQueue.drain(onConductorCommandFunc) + nioSelector.processKeys();
    }

    private void onAddSubscription(final ReceiveChannelEndpoint channelEndpoint, final int streamId)
    {
        channelEndpoint.dispatcher().addSubscription(streamId);
    }

    private void onRemoveSubscription(final ReceiveChannelEndpoint channelEndpoint, final int streamId)
    {
        channelEndpoint.dispatcher().removeSubscription(streamId);
    }

    private void onNewConnection(final NewConnectionCmd cmd)
    {
        final ReceiveChannelEndpoint channelEndpoint = cmd.receiveChannelEndpoint();

        channelEndpoint.dispatcher().addConnection(cmd.connection());
    }

    private void onRemoveConnection(final RemoveConnectionCmd cmd)
    {
        final ReceiveChannelEndpoint channelEndpoint = cmd.receiveChannelEndpoint();

        channelEndpoint.dispatcher().removeConnection(cmd.connection());
    }

    private void onRegisterMediaSubscriptionEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        channelEndpoint.registerForRead(nioSelector);
    }

    private void onRemovePendingSetup(final RemovePendingSetupCmd cmd)
    {
        final ReceiveChannelEndpoint channelEndpoint = cmd.channelEndpoint();

        channelEndpoint.dispatcher().removePendingSetup(cmd.sessionId(), cmd.streamId());
    }
}
