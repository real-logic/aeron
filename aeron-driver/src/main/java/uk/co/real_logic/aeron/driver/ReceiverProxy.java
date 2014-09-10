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

import uk.co.real_logic.aeron.driver.cmd.*;

import java.util.Queue;

/**
 * Proxy for writing into the Receiver Thread's command buffer.
 */
// TODO: put all the command object creation inside
// TODO: pull in receiver proxy retry loop
public class ReceiverProxy
{
    private final Queue<? super Object> commandQueue;

    public ReceiverProxy(final Queue<? super Object> commandQueue)
    {
        this.commandQueue = commandQueue;
    }

    public boolean addSubscription(final ReceiveChannelEndpoint mediaEndpoint, final int streamId)
    {
        return commandQueue.offer(new AddSubscriptionCmd(mediaEndpoint, streamId));
    }

    public boolean removeSubscription(final ReceiveChannelEndpoint mediaEndpoint, final int streamId)
    {
        return commandQueue.offer(new RemoveSubscriptionCmd(mediaEndpoint, streamId));
    }

    public boolean newConnection(final NewConnectionCmd e)
    {
        return commandQueue.offer(e);
    }

    public boolean removeConnection(final DriverConnection connection)
    {
        return commandQueue.offer(new RemoveConnectionCmd(connection.receiveChannelEndpoint(), connection));
    }

    public boolean registerMediaEndpoint(final RegisterReceiveChannelEndpointCmd cmd)
    {
        return commandQueue.offer(cmd);
    }

    public boolean closeMediaEndpoint(final CloseReceiveChannelEndpointCmd cmd)
    {
        return commandQueue.offer(cmd);
    }

    public boolean removePendingSetup(final RemovePendingSetupCmd cmd)
    {
        return commandQueue.offer(cmd);
    }

    public void closeSubscription(final DriverSubscription subscription)
    {
        commandQueue.offer(new CloseSubscriptionCmd(subscription));
    }

}
