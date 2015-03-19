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
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.util.function.Consumer;

/**
 * Receiver agent for JVM based media driver, uses an event loop with command buffer
 */
public class Receiver implements Agent
{
    private final TransportPoller transportPoller;
    private final OneToOneConcurrentArrayQueue<ReceiverCmd> commandQueue;
    private final Consumer<ReceiverCmd> onReceiverCmdFunc = this::onReceiverCmd;
    private final AtomicCounter totalBytesReceived;

    public Receiver(final MediaDriver.Context ctx)
    {
        this.transportPoller = ctx.receiverNioSelector();
        this.commandQueue = ctx.receiverCommandQueue();
        this.totalBytesReceived = ctx.systemCounters().bytesReceived();
    }

    public String roleName()
    {
        return "receiver";
    }

    public int doWork() throws Exception
    {
        final int workCount = commandQueue.drain(onReceiverCmdFunc);
        final int bytesReceived = transportPoller.pollTransports();

        totalBytesReceived.addOrdered(bytesReceived);

        return workCount + bytesReceived;
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

    public void onRegisterMediaChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
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

    private void onReceiverCmd(final ReceiverCmd cmd)
    {
        cmd.execute(this);
    }
}
