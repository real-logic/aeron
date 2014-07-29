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
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.driver.cmd.*;

/**
 * Receiver service for JVM based media driver, uses an event loop with command buffer
 */
public class Receiver extends Agent
{
    private final NioSelector nioSelector;
    private final OneToOneConcurrentArrayQueue<? super Object> commandQueue;
    private final EventLogger logger;

    public Receiver(final MediaDriver.Context ctx) throws Exception
    {
        super(ctx.receiverIdleStrategy(), ctx.eventLoggerException());

        this.nioSelector = ctx.receiverNioSelector();
        this.commandQueue = ctx.receiverCommandQueue();
        this.logger = ctx.eventLogger();
    }

    public int doWork() throws Exception
    {
        return nioSelector.processKeys() + processConductorCommands();
    }

    private int processConductorCommands()
    {
        return commandQueue.drain(
            (obj) ->
            {
                try
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
                        onRemoveSubscription(cmd.mediaSubscriptionEndpoint(), cmd.streamId());
                    }
                    else if (obj instanceof RegisterReceiverChannelEndpointCmd)
                    {
                        final RegisterReceiverChannelEndpointCmd cmd = (RegisterReceiverChannelEndpointCmd)obj;
                        onRegisterMediaSubscriptionEndpoint(cmd.receiverChannelEndpoint());
                    }
                    else if (obj instanceof RemoveConnectionCmd)
                    {
                        onRemoveConnection((RemoveConnectionCmd) obj);
                    }
                }
                catch (final Exception ex)
                {
                    logger.logException(ex);
                }
            });
    }

    /**
     * Close ReceiverThread down. Returns immediately.
     */
    public void close()
    {
        stop();
    }

    /**
     * Return the {@link NioSelector} in use by the thread
     *
     * @return the {@link NioSelector} in use by the thread
     */
    public NioSelector nioSelector()
    {
        return nioSelector;
    }

    private void onAddSubscription(final ReceiveChannelEndpoint receiveChannelEndpoint, final int streamId)
        throws Exception
    {
        receiveChannelEndpoint.dispatcher().addSubscription(streamId);
    }

    private void onRemoveSubscription(final ReceiveChannelEndpoint receiveChannelEndpoint, final int streamId)
    {
        receiveChannelEndpoint.dispatcher().removeSubscription(streamId);
    }

    private void onNewConnection(final NewConnectionCmd cmd)
    {
        final ReceiveChannelEndpoint receiveChannelEndpoint = cmd.receiverChannelEndpoint();

        receiveChannelEndpoint.dispatcher().addConnection(cmd.connection());
    }

    private void onRemoveConnection(final RemoveConnectionCmd cmd)
    {
        final ReceiveChannelEndpoint receiveChannelEndpoint = cmd.receiverChannelEndpoint();

        receiveChannelEndpoint.dispatcher().removeConnection(cmd.connection());
    }

    private void onRegisterMediaSubscriptionEndpoint(final ReceiveChannelEndpoint receiveChannelEndpoint)
    {
        receiveChannelEndpoint.registerForRead(nioSelector);
    }
}
