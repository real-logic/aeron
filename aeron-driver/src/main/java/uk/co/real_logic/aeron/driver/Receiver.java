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
import uk.co.real_logic.aeron.driver.cmd.AddSubscriptionCmd;
import uk.co.real_logic.aeron.driver.cmd.NewConnectedSubscriptionCmd;
import uk.co.real_logic.aeron.driver.cmd.RegisterMediaSubscriptionEndpointCmd;
import uk.co.real_logic.aeron.driver.cmd.RemoveSubscriptionCmd;

/**
 * Receiver service for JVM based media driver, uses an event loop with command buffer
 */
public class Receiver extends Agent
{
    private final NioSelector nioSelector;
    private final DriverConductorProxy conductorProxy;
    private final OneToOneConcurrentArrayQueue<? super Object> commandQueue;
    private final EventLogger logger;

    public Receiver(final MediaDriver.DriverContext ctx) throws Exception
    {
        super(ctx.receiverIdleStrategy(), ctx.receiverLogger()::logException);

        this.conductorProxy = ctx.driverConductorProxy();
        this.nioSelector = ctx.receiverNioSelector();
        this.commandQueue = ctx.receiverCommandQueue();
        this.logger = ctx.receiverLogger();
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
                    if (obj instanceof NewConnectedSubscriptionCmd)
                    {
                        onNewConnectedSubscription((NewConnectedSubscriptionCmd)obj);
                    }
                    else if (obj instanceof AddSubscriptionCmd)
                    {
                        final AddSubscriptionCmd cmd = (AddSubscriptionCmd)obj;
                        onAddSubscription(cmd.mediaSubscriptionEndpoint(), cmd.channelId());
                    }
                    else if (obj instanceof RemoveSubscriptionCmd)
                    {
                        final RemoveSubscriptionCmd cmd = (RemoveSubscriptionCmd)obj;
                        onRemoveSubscription(cmd.mediaSubscriptionEndpoint(), cmd.channelId());
                    }
                    else if (obj instanceof RegisterMediaSubscriptionEndpointCmd)
                    {
                        final RegisterMediaSubscriptionEndpointCmd cmd = (RegisterMediaSubscriptionEndpointCmd)obj;
                        onRegisterMediaSubscriptionEndpoint(cmd.mediaSubscriptionEndpoint());
                    }
                }
                catch (final Exception ex)
                {
                    // TODO: Send error to client - however best if validated by conductor so receiver not delayed
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

    private void onAddSubscription(final MediaSubscriptionEndpoint mediaSubscriptionEndpoint,
                                   final long channelId) throws Exception
    {
        mediaSubscriptionEndpoint.dispatcher().addSubscription(channelId);
    }

    private void onRemoveSubscription(final MediaSubscriptionEndpoint mediaSubscriptionEndpoint,
                                      final long channelId)
    {
        mediaSubscriptionEndpoint.dispatcher().removeSubscription(channelId);
    }

    private void onNewConnectedSubscription(final NewConnectedSubscriptionCmd cmd)
    {
        final MediaSubscriptionEndpoint mediaSubscriptionEndpoint = cmd.mediaSubscriptionEndpoint();

        mediaSubscriptionEndpoint.dispatcher().addConnectedSubscription(cmd.connectedSubscription());
    }

    private void onRegisterMediaSubscriptionEndpoint(final MediaSubscriptionEndpoint mediaSubscriptionEndpoint)
    {
        mediaSubscriptionEndpoint.registerForRead(nioSelector);
    }
}
