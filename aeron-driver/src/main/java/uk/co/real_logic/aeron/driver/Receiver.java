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
import uk.co.real_logic.aeron.common.event.EventCode;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.driver.cmd.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Receiver service for JVM based media driver, uses an event loop with command buffer
 */
public class Receiver extends Agent
{
    private final NioSelector nioSelector;
    private final DriverConductorProxy conductorProxy;
    private final Map<UdpDestination, SubscriptionMediaEndpoint> mediaEndpointByDestinationMap = new HashMap<>();
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
                        onAddSubscription(cmd.destination(), cmd.channelId());
                    }
                    else if (obj instanceof RemoveSubscriptionCmd)
                    {
                        final RemoveSubscriptionCmd cmd = (RemoveSubscriptionCmd)obj;
                        onRemoveSubscription(cmd.destination(), cmd.channelId());
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
        mediaEndpointByDestinationMap.forEach((destination, mediaEndpoint) -> mediaEndpoint.close());
    }

    /**
     * Return the {@link uk.co.real_logic.aeron.driver.NioSelector} in use by the thread
     *
     * @return the {@link uk.co.real_logic.aeron.driver.NioSelector} in use by the thread
     */
    public NioSelector nioSelector()
    {
        return nioSelector;
    }

    public SubscriptionMediaEndpoint subscriptionMediaEndpoint(final UdpDestination destination)
    {
        return mediaEndpointByDestinationMap.get(destination);
    }

    private void onAddSubscription(final UdpDestination udpDestination, final long channelId) throws Exception
    {
        SubscriptionMediaEndpoint mediaEndpoint = subscriptionMediaEndpoint(udpDestination);

        if (null == mediaEndpoint)
        {
            mediaEndpoint = new SubscriptionMediaEndpoint(udpDestination, nioSelector, conductorProxy, new EventLogger());
            mediaEndpointByDestinationMap.put(udpDestination, mediaEndpoint);
        }

        mediaEndpoint.addSubscription(channelId);
    }

    private void onRemoveSubscription(final UdpDestination udpDestination, final long channelId)
    {
        final SubscriptionMediaEndpoint mediaEndpoint = subscriptionMediaEndpoint(udpDestination);

        if (null == mediaEndpoint)
        {
            throw new UnknownSubscriptionException("Unknown Subscription: destination=" + udpDestination);
        }

        mediaEndpoint.removeSubscription(channelId);

        if (0 == mediaEndpoint.subscriptionCount())
        {
            mediaEndpointByDestinationMap.remove(udpDestination);
            mediaEndpoint.close();
        }
    }

    private void onNewConnectedSubscription(final NewConnectedSubscriptionCmd cmd)
    {
        final DriverConnectedSubscription connectedSubscription = cmd.connectedSubscription();
        final SubscriptionMediaEndpoint mediaEndpoint = subscriptionMediaEndpoint(connectedSubscription.udpDestination());

        if (null != mediaEndpoint)
        {
            mediaEndpoint.onConnectedSubscriptionReady(connectedSubscription);
        }
        else
        {
            final String destination = connectedSubscription.udpDestination().toString();
            EventLogger.log(EventCode.COULD_NOT_FIND_FRAME_HANDLER_FOR_NEW_CONNECTED_SUBSCRIPTION, destination);
        }
    }
}
