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
    private static final EventLogger LOGGER = new EventLogger(Receiver.class);

    private final NioSelector nioSelector;
    private final DriverConductorProxy conductorProxy;
    private final Map<UdpDestination, DataFrameHandler> frameHandlerByDestinationMap = new HashMap<>();
    private final OneToOneConcurrentArrayQueue<? super Object> commandQueue;

    public Receiver(final MediaDriver.DriverContext ctx) throws Exception
    {
        super(ctx.receiverIdleStrategy(), LOGGER::logException);

        this.conductorProxy = ctx.driverConductorProxy();
        this.nioSelector = ctx.receiverNioSelector();
        this.commandQueue = ctx.receiverCommandQueue();
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
                    LOGGER.logException(ex);
                }
            });
    }

    /**
     * Close ReceiverThread down. Returns immediately.
     */
    public void close()
    {
        stop();
        frameHandlerByDestinationMap.forEach((destination, frameHandler) -> frameHandler.close());
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

    public DataFrameHandler getFrameHandler(final UdpDestination destination)
    {
        return frameHandlerByDestinationMap.get(destination);
    }

    private void onAddSubscription(final UdpDestination udpDestination, final long channelId) throws Exception
    {
        DataFrameHandler frameHandler = getFrameHandler(udpDestination);

        if (null == frameHandler)
        {
            frameHandler = new DataFrameHandler(udpDestination, nioSelector, conductorProxy);
            frameHandlerByDestinationMap.put(udpDestination, frameHandler);
        }

        frameHandler.addSubscription(channelId);
    }

    private void onRemoveSubscription(final UdpDestination udpDestination, final long channelId)
    {
        final DataFrameHandler frameHandler = getFrameHandler(udpDestination);

        if (null == frameHandler)
        {
            throw new UnknownSubscriptionException("Unknown Subscription: destination=" + udpDestination);
        }

        frameHandler.removeSubscription(channelId);

        if (0 == frameHandler.subscriptionCount())
        {
            frameHandlerByDestinationMap.remove(udpDestination);
            frameHandler.close();
        }
    }

    private void onNewConnectedSubscription(final NewConnectedSubscriptionCmd cmd)
    {
        final DriverConnectedSubscription connectedSubscription = cmd.connectedSubscription();
        final DataFrameHandler frameHandler = getFrameHandler(connectedSubscription.udpDestination());

        if (null != frameHandler)
        {
            frameHandler.onConnectedSubscriptionReady(connectedSubscription);
        }
        else
        {
            final String destination = connectedSubscription.udpDestination().toString();
            LOGGER.log(EventCode.COULD_NOT_FIND_FRAME_HANDLER_FOR_NEW_CONNECTED_SUBSCRIPTION, destination);
        }
    }
}
