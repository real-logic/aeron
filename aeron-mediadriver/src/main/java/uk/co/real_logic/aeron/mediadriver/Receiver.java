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
package uk.co.real_logic.aeron.mediadriver;

import uk.co.real_logic.aeron.mediadriver.cmd.*;
import uk.co.real_logic.aeron.util.*;
import uk.co.real_logic.aeron.util.concurrent.AtomicArray;
import uk.co.real_logic.aeron.util.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.aeron.util.event.EventCode;
import uk.co.real_logic.aeron.util.event.EventLogger;

import java.util.HashMap;
import java.util.Map;

/**
 * Receiver service for JVM based media driver, uses an event loop with command buffer
 */
public class Receiver extends Agent
{
    private static final EventLogger LOGGER = new EventLogger(Receiver.class);

    private final NioSelector nioSelector;
    private final TimerWheel conductorTimerWheel;
    private final MediaConductorProxy conductorProxy;
    private final Map<UdpDestination, DataFrameHandler> frameHandlerByDestinationMap = new HashMap<>();
    private final OneToOneConcurrentArrayQueue<? super Object> commandQueue;
    private final AtomicArray<DriverConnectedSubscription> connectedSubscriptions;

    public Receiver(final MediaDriver.MediaDriverContext ctx) throws Exception
    {
        super(ctx.receiverIdleStrategy());

        this.conductorProxy = ctx.mediaConductorProxy();
        this.nioSelector = ctx.receiverNioSelector();
        this.commandQueue = ctx.receiverCommandQueue();
        this.conductorTimerWheel = ctx.conductorTimerWheel();
        this.connectedSubscriptions = ctx.connectedSubscriptions();
    }

    public int doWork()
    {
        int workCount = 0;
        try
        {
            workCount += nioSelector.processKeys();
            workCount += processConductorCommands();
        }
        catch (final Exception ex)
        {
            LOGGER.logException(ex);
        }

        return workCount;
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
     * Return the {@link uk.co.real_logic.aeron.mediadriver.NioSelector} in use by the thread
     *
     * @return the {@link uk.co.real_logic.aeron.mediadriver.NioSelector} in use by the thread
     */
    public NioSelector nioSelector()
    {
        return nioSelector;
    }

    public DataFrameHandler getFrameHandler(final UdpDestination destination)
    {
        return frameHandlerByDestinationMap.get(destination);
    }

    private void onAddSubscription(final String destination, final long channelId) throws Exception
    {
        final UdpDestination udpDestination = UdpDestination.parse(destination);
        DataFrameHandler frameHandler = getFrameHandler(udpDestination);

        if (null == frameHandler)
        {
            frameHandler = new DataFrameHandler(udpDestination, nioSelector, conductorProxy, connectedSubscriptions);
            frameHandlerByDestinationMap.put(udpDestination, frameHandler);
        }

        frameHandler.addSubscription(channelId);
    }

    private void onRemoveSubscription(final String destination, final long channelId)
    {
        final UdpDestination udpDestination = UdpDestination.parse(destination);
        final DataFrameHandler frameHandler = getFrameHandler(udpDestination);

        if (null == frameHandler)
        {
            throw new SubscriptionNotRegisteredException("destination unknown for receiver remove: " + destination);
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
        final DataFrameHandler frameHandler = getFrameHandler(cmd.destination());

        if (null == frameHandler)
        {
            final String destination = cmd.destination().toString();
            LOGGER.log(EventCode.COULD_NOT_FIND_FRAME_HANDLER_FOR_NEW_CONNECTED_SUBSCRIPTION, destination);
            return;
        }

        frameHandler.onConnectedSubscriptionReady(cmd);
    }
}
