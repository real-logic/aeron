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

import uk.co.real_logic.aeron.util.*;
import uk.co.real_logic.aeron.util.command.ControlProtocolEvents;
import uk.co.real_logic.aeron.util.command.SubscriptionMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.GapScanner;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import static uk.co.real_logic.aeron.mediadriver.MediaConductor.NAK_MULTICAST_DELAY_GENERATOR;
import static uk.co.real_logic.aeron.mediadriver.MediaConductor.NAK_UNICAST_DELAY_GENERATOR;

/**
 * Receiver service for JVM based media driver, uses an event loop with command buffer
 */
public class Receiver extends Agent
{
    private final RingBuffer commandBuffer;
    private final NioSelector nioSelector;
    private final TimerWheel conductorTimerWheel;
    private final MediaConductorProxy conductorProxy;
    private final Map<UdpDestination, DataFrameHandler> frameHandlerByDestinationMap = new HashMap<>();
    private final SubscriptionMessageFlyweight subscriberMessage = new SubscriptionMessageFlyweight();
    private final Queue<NewConnectedSubscriptionEvent> newConnectedSubscriptionEventQueue;
    private final AtomicArray<DriverConnectedSubscription> connectedSubscriptions;

    public Receiver(final MediaDriver.MediaDriverContext ctx) throws Exception
    {
        super(ctx.receiverIdleStrategy());

        this.commandBuffer = ctx.receiverCommandBuffer();
        this.conductorProxy = ctx.mediaConductorProxy();
        this.nioSelector = ctx.receiverNioSelector();
        this.newConnectedSubscriptionEventQueue = ctx.newConnectedSubscriptionEventQueue();
        this.conductorTimerWheel = ctx.conductorTimerWheel();
        this.connectedSubscriptions = ctx.connectedSubscriptions();
    }

    public int doWork()
    {
        int workCount = 0;
        try
        {
            workCount += nioSelector.processKeys();
            workCount += processCommandBuffer();
            workCount += processConnectedSubscriptionEventQueue();
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
            // TODO: log
        }

        return workCount;
    }

    private int processConnectedSubscriptionEventQueue()
    {
        int workCount = 0;

        NewConnectedSubscriptionEvent state;
        while ((state = newConnectedSubscriptionEventQueue.poll()) != null)
        {
            ++workCount;
            onNewConnectedSubscription(state);
        }

        return workCount;
    }

    private int processCommandBuffer()
    {
        return commandBuffer.read(
            (msgTypeId, buffer, index, length) ->
            {
                try
                {
                    switch (msgTypeId)
                    {
                        case ControlProtocolEvents.ADD_SUBSCRIPTION:
                            subscriberMessage.wrap(buffer, index);
                            onAddSubscription(subscriberMessage.destination(), subscriberMessage.channelIds());
                            break;

                        case ControlProtocolEvents.REMOVE_SUBSCRIPTION:
                            subscriberMessage.wrap(buffer, index);
                            onRemoveSubscription(subscriberMessage.destination(), subscriberMessage.channelIds());
                            break;
                    }
                }
                catch (final InvalidDestinationException ex)
                {
                    // TODO: log this
                    onError(ErrorCode.INVALID_DESTINATION, length);
                }
                catch (final SubscriptionNotRegisteredException ex)
                {
                    // TODO: log this
                    onError(ErrorCode.SUBSCRIBER_NOT_REGISTERED, length);
                }
                catch (final Exception ex)
                {
                    // TODO: log this as well as send the error response
                    ex.printStackTrace();
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

    public DataFrameHandler frameHandler(final UdpDestination destination)
    {
        return frameHandlerByDestinationMap.get(destination);
    }

    private void onError(final ErrorCode errorCode, final int length)
    {
        conductorProxy.addErrorResponse(errorCode, subscriberMessage, length);
    }

    private void onAddSubscription(final String destination, final long[] channelIds) throws Exception
    {
        final UdpDestination rcvDestination = UdpDestination.parse(destination);
        DataFrameHandler frameHandler = frameHandler(rcvDestination);

        if (null == frameHandler)
        {
            frameHandler = new DataFrameHandler(rcvDestination, nioSelector, conductorProxy, connectedSubscriptions);
            frameHandlerByDestinationMap.put(rcvDestination, frameHandler);
        }

        frameHandler.addSubscriptions(channelIds);
    }

    private void onRemoveSubscription(final String destination, final long[] channelIds)
    {
        final UdpDestination rcvDestination = UdpDestination.parse(destination);
        final DataFrameHandler frameHandler = frameHandler(rcvDestination);

        if (null == frameHandler)
        {
            throw new SubscriptionNotRegisteredException("destination unknown for receiver remove: " + destination);
        }

        frameHandler.removeSubscriptions(channelIds);

        if (0 == frameHandler.subscribedChannelCount())
        {
            frameHandlerByDestinationMap.remove(rcvDestination);
            frameHandler.close();
        }
    }

    private void onNewConnectedSubscription(final NewConnectedSubscriptionEvent e)
    {
        final DataFrameHandler frameHandler = frameHandler(e.destination());
        FeedbackDelayGenerator delayGenerator;

        if (null == frameHandler)
        {
            System.err.println("onNewConnectedSubscription: could not find frameHandler");
            return;
        }

        final GapScanner[] scanners = e.bufferRotator().buffers()
            .map((r) -> new GapScanner(r.logBuffer(), r.stateBuffer()))
            .toArray(GapScanner[]::new);

        if (e.destination().isMulticast())
        {
            delayGenerator = NAK_MULTICAST_DELAY_GENERATOR;
        }
        else
        {
            delayGenerator = NAK_UNICAST_DELAY_GENERATOR;
        }

        final LossHandler lossHandler = new LossHandler(scanners, conductorTimerWheel, delayGenerator);

        lossHandler.currentTermId(e.termId());
        frameHandler.onSubscriptionReady(e, lossHandler);
    }
}
