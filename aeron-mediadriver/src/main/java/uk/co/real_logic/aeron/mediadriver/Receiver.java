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
import uk.co.real_logic.aeron.util.command.SubscriberMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.util.*;

import static uk.co.real_logic.aeron.mediadriver.MediaDriver.AGENT_SLEEP_NANOS;
import static uk.co.real_logic.aeron.util.ErrorCode.INVALID_DESTINATION;
import static uk.co.real_logic.aeron.util.ErrorCode.SUBSCRIBER_NOT_REGISTERED;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.ADD_SUBSCRIBER;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.REMOVE_SUBSCRIBER;

/**
 * Receiver service for JVM based mediadriver, uses an event loop with command buffer
 */
public class Receiver extends Agent
{
    private final RingBuffer commandBuffer;
    private final NioSelector nioSelector;
    private final MediaConductorProxy conductorProxy;
    private final Map<UdpDestination, DataFrameHandler> frameHandlerByDestinationMap = new HashMap<>();
    private final SubscriberMessageFlyweight subscriberMessage = new SubscriberMessageFlyweight();
    private final Queue<NewReceiveBufferEvent> newBufferEventQueue;

    private final AtomicArray<SubscribedSession> globalSubscribedSessions = new AtomicArray<>();

    public Receiver(final MediaDriver.Context context) throws Exception
    {
        super(AGENT_SLEEP_NANOS);

        this.commandBuffer = context.receiverCommandBuffer();
        this.conductorProxy = context.mediaConductorProxy();
        this.nioSelector = context.receiverNioSelector();
        this.newBufferEventQueue = context.newReceiveBufferEventQueue();
    }

    public boolean doWork()
    {
        boolean hasDoneWork = false;
        try
        {
            hasDoneWork |= nioSelector.processKeys();
            hasDoneWork |= processCommandBuffer();
            hasDoneWork |= processNewBufferEventQueue();
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
            // TODO: log
        }

        return hasDoneWork;
    }

    private boolean processNewBufferEventQueue()
    {
        boolean workDone = false;

        NewReceiveBufferEvent state;
        while ((state = newBufferEventQueue.poll()) != null)
        {
            workDone = true;
            onNewReceiveBuffers(state);
        }

        return workDone;
    }

    private boolean processCommandBuffer()
    {
        final int messageRead = commandBuffer.read(
            (msgTypeId, buffer, index, length) ->
            {
                try
                {
                    switch (msgTypeId)
                    {
                        case ADD_SUBSCRIBER:
                            subscriberMessage.wrap(buffer, index);
                            onNewSubscriber(subscriberMessage.destination(),
                                            subscriberMessage.channelIds());
                            break;

                        case REMOVE_SUBSCRIBER:
                            subscriberMessage.wrap(buffer, index);
                            onRemoveSubscriber(subscriberMessage.destination(),
                                               subscriberMessage.channelIds());
                            break;
                    }
                }
                catch (final InvalidDestinationException ex)
                {
                    // TODO: log this
                    onError(INVALID_DESTINATION, length);
                }
                catch (final SubscriptionNotRegisteredException ex)
                {
                    // TODO: log this
                    onError(SUBSCRIBER_NOT_REGISTERED, length);
                }
                catch (final Exception ex)
                {
                    // TODO: log this as well as send the error response
                    ex.printStackTrace();
                }
            });

        return messageRead > 0;
    }

    private void onError(final ErrorCode errorCode, final int length)
    {
        conductorProxy.addErrorResponse(errorCode, subscriberMessage, length);
    }

    /**
     * Close ReceiverThread down. Returns immediately.
     */
    public void close()
    {
        stop();
        wakeup();

        frameHandlerByDestinationMap.forEach((destination, frameHandler) ->frameHandler.close());
        // TODO: if needed, use a CountdownLatch to sync...
    }

    /**
     * Wake up the selector if blocked
     */
    public void wakeup()
    {
        // TODO
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

    private void onNewSubscriber(final String destination, final long[] channelIds) throws Exception
    {
        final UdpDestination rcvDestination = UdpDestination.parse(destination);
        DataFrameHandler frameHandler = frameHandler(rcvDestination);

        if (null == frameHandler)
        {
            frameHandler = new DataFrameHandler(rcvDestination, nioSelector, conductorProxy, globalSubscribedSessions);
            frameHandlerByDestinationMap.put(rcvDestination, frameHandler);
        }

        frameHandler.addChannels(channelIds);
    }

    private void onRemoveSubscriber(final String destination, final long[] channelIds)
    {
        final UdpDestination rcvDestination = UdpDestination.parse(destination);
        DataFrameHandler frameHandler = frameHandler(rcvDestination);

        if (null == frameHandler)
        {
            throw new SubscriptionNotRegisteredException("destination unknown for receiver remove: " + destination);
        }

        frameHandler.removeChannels(channelIds);

        // if all channels gone, then take care of removing everything and closing the framehandler
        if (0 == frameHandler.channelCount())
        {
            frameHandlerByDestinationMap.remove(rcvDestination);
            frameHandler.close();
        }
    }

    private void onNewReceiveBuffers(final NewReceiveBufferEvent e)
    {
        final DataFrameHandler frameHandler = frameHandler(e.destination());

        if (null == frameHandler)
        {
            // should not happen
            // TODO: log this
            return;
        }

        frameHandler.onSubscriptionReady(e);
    }

    /**
     * Called by MediaConductor on its thread, must
     */
    public void processBufferRotation()
    {
        globalSubscribedSessions.forEach(SubscribedSession::processBufferRotation);
    }

    /**
     * Called by MediaConductor on its thread
     */
    public void scanForGaps()
    {
        globalSubscribedSessions.forEach(SubscribedSession::scanForGaps);
    }
}
