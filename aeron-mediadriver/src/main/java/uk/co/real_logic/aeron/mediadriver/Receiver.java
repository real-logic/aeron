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

import uk.co.real_logic.aeron.util.Agent;
import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.ErrorCode;
import uk.co.real_logic.aeron.util.TimerWheel;
import uk.co.real_logic.aeron.util.command.ControlProtocolEvents;
import uk.co.real_logic.aeron.util.command.SubscriberMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.GapScanner;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

/**
 * Receiver service for JVM based mediadriver, uses an event loop with command buffer
 */
public class Receiver extends Agent
{
    private final RingBuffer commandBuffer;
    private final NioSelector nioSelector;
    private final TimerWheel conductorTimerWheel;
    private final MediaConductorProxy conductorProxy;
    private final Map<UdpDestination, DataFrameHandler> frameHandlerByDestinationMap = new HashMap<>();
    private final SubscriberMessageFlyweight subscriberMessage = new SubscriberMessageFlyweight();
    private final Queue<NewReceiveBufferEvent> newBufferEventQueue;
    private final AtomicArray<SubscribedSession> globalSubscribedSessions = new AtomicArray<>();

    public Receiver(final MediaDriver.Context context) throws Exception
    {
        super(MediaDriver.AGENT_SLEEP_NANOS);

        this.commandBuffer = context.receiverCommandBuffer();
        this.conductorProxy = context.mediaConductorProxy();
        this.nioSelector = context.receiverNioSelector();
        this.newBufferEventQueue = context.newReceiveBufferEventQueue();
        this.conductorTimerWheel = context.conductorTimerWheel();
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
                        case ControlProtocolEvents.ADD_SUBSCRIBER:
                            subscriberMessage.wrap(buffer, index);
                            onNewSubscriber(subscriberMessage.destination(),
                                            subscriberMessage.channelIds());
                            break;

                        case ControlProtocolEvents.REMOVE_SUBSCRIBER:
                            subscriberMessage.wrap(buffer, index);
                            onRemoveSubscriber(subscriberMessage.destination(),
                                               subscriberMessage.channelIds());
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

        return messageRead > 0;
    }

    /**
     * Close ReceiverThread down. Returns immediately.
     */
    public void close()
    {
        stop();

        frameHandlerByDestinationMap.forEach((destination, frameHandler) ->frameHandler.close());
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

    /**
     * Called by MediaConductor on its thread.
     */
    public boolean processBufferRotation()
    {
        return globalSubscribedSessions.forEach(0, SubscribedSession::processBufferRotation);
    }

    /**
     * Called by MediaConductor on its thread.
     */
    public boolean scanForGaps()
    {
        return globalSubscribedSessions.forEach(0, SubscribedSession::scanForGaps);
    }

    private void onError(final ErrorCode errorCode, final int length)
    {
        conductorProxy.addErrorResponse(errorCode, subscriberMessage, length);
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
            System.err.println("onNewReceiverBuffers: could not find frameHandler");
            return;
        }

        final GapScanner[] scanners = e.buffer().buffers()
            .map((r) -> new GapScanner(r.logBuffer(), r.stateBuffer()))
            .toArray(GapScanner[]::new);

        final LossHandler lossHandler = new LossHandler(scanners, conductorTimerWheel,
            MediaConductor.NAK_UNICAST_DELAY_GENERATOR);

        frameHandler.onSubscriptionReady(e, lossHandler);
    }
}
