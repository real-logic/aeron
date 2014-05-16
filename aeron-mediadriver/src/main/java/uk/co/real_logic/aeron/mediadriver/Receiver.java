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
import uk.co.real_logic.aeron.util.command.SubscriberMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import static uk.co.real_logic.aeron.mediadriver.MediaDriver.SELECT_TIMEOUT;
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
    private final MediaConductorCursor adminThreadCursor;
    private final Map<UdpDestination, RcvFrameHandler> rcvDestinationMap = new HashMap<>();
    private final SubscriberMessageFlyweight subscriberMessage = new SubscriberMessageFlyweight();
    private final Queue<RcvBufferState> buffers = new OneToOneConcurrentArrayQueue<>(1024);
    private final RcvFrameHandlerFactory frameHandlerFactory;

    private final AtomicArray<RcvSessionState> sessionState = new AtomicArray<>();

    public Receiver(final MediaDriver.Context context) throws Exception
    {
        super(SELECT_TIMEOUT);

        commandBuffer = context.receiverCommandBuffer();
        adminThreadCursor =
            new MediaConductorCursor(context.conductorCommandBuffer(), context.adminNioSelector());
        nioSelector = context.rcvNioSelector();
        frameHandlerFactory = context.rcvFrameHandlerFactory();
    }

    public void process()
    {
        try
        {
            nioSelector.processKeys();
            processCommandBuffer();
            processBufferQueue();
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
            // TODO: log
        }
    }

    private void processBufferQueue()
    {
        RcvBufferState state;
        while ((state = buffers.poll()) != null)
        {
            attachBufferState(state);
        }
    }

    private void processCommandBuffer()
    {
        commandBuffer.read(
            (eventTypeId, buffer, index, length) ->
            {
               try
               {
                   switch (eventTypeId)
                   {
                       case ADD_SUBSCRIBER:
                           subscriberMessage.wrap(buffer, index);
                           onNewSubscriber(subscriberMessage.destination(), subscriberMessage.channelIds());
                           return;

                       case REMOVE_SUBSCRIBER:
                           subscriberMessage.wrap(buffer, index);
                           onRemoveSubscriber(subscriberMessage.destination(), subscriberMessage.channelIds());
                           return;
                   }
               }
               catch (final InvalidDestinationException ex)
               {
                   // TODO: log this
                   onError(INVALID_DESTINATION, length);
               }
               catch (final ReceiverNotRegisteredException ex)
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
    }

    private void onError(final ErrorCode errorCode, final int length)
    {
        adminThreadCursor.addErrorResponse(errorCode, subscriberMessage, length);
    }

    public AtomicArray<RcvSessionState> sessionState()
    {
        return sessionState;
    }

    /**
     * Close ReceiverThread down. Returns immediately.
     */
    public void close()
    {
        stop();
        wakeup();

        rcvDestinationMap.forEach(
            (destination, frameHandler) ->
            {
              frameHandler.close();
            });
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

    public boolean sendBuffer(final RcvBufferState buffer)
    {
        return buffers.offer(buffer);
    }

    public RcvFrameHandler frameHandler(final UdpDestination destination)
    {
        return rcvDestinationMap.get(destination);
    }

    private void onNewSubscriber(final String destination, final long[] channelIdList) throws Exception
    {
        final UdpDestination rcvDestination = UdpDestination.parse(destination);
        RcvFrameHandler rcv = frameHandler(rcvDestination);

        if (null == rcv)
        {
            rcv = frameHandlerFactory.newInstance(rcvDestination, sessionState);
            rcvDestinationMap.put(rcvDestination, rcv);
        }

        rcv.addChannels(channelIdList);
    }

    private void onRemoveSubscriber(final String destination, final long[] channelIdList)
    {
        final UdpDestination rcvDestination = UdpDestination.parse(destination);
        RcvFrameHandler rcv = frameHandler(rcvDestination);

        if (null == rcv)
        {
            throw new ReceiverNotRegisteredException("destination unknown for receiver remove: " + destination);
        }

        rcv.removeChannels(channelIdList);

        // if all channels gone, then take care of removing everything and closing the framehandler
        if (0 == rcv.channelCount())
        {
            rcvDestinationMap.remove(rcvDestination);
            rcv.close();
        }
    }

    private void attachBufferState(final RcvBufferState buffer)
    {
        RcvFrameHandler rcv = frameHandler(buffer.destination());

        if (null == rcv)
        {
            // should not happen
            // TODO: log this
            return;
        }

        rcv.attachBufferState(buffer);
    }

    /**
     * Called by MediaConductor on its thread, must
     */
    public void processBufferRotation()
    {
        sessionState.forEach(RcvSessionState::processBufferRotation);
    }

    /**
     * Called by MediaConductor on its thread
     */
    public void scanForGaps()
    {
        sessionState.forEach(RcvSessionState::scanForGaps);
    }
}
