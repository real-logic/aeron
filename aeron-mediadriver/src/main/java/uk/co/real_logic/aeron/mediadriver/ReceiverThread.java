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

import uk.co.real_logic.aeron.util.ClosableThread;
import uk.co.real_logic.aeron.util.ErrorCode;
import uk.co.real_logic.aeron.util.command.ControlProtocolEvents;
import uk.co.real_logic.aeron.util.command.ConsumerMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static uk.co.real_logic.aeron.mediadriver.MediaDriver.SELECT_TIMEOUT;
import static uk.co.real_logic.aeron.util.ErrorCode.INVALID_DESTINATION;
import static uk.co.real_logic.aeron.util.ErrorCode.CONSUMER_NOT_REGISTERED;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.ADD_CONSUMER;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.REMOVE_CONSUMER;

/**
 * Receiver Thread for JVM based mediadriver, uses an event loop with command buffer
 */
public class ReceiverThread extends ClosableThread
{
    private final RingBuffer commandBuffer;
    private final NioSelector nioSelector;
    private final MediaDriverAdminThreadCursor adminThreadCursor;
    private final Map<UdpDestination, RcvFrameHandler> rcvDestinationMap = new HashMap<>();
    private final ConsumerMessageFlyweight consumerMessage;
    private final Queue<RcvBufferState> buffers;
    private final RcvFrameHandlerFactory frameHandlerFactory;

    public ReceiverThread(final MediaDriver.MediaDriverContext context) throws Exception
    {
        super(SELECT_TIMEOUT);
        this.commandBuffer = context.receiverThreadCommandBuffer();
        this.adminThreadCursor = new MediaDriverAdminThreadCursor(context.adminThreadCommandBuffer(),
                                                                             context.adminNioSelector());
        this.nioSelector = context.rcvNioSelector();
        this.frameHandlerFactory = context.rcvFrameHandlerFactory();
        this.consumerMessage = new ConsumerMessageFlyweight();
        this.buffers = new ConcurrentLinkedQueue<>();
    }

    public void process()
    {
        try
        {
            nioSelector.processKeys();

            // check command buffer for commands
            commandBuffer.read((eventTypeId, buffer, index, length) ->
            {
                try
                {
                    switch (eventTypeId)
                    {
                        case ADD_CONSUMER:
                            consumerMessage.wrap(buffer, index);
                            onNewConsumer(consumerMessage.destination(), consumerMessage.channelIds());
                            return;

                        case REMOVE_CONSUMER:
                            consumerMessage.wrap(buffer, index);
                            onRemoveConsumer(consumerMessage.destination(), consumerMessage.channelIds());
                            return;
                    }
                }
                catch (final InvalidDestinationException e)
                {
                    // TODO: log this
                    onError(INVALID_DESTINATION, length);
                }
                catch (final ReceiverNotRegisteredException e)
                {
                    // TODO: log this
                    onError(CONSUMER_NOT_REGISTERED, length);
                }
                catch (final Exception e)
                {
                    // TODO: log this as well as send the error response
                    e.printStackTrace();
                }
            });

            // check AtomicArray for any new buffers created
            RcvBufferState state;
            while ((state = buffers.poll()) != null)
            {
                attachBufferState(state);
            }
        }
        catch (final Exception e)
        {
            e.printStackTrace();
        }
    }

    private void onError(final ErrorCode errorCode, final int length)
    {
        adminThreadCursor.addErrorResponse(errorCode, consumerMessage, length);
    }

    /**
     * Close ReceiverThread down. Returns immediately.
     */
    public void close()
    {
        stop();
        wakeup();

        rcvDestinationMap.forEach((destination, frameHandler) ->
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

    private void onNewConsumer(final String destination, final long[] channelIdList) throws Exception
    {
        final UdpDestination rcvDestination = UdpDestination.parse(destination);
        RcvFrameHandler rcv = rcvDestinationMap.get(rcvDestination);

        if (null == rcv)
        {
            rcv = frameHandlerFactory.newInstance(rcvDestination);
            rcvDestinationMap.put(rcvDestination, rcv);
        }

        rcv.addChannels(channelIdList);
    }

    private void onRemoveConsumer(final String destination, final long[] channelIdList)
    {
        final UdpDestination rcvDestination = UdpDestination.parse(destination);
        RcvFrameHandler rcv = rcvDestinationMap.get(rcvDestination);

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
        RcvFrameHandler rcv = rcvDestinationMap.get(buffer.destination());

        if (null == rcv)
        {
            // should not happen
            // TODO: log this
            return;
        }

        rcv.attachBufferState(buffer);
    }
}
