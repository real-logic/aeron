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

import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.ClosableThread;
import uk.co.real_logic.aeron.util.command.ControlProtocolEvents;
import uk.co.real_logic.aeron.util.command.ReceiverMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.util.HashMap;
import java.util.Map;

/**
 * Receiver Thread for JVM based mediadriver, uses an event loop with command buffer
 */
public class ReceiverThread extends ClosableThread
{
    private final RingBuffer commandBuffer;
    private final NioSelector nioSelector;
    private final MediaDriverAdminThreadCursor mediaDriverAdminThreadCursor;
    private final Map<UdpDestination, RcvFrameHandler> rcvDestinationMap = new HashMap<>();
    private final ReceiverMessageFlyweight receiverMessage;
    private final AtomicArray<RcvBufferState> buffers;

    public ReceiverThread(final MediaDriver.TopologyBuilder builder) throws Exception
    {
        this.commandBuffer = builder.receiverThreadCommandBuffer();
        this.mediaDriverAdminThreadCursor = new MediaDriverAdminThreadCursor(builder.adminThreadCommandBuffer(),
                                                                             builder.adminNioSelector());
        this.nioSelector = builder.rcvNioSelector();
        this.receiverMessage = new ReceiverMessageFlyweight();
        this.buffers = new AtomicArray<>();
    }

    public void process()
    {
        try
        {
            nioSelector.processKeys(MediaDriver.SELECT_TIMEOUT);

            // check command buffer for commands
            commandBuffer.read((eventTypeId, buffer, index, length) ->
            {
                switch (eventTypeId)
                {
                    case ControlProtocolEvents.ADD_RECEIVER:
                        receiverMessage.wrap(buffer, index);
                        onNewReceiverEvent(receiverMessage.destination(), receiverMessage.channelIds());
                        return;

                    case ControlProtocolEvents.REMOVE_RECEIVER:
                        receiverMessage.wrap(buffer, index);
                        onRemoveReceiverEvent(receiverMessage.destination(), receiverMessage.channelIds());
                        return;
                }
            });

            // check AtomicArray for any new buffers created
            if (buffers.changedSinceLastMark())
            {
                buffers.forEach(buffer ->
                {
                    if (buffer.state() == RcvBufferState.STATE_PENDING)
                    {
                        // attach buffer to rcvDestinationMap and then appropriate RcvFrameHandler
                        attachBufferState(buffer);
                        buffer.state(RcvBufferState.STATE_READY);
                    }
                });
                buffers.mark();
            }
        }
        catch (final Exception e)
        {
            e.printStackTrace();
        }
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
        nioSelector.wakeup();
    }

    /**
     * Return the {@link uk.co.real_logic.aeron.mediadriver.NioSelector} in use by the thread
     * @return the {@link uk.co.real_logic.aeron.mediadriver.NioSelector} in use by the thread
     */
    public NioSelector nioSelector()
    {
        return nioSelector;
    }

    public void addBuffer(final RcvBufferState buffer)
    {
        buffers.add(buffer);
    }

    public void removeBuffer(final RcvBufferState buffer)
    {
        buffers.remove(buffer);
    }

    public RcvFrameHandler frameHandler(final UdpDestination destination)
    {
        return rcvDestinationMap.get(destination);
    }

    private void onNewReceiverEvent(final String destination, final long[] channelIdList)
    {
        try
        {
            final UdpDestination rcvDestination = UdpDestination.parse(destination);
            RcvFrameHandler rcv = rcvDestinationMap.get(rcvDestination);

            if (null == rcv)
            {
                rcv = new RcvFrameHandler(rcvDestination, nioSelector, mediaDriverAdminThreadCursor);
                rcvDestinationMap.put(rcvDestination, rcv);
            }

            rcv.addChannels(channelIdList);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            // TODO: AdminThread.sendErrorResponse(ErrorCode.GENERIC_ERROR.value(), e.getMessage().getBytes());
            // TODO: log this as well as send the error response
        }
    }

    private void onRemoveReceiverEvent(final String destination, final long[] channelIdList)
    {
        try
        {
            final UdpDestination rcvDestination = UdpDestination.parse(destination);
            RcvFrameHandler rcv = rcvDestinationMap.get(rcvDestination);

            if (null == rcv)
            {
                throw new IllegalArgumentException("destination unknown for receiver remove: " + destination);
            }

            rcv.removeChannels(channelIdList);

            // if all channels gone, then take care of removing everything and closing the framehandler
            if (0 == rcv.channelCount())
            {
                rcvDestinationMap.remove(rcvDestination);
                rcv.close();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            // TODO: AdminThread.sendErrorResponse(ErrorCode.GENERIC_ERROR.value(), e.getMessage().getBytes());
            // TODO: log this as well as send the error response
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
