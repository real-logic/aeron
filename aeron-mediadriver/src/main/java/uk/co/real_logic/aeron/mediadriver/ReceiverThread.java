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
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.util.HashMap;
import java.util.Map;

/**
 * Receiver Thread for JVM based mediadriver, uses an event loop with command buffer
 */
public class ReceiverThread extends ClosableThread
{
    private final RingBuffer commandBuffer;
    private final RingBuffer adminThreadCommandBuffer;
    private final NioSelector nioSelector;
    private final Map<UdpDestination, RcvFrameHandler> rcvDestinationMap = new HashMap<>();

    public ReceiverThread(final MediaDriver.TopologyBuilder builder) throws Exception
    {
        this.commandBuffer = builder.receiverThreadCommandBuffer();
        this.adminThreadCommandBuffer = builder.adminThreadCommandBuffer();
        this.nioSelector = builder.rcvNioSelector();
    }

    public void addRcvCreateTermBufferEvent(final UdpDestination destination,
                                            final long sessionId,
                                            final long channelId,
                                            final long termId)
    {
        // send on to admin thread
        MediaDriverAdminThread.addRcvCreateNewTermBufferEvent(adminThreadCommandBuffer, destination,
                                                              sessionId, channelId, termId);
    }

    @Override
    public void process()
    {
        try
        {
            nioSelector.processKeys(MediaDriver.SELECT_TIMEOUT);
            // TODO: check command buffer for commands
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
        // TODO: if needed, use a CountdownLatch to sync...
    }

    /**
     * Wake up the selector if blocked
     */
    public void wakeup()
    {
        nioSelector.wakeup();
    }

    private void onNewReceiverEvent(final String destination, final long[] channelIdList)
    {
        try
        {
            final UdpDestination rcvDestination = UdpDestination.parse(destination);
            RcvFrameHandler rcv = rcvDestinationMap.get(rcvDestination);

            if (null == rcv)
            {
                rcv = new RcvFrameHandler(rcvDestination, nioSelector);
                rcvDestinationMap.put(rcvDestination, rcv);
            }

            rcv.addChannels(channelIdList);
        }
        catch (Exception e)
        {
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
            // TODO: AdminThread.sendErrorResponse(ErrorCode.GENERIC_ERROR.value(), e.getMessage().getBytes());
            // TODO: log this as well as send the error response
        }
    }

}
