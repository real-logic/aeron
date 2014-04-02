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

import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.util.HashMap;
import java.util.Map;

/**
 * Receiver Thread for JVM based mediadriver, uses an event loop with command buffer.
 *
 * Contains Selector logic
 *
 * Does not provide timers
 */
public class ReceiverThread implements AutoCloseable, Runnable
{
    private volatile boolean done;
    private final RingBuffer commandBuffer;
    private final RingBuffer adminThreadCommandBuffer;
    private final NioSelector nioSelector;
    private final Map<UdpDestination, RcvFrameHandler> rcvDestinationMap = new HashMap<>();

    public ReceiverThread(final MediaDriver.TopologyBuilder builder) throws Exception
    {
        this.commandBuffer = builder.receiverThreadCommandBuffer();
        this.adminThreadCommandBuffer = builder.adminThreadCommandBuffer();
        this.nioSelector = builder.rcvNioSelector();
        this.done = false;
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

    /**
     * Main loop of the ReceiverThread
     *
     * Everything is done here and bubbles up via the handlers.
     */
    public void run()
    {
        try
        {
            while (!done)
            {
                nioSelector.processKeys(MediaDriver.SELECT_TIMEOUT);
                // TODO: check command buffer for commands
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
        done = true;
        nioSelector.wakeup();
        // TODO: if needed, use a CountdownLatch to sync...
    }

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
