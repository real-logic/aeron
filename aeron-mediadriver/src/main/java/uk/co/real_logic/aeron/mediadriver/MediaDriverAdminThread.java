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
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.util.command.ErrorCode;
import uk.co.real_logic.aeron.util.command.LibraryFacade;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Admin thread to take commands from Producers and Consumers as well as handle NAKs and retransmissions
 */
public class MediaDriverAdminThread extends ClosableThread implements LibraryFacade
{
    private final Map<UdpDestination, SrcFrameHandler> srcDestinationMap = new HashMap<>();
    private final Map<UdpDestination, RcvFrameHandler> rcvDestinationMap = new HashMap<>();
    private final RingBuffer commandBuffer;
    private final RingBuffer senderThreadCommandBuffer;
    private final ReceiverThread receiverThread;
    private final SenderThread senderThread;
    private final BufferManagementStrategy bufferManagementStrategy;
    private final Map<Long, Map<Long, ByteBuffer>> termBufferMap = new Long2ObjectHashMap<>();

    public MediaDriverAdminThread(final MediaDriver.TopologyBuilder builder,
                                  final ReceiverThread receiverThread,
                                  final SenderThread senderThread)
    {
        this.commandBuffer = builder.adminThreadCommandBuffer();
        this.senderThreadCommandBuffer = builder.senderThreadCommandBuffer();
        this.bufferManagementStrategy = builder.bufferManagementStrategy();
        this.receiverThread = receiverThread;
        this.senderThread = senderThread;
    }

    /**
     * Add NAK frame to command buffer for this thread
     * @param header for the NAK frame
     */
    public static void addNakEvent(final RingBuffer buffer, final HeaderFlyweight header)
    {
        // TODO: add NAK frame to this threads command buffer
    }

    @Override
    public void process()
    {
        // TODO: read from control buffer and call onAddChannel, etc.
        // TODO: read from commandBuffer and dispatch to onNakEvent, etc.
    }

    @Override
    public void sendStatusMessage(final HeaderFlyweight header)
    {
        // TODO: send SM on through to control buffer
    }

    @Override
    public void sendErrorResponse(final int code, final byte[] message)
    {
        // TODO: construct error response for control buffer and write it in
    }

    @Override
    public void sendError(final int code, final byte[] message)
    {
        // TODO: construct error notification for control buffer and write it in
    }

    @Override
    public void sendNewBufferNotification(final long sessionId,
                                          final long channelId,
                                          final long termId,
                                          final boolean isSender)
    {

    }

    @Override
    public void onAddChannel(final String destination, final long sessionId, final long channelId)
    {
        try
        {
            final UdpDestination srcDestination = UdpDestination.parse(destination);
            final long termId = (long)(Math.random() * 0xFFFFFFFFL);  // FIXME: this may not be correct
            SrcFrameHandler src = srcDestinationMap.get(srcDestination);

            if (null == src)
            {
                src = new SrcFrameHandler(srcDestination, receiverThread, commandBuffer, senderThreadCommandBuffer);
                srcDestinationMap.put(srcDestination, src);
            }

            // create the buffer, but hold onto it in the strategy. The senderThread will do a lookup on it
            bufferManagementStrategy.addSenderTerm(sessionId, channelId, termId);

            // tell senderThread about the new buffer that was created
            SenderThread.addNewTermEvent(senderThreadCommandBuffer, sessionId, channelId, termId);

            // tell the client admin thread of the new buffer
            sendNewBufferNotification(sessionId, channelId, termId, true);
        }
        catch (Exception e)
        {
            sendErrorResponse(ErrorCode.GENERIC_ERROR.value(), e.getMessage().getBytes());
            // TODO: log this as well as send the error response
        }
    }

    @Override
    public void onRemoveChannel(final String destination, final long sessionId, final long channelId)
    {
        try
        {
            final UdpDestination srcDestination = UdpDestination.parse(destination);
            final SrcFrameHandler src = srcDestinationMap.get(srcDestination);

            if (null == src)
            {
                throw new IllegalArgumentException("destination unknown for channel remove: " + destination);
            }

            // remove from buffer management, but will be unmapped once SenderThread releases it and it can be GCed
            bufferManagementStrategy.removeSenderChannel(sessionId, channelId);

            // inform SenderThread
            SenderThread.addRemoveChannelEvent(senderThreadCommandBuffer, sessionId, channelId);

            // if no more channels, then remove framehandler and close it
            if (0 == bufferManagementStrategy.countChannels(sessionId))
            {
                srcDestinationMap.remove(srcDestination);
                src.close();
            }
        }
        catch (Exception e)
        {
            sendErrorResponse(ErrorCode.GENERIC_ERROR.value(), e.getMessage().getBytes());
            // TODO: log this as well as send the error response
        }
    }

    @Override
    public void onRemoveTerm(final String destination, final long sessionId, final long channelId, final long termId)
    {
        try
        {
            final UdpDestination srcDestination = UdpDestination.parse(destination);
            final SrcFrameHandler src = srcDestinationMap.get(srcDestination);

            if (null == src)
            {
                throw new IllegalArgumentException("destination unknown for term remove: " + destination);
            }

            // remove from buffer management, but will be unmapped once SenderThread releases it and it can be GCed
            bufferManagementStrategy.removeSenderTerm(sessionId, channelId, 0);

            // inform SenderThread
            SenderThread.addRemoveTermEvent(senderThreadCommandBuffer, sessionId, channelId, termId);

            // if no more channels, then remove framehandler and close it
            if (0 == bufferManagementStrategy.countChannels(sessionId))
            {
                srcDestinationMap.remove(srcDestination);
                src.close();
            }
        }
        catch (Exception e)
        {
            sendErrorResponse(ErrorCode.GENERIC_ERROR.value(), e.getMessage().getBytes());
            // TODO: log this as well as send the error response
        }
    }

    @Override
    public void onAddReceiver(final String destination, final long[] channelIdList)
    {
        try
        {
            final UdpDestination rcvDestination = UdpDestination.parse(destination);
            RcvFrameHandler rcv = rcvDestinationMap.get(rcvDestination);

            if (null == rcv)
            {
                rcv = new RcvFrameHandler(rcvDestination, receiverThread, channelIdList);
                rcvDestinationMap.put(rcvDestination, rcv);
            }
            else
            {
                // TODO: add new channels to an existing RcvFrameHandler
                // - need to do this via command queue to that running thread
            }

            // this thread does not add buffers. The RcvFrameHandler handle methods will send an event for this thread
            // to create buffers as needed
        }
        catch (Exception e)
        {
            sendErrorResponse(ErrorCode.GENERIC_ERROR.value(), e.getMessage().getBytes());
            // TODO: log this as well as send the error response
        }
    }

    @Override
    public void onRemoveReceiver(final String destination)
    {
        try
        {
            final UdpDestination rcvDestination = UdpDestination.parse(destination);
            RcvFrameHandler rcv = rcvDestinationMap.get(rcvDestination);

            if (null == rcv)
            {
                throw new IllegalArgumentException("destination unknown for receiver remove: " + destination);
            }

            rcvDestinationMap.remove(rcvDestination);

            // TODO: send event to receiver thread to remove (and close) this framehandler
        }
        catch (Exception e)
        {
            sendErrorResponse(ErrorCode.GENERIC_ERROR.value(), e.getMessage().getBytes());
            // TODO: log this as well as send the error response
        }
    }

    @Override
    public void onRequestTerm(final long sessionId, final long channelId, final long termId)
    {

    }

    private void onNakEvent(final HeaderFlyweight header)
    {
        // TODO: handle the NAK.
    }
}
