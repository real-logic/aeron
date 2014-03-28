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
import uk.co.real_logic.aeron.util.command.ErrorCode;
import uk.co.real_logic.aeron.util.command.LibraryFacade;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.util.HashMap;
import java.util.Map;

/**
 * Admin thread to take commands from Producers and Consumers as well as handle NAKs and retransmissions
 */
public class MediaDriverAdminThread extends ClosableThread implements LibraryFacade
{
    private final Map<UdpDestination, SrcFrameHandler> srcDestinationMap = new HashMap<>();
    private final RingBuffer commandBuffer;
    private final RingBuffer senderThreadCommandBuffer;
    private final RingBuffer receiverThreadCommandBuffer;
    private final ReceiverThread receiverThread;
    private final SenderThread senderThread;
    private final BufferManagementStrategy bufferManagementStrategy;
    private final RingBuffer adminReceiveBuffer;

    public MediaDriverAdminThread(final MediaDriver.TopologyBuilder builder,
                                  final ReceiverThread receiverThread,
                                  final SenderThread senderThread)
    {
        this.commandBuffer = builder.adminThreadCommandBuffer();
        this.senderThreadCommandBuffer = builder.senderThreadCommandBuffer();
        this.receiverThreadCommandBuffer = builder.receiverThreadCommandBuffer();
        this.bufferManagementStrategy = builder.bufferManagementStrategy();
        this.receiverThread = receiverThread;
        this.senderThread = senderThread;
        try
        {
            this.adminReceiveBuffer = new ManyToOneRingBuffer(new AtomicBuffer(builder.adminBufferStrategy().toMediaDriver()));
        }
        catch (Exception e)
        {
            throw new IllegalStateException("Unable to create the admin media buffers", e);
        }
    }

    /**
     * Add NAK frame to command buffer for this thread
     * @param header for the NAK frame
     */
    public static void addNakEvent(final RingBuffer buffer, final HeaderFlyweight header)
    {
        // TODO: add NAK frame to this threads command buffer
    }

    public static void addRcvCreateNewTermBufferEvent(final RingBuffer buffer,
                                                      final UdpDestination destination,
                                                      final long sessionId,
                                                      final long channelId,
                                                      final long termId)
    {
        // TODO: add event to command buffer
    }

    @Override
    public void process()
    {
        adminReceiveBuffer.read((eventTypeId, buffer, index, length) ->
        {
            // TODO: call onAddChannel, etc.
        });
        // TODO: read from commandBuffer and dispatch to onNakEvent, etc.
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
                                          final boolean isSender, final String destination)
    {

    }

    @Override
    public void onAddChannel(final String destination, final long sessionId, final long channelId)
    {
        try
        {
            final UdpDestination srcDestination = UdpDestination.parse(destination);
            final long termId = (long)(Math.random() * 0xFFFFFFFFL);  // FIXME: this may not be random enough
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
            sendNewBufferNotification(sessionId, channelId, termId, true, destination);
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
        // instruct receiver thread of new framehandler and new channelIdlist for such
        ReceiverThread.addNewReceiverEvent(receiverThreadCommandBuffer, destination, channelIdList);

        // this thread does not add buffers. The RcvFrameHandler handle methods will send an event for this thread
        // to create buffers as needed
    }

    @Override
    public void onRemoveReceiver(final String destination, final long[] channelIdList)
    {
        // instruct receiver thread to get rid of channels and destination
        ReceiverThread.addRemoveReceiverEvent(receiverThreadCommandBuffer, destination, channelIdList);
    }

    @Override
    public void onRequestTerm(final long sessionId, final long channelId, final long termId)
    {

    }

    private void onNakEvent(final HeaderFlyweight header)
    {
        // TODO: handle the NAK.
    }

    private void onRcvCreateNewTermBufferEvent(final String destination,
                                               final long sessionId,
                                               final long channelId,
                                               final long termId)
    {
        // TODO: create new buffer via strategy, then instruct the receiver thread that we are done and it can grab it

        ReceiverThread.addTermBufferCreatedEvent(receiverThreadCommandBuffer, sessionId, channelId, termId);
    }
}
