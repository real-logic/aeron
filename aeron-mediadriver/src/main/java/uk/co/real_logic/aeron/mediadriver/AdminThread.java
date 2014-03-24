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
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Admin thread to take commands from Producers and Consumers as well as handle NAKs and retransmissions
 */
public class AdminThread extends ClosableThread implements LibraryFacade
{
    private final Map<UdpDestination, SrcFrameHandler> srcDestinationMap = new HashMap<>();
    private final Map<UdpDestination, RcvFrameHandler> rcvDestinationMap = new HashMap<>();
    private final ReceiverThread receiverThread;
    private final SenderThread senderThread;
    private final ByteBuffer commandBuffer;
    private final BufferManagementStrategy bufferManagementStrategy;
    private final Map<Long, Map<Long, ByteBuffer>> termBufferMap = new Long2ObjectHashMap<>();

    public AdminThread(final ByteBuffer commandBuffer,
                       final ReceiverThread receiverThread,
                       final SenderThread senderThread)
    {
        this.receiverThread = receiverThread;
        this.senderThread = senderThread;
        this.commandBuffer = commandBuffer;
        this.bufferManagementStrategy = null;
    }

    /**
     * Add NAK frame to command buffer for this thread
     * @param header for the NAK frame
     */
    public void offerNAK(final HeaderFlyweight header)
    {
        // TODO: add NAK frame to this threads command buffer
    }

    public void process()
    {
        // TODO: read from control buffer and call onAddChannel, etc.
        // TODO: read from commandBuffer and dispatch to onNAK, etc.
    }

    public void sendStatusMessage(final HeaderFlyweight header)
    {
        // TODO: send SM on through to control buffer
    }

    public void sendErrorResponse(final int code, final byte[] message)
    {
        // TODO: construct error response for control buffer and write it in
    }

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

    public void onAddChannel(final String destination, final long sessionId, final long channelId)
    {
        try
        {
            final UdpDestination srcDestination = UdpDestination.parse(destination);
            final long termId = (long)(Math.random() * 0xFFFFFFFFL);  // FIXME: this may not be correct
            SrcFrameHandler src = srcDestinationMap.get(srcDestination);

            if (null == src)
            {
                src = new SrcFrameHandler(srcDestination, receiverThread, this, senderThread);
                srcDestinationMap.put(srcDestination, src);
            }

            // TODO: look in the termBufferMap for channelId, then sessionId, then termId
            // TODO: must error check if new channel or not. And sessionId collision

            final ByteBuffer termBuffer = bufferManagementStrategy.addSenderTerm(sessionId, channelId, termId);

            // send command to SenderThread to start reading from new termBuffer and to send via SrcFrameHandler
            senderThread.offerNewSenderTerm(sessionId, channelId, termId, termBuffer);

        }
        catch (Exception e)
        {
            sendErrorResponse(ErrorCode.GENERIC_ERROR.value(), e.getMessage().getBytes());
            // TODO: log this as well as send the error response
        }
    }

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

            // TODO: only remove if no more channels.
            //srcDestinationMap.remove(srcDestination);
        }
        catch (Exception e)
        {
            sendErrorResponse(ErrorCode.GENERIC_ERROR.value(), e.getMessage().getBytes());
            // TODO: log this as well as send the error response
        }
    }

    public void onRemoveTerm(final String destination, final long sessionId, final long channelId, final long termId)
    {

    }

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

            // this thread does not add buffers. The RcvFrameHandler handle methods will create new buffers on demand
        }
        catch (Exception e)
        {
            sendErrorResponse(ErrorCode.GENERIC_ERROR.value(), e.getMessage().getBytes());
            // TODO: log this as well as send the error response
        }
    }

    public void onRemoveReceiver(final String destination)
    {

    }

    @Override
    public void onRequestTerm(final long sessionId, final long channelId, final long termId)
    {

    }

    /**
     * Handling of NAKs as they come in via the command buffer from the SrcFrameHandler
     * @param header
     */
    public void onNAK(final HeaderFlyweight header)
    {
        // TODO: handle the NAK.
    }
}
