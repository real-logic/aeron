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

import uk.co.real_logic.aeron.mediadriver.buffer.BufferManagementStrategy;
import uk.co.real_logic.aeron.util.ClosableThread;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.ControlProtocolEvents;
import uk.co.real_logic.aeron.util.command.ErrorCode;
import uk.co.real_logic.aeron.util.command.LibraryFacade;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.ErrorHeaderFlyweight;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * Admin thread to take commands from Producers and Consumers as well as handle NAKs and retransmissions
 */
public class MediaDriverAdminThread extends ClosableThread implements LibraryFacade
{

    private final RingBuffer commandBuffer;
    private final ReceiverThreadCursor receiverThreadCursor;
    private final NioSelector nioSelector;
    private final ReceiverThread receiverThread;
    private final SenderThread senderThread;
    private final BufferManagementStrategy bufferManagementStrategy;
    private final RingBuffer adminReceiveBuffer;
    private final Long2ObjectHashMap<ControlFrameHandler> srcDestinationMap;
    private final Supplier<SenderFlowControlStrategy> senderFlowControl;

    private final ThreadLocalRandom rng = ThreadLocalRandom.current();

    private final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
    private final ErrorHeaderFlyweight errorHeaderFlyweight = new ErrorHeaderFlyweight();

    public MediaDriverAdminThread(final MediaDriver.TopologyBuilder builder,
                                  final ReceiverThread receiverThread,
                                  final SenderThread senderThread)
    {
        this.commandBuffer = builder.adminThreadCommandBuffer();
        this.receiverThreadCursor = new ReceiverThreadCursor(builder.receiverThreadCommandBuffer(), receiverThread);
        this.bufferManagementStrategy = builder.bufferManagementStrategy();
        this.nioSelector = builder.adminNioSelector();
        this.receiverThread = receiverThread;
        this.senderThread = senderThread;
        this.senderFlowControl = builder.senderFlowControl();
        this.srcDestinationMap = new Long2ObjectHashMap<>();

        try
        {
            final ByteBuffer buffer = builder.adminBufferStrategy().toMediaDriver();
            this.adminReceiveBuffer = new ManyToOneRingBuffer(new AtomicBuffer(buffer));
        }
        catch (Exception e)
        {
            throw new IllegalStateException("Unable to create the admin media buffers", e);
        }
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
        try
        {
            nioSelector.processKeys(MediaDriver.SELECT_TIMEOUT);
        }
        catch (Exception e)
        {
            // TODO: error
            e.printStackTrace();
        }

        adminReceiveBuffer.read((eventTypeId, buffer, index, length) ->
        {
            // TODO: call onAddChannel, etc.

            switch (eventTypeId)
            {
                case ControlProtocolEvents.ADD_CHANNEL:
                    channelMessage.wrap(buffer, index);
                    onAddChannel(channelMessage.destination(),
                                 channelMessage.sessionId(),
                                 channelMessage.channelId());
                    return;
                case ControlProtocolEvents.REMOVE_CHANNEL:
                    channelMessage.wrap(buffer, index);
                    onRemoveChannel(channelMessage.destination(),
                                    channelMessage.sessionId(),
                                    channelMessage.channelId());
                    return;
            }
        });
        // TODO: read from commandBuffer and dispatch to onNakEvent, etc.
    }

    @Override
    public void close()
    {
        stop();
        wakeup();
    }

    public void wakeup()
    {
        nioSelector.wakeup();
    }

    /**
     * Return the {@link uk.co.real_logic.aeron.mediadriver.NioSelector} in use by this admin thread.
     *
     * @return the {@link uk.co.real_logic.aeron.mediadriver.NioSelector} in use by this admin thread
     */
    public NioSelector nioSelector()
    {
        return nioSelector;
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
                                          final boolean isSender,
                                          final String destination)
    {

    }

    @Override
    public void onAddChannel(final String destination, final long sessionId, final long channelId)
    {
        // TODO: to accommodate error handling, probably need to pass in Flyweight itself...
        try
        {
            final UdpDestination srcDestination = UdpDestination.parse(destination);
            ControlFrameHandler frameHandler = srcDestinationMap.get(srcDestination.consistentHash());
            if (null == frameHandler)
            {
                frameHandler = new ControlFrameHandler(srcDestination, this);
                srcDestinationMap.put(srcDestination.consistentHash(), frameHandler);
            }
            else
            {
                // check for hash collision
                if (!frameHandler.destination().equals(srcDestination))
                {
                    throw new IllegalStateException("destinations hash same, but destinations different");
                }
            }

            SenderChannel channel = frameHandler.findChannel(sessionId, channelId);
            if (null != channel)
            {
                throw new IllegalArgumentException("channel and session already exist on destination");
            }

            // new channel, so generate "random"-ish termId and create term buffer
            final long initialTermId = rng.nextLong();
            final ByteBuffer buffer = bufferManagementStrategy.addSenderTerm(srcDestination, sessionId,
                    channelId, initialTermId);

            channel = new SenderChannel(frameHandler,
                                        senderFlowControl.get(),
                                        buffer,
                                        srcDestination,
                                        sessionId,
                                        channelId,
                                        initialTermId);

            // add channel to SrcFrameHandler so it can demux NAKs and SMs
            frameHandler.addChannel(channel);

            // tell the client admin thread of the new buffer
            sendNewBufferNotification(sessionId, channelId, initialTermId, true, destination);

            // add channel to sender thread atomic array so it can be integrated in
            senderThread.addChannel(channel);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            sendErrorResponse(ErrorCode.GENERIC_ERROR.value(), e.getMessage().getBytes());
            // TODO: log this as well as send the error response
        }
    }

    @Override
    public void onRemoveChannel(final String destination, final long sessionId, final long channelId)
    {
        // TODO: to accommodate error handling, probably need to pass in Flyweight itself...
        try
        {
            final UdpDestination srcDestination = UdpDestination.parse(destination);
            final ControlFrameHandler frameHandler = srcDestinationMap.get(srcDestination.consistentHash());
            if (null == frameHandler)
            {
                throw new IllegalArgumentException("destination unknown");
            }

            final SenderChannel channel = frameHandler.removeChannel(sessionId, channelId);
            if (null == channel)
            {
                throw new IllegalArgumentException("session and channel unknown for destination");
            }

            // remove from buffer management
            bufferManagementStrategy.removeSenderChannel(srcDestination, sessionId, channelId);

            senderThread.removeChannel(channel);

            // if no more channels, then remove framehandler and close it
            if (frameHandler.numSessions() == 0)
            {
                srcDestinationMap.remove(srcDestination.consistentHash());
                frameHandler.close();
            }
        }
        catch (Exception e)
        {
            // TODO: log this as well as send the error response
            e.printStackTrace();
            sendErrorResponse(ErrorCode.GENERIC_ERROR.value(), e.getMessage().getBytes());
        }
    }

    @Override
    public void onRemoveTerm(final String destination, final long sessionId, final long channelId, final long termId)
    {
        try
        {
            final UdpDestination srcDestination = UdpDestination.parse(destination);
            final ControlFrameHandler frameHandler = srcDestinationMap.get(srcDestination.consistentHash());
            if (null == frameHandler)
            {
                throw new IllegalArgumentException("destination unknown");
            }

            final SenderChannel channel = frameHandler.findChannel(sessionId, channelId);
            if (channel == null)
            {
                throw new IllegalArgumentException("session and channel unknown for destination");
            }

            // remove from buffer management, but will be unmapped once SenderThread releases it and it can be GCed
            bufferManagementStrategy.removeSenderTerm(srcDestination, sessionId, channelId, termId);

            // TODO: sender thread only uses current term, so if we are removing the current term
            // TODO: inform SenderChannel to get rid of term
            // TODO: adding/removing of terms to SenderChannel should be serialized by this thread
            // TODO: just need to know when to remove from SrcFrameHandler (thus removing NAKs/SMs) and SenderChannel
            // TODO: inform SenderThread as this could be a term it is using (handle like onRemoveChannel?)

            // if no more channels, then remove framehandler and close it
            if (frameHandler.numSessions() == 0)
            {
                srcDestinationMap.remove(srcDestination.consistentHash());
                frameHandler.close();
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
        receiverThreadCursor.addNewReceiverEvent(destination, channelIdList);

        // this thread does not add buffers. The RcvFrameHandler handle methods will send an event for this thread
        // to create buffers as needed
    }

    @Override
    public void onRemoveReceiver(final String destination, final long[] channelIdList)
    {
        // instruct receiver thread to get rid of channels and possibly destination
        receiverThreadCursor.addRemoveReceiverEvent(destination, channelIdList);
    }

    @Override
    public void onRequestTerm(final long sessionId, final long channelId, final long termId)
    {

    }

    private void onRcvCreateNewTermBufferEvent(final String destination,
                                               final long sessionId,
                                               final long channelId,
                                               final long termId)
    {
        try
        {
            final UdpDestination rcvDestination = UdpDestination.parse(destination);
            final ByteBuffer buffer = bufferManagementStrategy.addReceiverTerm(rcvDestination, sessionId,
                                                                               channelId, termId);

            // inform receiver thread of new buffer, destination, etc.
            receiverThread.addBuffer(new RcvBufferState(rcvDestination, sessionId, channelId, termId, buffer));
        }
        catch (Exception e)
        {
            // TODO: handle errors by logging
        }
    }
}
