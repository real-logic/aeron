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
import uk.co.real_logic.aeron.mediadriver.buffer.BufferRotator;
import uk.co.real_logic.aeron.util.*;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.CompletelyIdentifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.command.ConsumerMessageFlyweight;
import uk.co.real_logic.aeron.util.command.LibraryFacade;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.ErrorHeaderFlyweight;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static uk.co.real_logic.aeron.mediadriver.MediaDriver.*;
import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.BASE_HEADER_LENGTH;

/**
 * Admin thread to take commands from Producers and Consumers as well as handle NAKs and retransmissions
 */
public class MediaDriverAdminThread extends ClosableThread implements LibraryFacade
{
    public static final int WRITE_BUFFER_CAPACITY = 256;
    public static final int HEADER_LENGTH = BASE_HEADER_LENGTH + SIZE_OF_INT;

    private final RingBuffer commandBuffer;
    private final ReceiverThreadCursor receiverThreadCursor;
    private final NioSelector nioSelector;
    private final ReceiverThread receiverThread;
    private final SenderThread senderThread;
    private final BufferManagementStrategy bufferManagementStrategy;
    private final RingBuffer adminReceiveBuffer;
    private final RingBuffer adminSendBuffer;
    private final Long2ObjectHashMap<ControlFrameHandler> srcDestinationMap;
    private final AtomicBuffer writeBuffer;
    private final TimerWheel timerWheel;

    private final ByteBuffer toMediaDriver;
    private final ByteBuffer toApi;
    private final Supplier<SenderFlowControlStrategy> senderFlowControl;

    private final ThreadLocalRandom rng = ThreadLocalRandom.current();
    private final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
    private final ConsumerMessageFlyweight receiverMessageFlyweight = new ConsumerMessageFlyweight();
    private final ErrorHeaderFlyweight errorHeaderFlyweight = new ErrorHeaderFlyweight();
    private final CompletelyIdentifiedMessageFlyweight completelyIdentifiedMessageFlyweight =
        new CompletelyIdentifiedMessageFlyweight();
    private final int mtuLength;

    public MediaDriverAdminThread(final Context ctx,
                                  final ReceiverThread receiverThread,
                                  final SenderThread senderThread)
    {
        super(SELECT_TIMEOUT);

        this.commandBuffer = ctx.adminThreadCommandBuffer();
        this.receiverThreadCursor = new ReceiverThreadCursor(ctx.receiverThreadCommandBuffer(), ctx.rcvNioSelector());
        this.bufferManagementStrategy = ctx.bufferManagementStrategy();
        this.nioSelector = ctx.adminNioSelector();
        this.mtuLength = ctx.mtuLength();
        this.receiverThread = receiverThread;
        this.senderThread = senderThread;
        this.senderFlowControl = ctx.senderFlowControl();
        this.srcDestinationMap = new Long2ObjectHashMap<>();
        this.writeBuffer = new AtomicBuffer(ByteBuffer.allocateDirect(WRITE_BUFFER_CAPACITY));
        this.timerWheel = (ctx.adminTimerWheel() != null) ?
                              ctx.adminTimerWheel() :
                              new TimerWheel(ADMIN_THREAD_TICK_DURATION_MICROSECONDS,
                                             TimeUnit.MICROSECONDS,
                                             ADMIN_THREAD_TICKS_PER_WHEEL);

        try
        {
            final AdminBufferStrategy adminBufferStrategy = ctx.adminBufferStrategy();
            this.toMediaDriver = adminBufferStrategy.toMediaDriver();
            this.toApi = adminBufferStrategy.toApi();
            this.adminReceiveBuffer = new ManyToOneRingBuffer(new AtomicBuffer(toMediaDriver));
            this.adminSendBuffer = new ManyToOneRingBuffer(new AtomicBuffer(toApi));
        }
        catch (final Exception ex)
        {
            throw new IllegalStateException("Unable to create the admin media buffers", ex);
        }
    }

    public ControlFrameHandler frameHandler(final UdpDestination destination)
    {
        return srcDestinationMap.get(destination.consistentHash());
    }

    public void process()
    {
        try
        {
            nioSelector.processKeys();
        }
        catch (final Exception ex)
        {
            // TODO: error
            ex.printStackTrace();
        }

        senderThread.processBufferRotation();
        processReceiveBuffer();
        processCommandBuffer();
        processTimers();
    }

    private void processCommandBuffer()
    {
        commandBuffer.read(
              (eventTypeId, buffer, index, length) ->
              {
                  switch (eventTypeId)
                  {
                      case CREATE_CONSUMER_TERM_BUFFER:
                          completelyIdentifiedMessageFlyweight.wrap(buffer, index);
                          onCreateConsumerTermBufferEvent(completelyIdentifiedMessageFlyweight);
                          return;

                      case REMOVE_CONSUMER_TERM_BUFFER:
                          completelyIdentifiedMessageFlyweight.wrap(buffer, index);
                          onRemoveConsumerTermBufferEvent(completelyIdentifiedMessageFlyweight);
                          return;

                      case ERROR_RESPONSE:
                          errorHeaderFlyweight.wrap(buffer, index);
                          adminSendBuffer.write(eventTypeId, buffer, index, length);
                          return;
                  }
              });
    }

    private void processReceiveBuffer()
    {
        adminReceiveBuffer.read(
            (eventTypeId, buffer, index, length) ->
            {
                Flyweight flyweight = channelMessage;

                try
                {
                    switch (eventTypeId)
                    {
                        case ADD_CHANNEL:
                            channelMessage.wrap(buffer, index);
                            flyweight = channelMessage;
                            onAddChannel(channelMessage);
                            return;

                        case REMOVE_CHANNEL:
                            channelMessage.wrap(buffer, index);
                            flyweight = channelMessage;
                            onRemoveChannel(channelMessage);
                            return;

                        case ADD_CONSUMER:
                            receiverMessageFlyweight.wrap(buffer, index);
                            flyweight = receiverMessageFlyweight;
                            onAddConsumer(receiverMessageFlyweight);
                            return;

                        case REMOVE_CONSUMER:
                            receiverMessageFlyweight.wrap(buffer, index);
                            flyweight = receiverMessageFlyweight;
                            onRemoveConsumer(receiverMessageFlyweight);
                            return;
                    }
                }
                catch (final ControlProtocolException ex)
                {
                    final byte[] err = ex.getMessage().getBytes();
                    final int len = ErrorHeaderFlyweight.HEADER_LENGTH + length + err.length;

                    errorHeaderFlyweight.wrap(writeBuffer, 0);
                    errorHeaderFlyweight.errorCode(ex.errorCode())
                                        .offendingFlyweight(flyweight, length)
                                        .errorString(err)
                                        .frameLength(len);

                    adminSendBuffer.write(ERROR_RESPONSE, writeBuffer, 0, errorHeaderFlyweight.frameLength());
                }
                catch (final Exception ex)
                {
                    // TODO: log this instead
                    ex.printStackTrace();
                }
            });
    }

    public void processTimers()
    {
        if (timerWheel.calculateDelayInMsec() <= 0)
        {
            timerWheel.expireTimers();
        }
    }

    public void close()
    {
        stop();
        wakeup();

        srcDestinationMap.forEach(
            (hash, frameHandler) ->
            {
              frameHandler.close();
            });

        IoUtil.unmap((java.nio.MappedByteBuffer)toMediaDriver);
        IoUtil.unmap((java.nio.MappedByteBuffer)toApi);
    }

    public void wakeup()
    {
        // TODO
    }

    /**
     * Return the {@link NioSelector} in use by this admin thread.
     *
     * @return the {@link NioSelector} in use by this admin thread
     */
    public NioSelector nioSelector()
    {
        return nioSelector;
    }

    public TimerWheel.Timer newTimeout(final long delay, final TimeUnit timeUnit, final Runnable task)
    {
        return timerWheel.newTimeout(delay, timeUnit, task);
    }

    public void sendErrorResponse(final int code, final byte[] message)
    {
        // TODO: construct error response for control buffer and write it in
    }

    public void sendError(final int code, final byte[] message)
    {
        // TODO: construct error notification for control buffer and write it in
    }

    public void sendNewBufferNotification(final long sessionId,
                                          final long channelId,
                                          final long termId,
                                          final boolean isSender,
                                          final String destination)
    {
        completelyIdentifiedMessageFlyweight.wrap(writeBuffer, 0);
        completelyIdentifiedMessageFlyweight.sessionId(sessionId)
                                            .channelId(channelId)
                                            .termId(termId)
                                            .destination(destination);

        if (isSender)
        {
            adminSendBuffer.write(NEW_SEND_BUFFER_NOTIFICATION, writeBuffer,
                                  0, completelyIdentifiedMessageFlyweight.length());
        }
        else
        {
            adminSendBuffer.write(NEW_RECEIVE_BUFFER_NOTIFICATION, writeBuffer,
                                  0, completelyIdentifiedMessageFlyweight.length());
        }
    }

    public void onAddChannel(final ChannelMessageFlyweight channelMessage)
    {
        final String destination = channelMessage.destination();
        final long sessionId = channelMessage.sessionId();
        final long channelId = channelMessage.channelId();

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
                    throw new ControlProtocolException(ErrorCode.CHANNEL_ALREADY_EXISTS,
                                                       "destinations hash same, but destinations different");
                }
            }

            SenderChannel channel = frameHandler.findChannel(sessionId, channelId);
            if (null != channel)
            {
                throw new ControlProtocolException(ErrorCode.CHANNEL_ALREADY_EXISTS,
                                                   "channel and session already exist on destination");
            }

            // new channel, so generate "random"-ish termId and create term buffer
            final long initialTermId = rng.nextLong();
            final BufferRotator buffers =
                bufferManagementStrategy.addProducerChannel(srcDestination, sessionId, channelId);

            channel = new SenderChannel(frameHandler,
                                        senderFlowControl.get(),
                                        buffers,
                                        srcDestination,
                                        sessionId,
                                        channelId,
                                        initialTermId,
                                        HEADER_LENGTH,
                                        mtuLength);

            // add channel to frameHandler so it can demux NAKs and SMs
            frameHandler.addChannel(channel);

            // tell the client admin thread of the new buffer
            sendNewBufferNotification(sessionId, channelId, initialTermId, true, destination);

            // add channel to sender thread atomic array so it can be integrated in
            senderThread.addChannel(channel);
        }
        catch (final ControlProtocolException ex)
        {
            throw ex; // rethrow up for handling as normal
        }
        catch (final Exception ex)
        {
            // convert into generic error
            // TODO: log this
            ex.printStackTrace();
            throw new ControlProtocolException(ErrorCode.GENERIC_ERROR_CHANNEL_MESSAGE, ex.getMessage());
        }
    }

    public void onRemoveChannel(final ChannelMessageFlyweight channelMessage)
    {
        final String destination = channelMessage.destination();
        final long sessionId = channelMessage.sessionId();
        final long channelId = channelMessage.channelId();

        try
        {
            final UdpDestination srcDestination = UdpDestination.parse(destination);
            final ControlFrameHandler frameHandler = srcDestinationMap.get(srcDestination.consistentHash());
            if (null == frameHandler)
            {
                throw new ControlProtocolException(ErrorCode.INVALID_DESTINATION, "destination unknown");
            }

            final SenderChannel channel = frameHandler.removeChannel(sessionId, channelId);
            if (null == channel)
            {
                throw new ControlProtocolException(ErrorCode.CHANNEL_UNKNOWN,
                                                   "session and channel unknown for destination");
            }

            // remove from buffer management
            bufferManagementStrategy.removeProducerChannel(srcDestination, sessionId, channelId);

            senderThread.removeChannel(channel);

            // if no more channels, then remove framehandler and close it
            if (frameHandler.numSessions() == 0)
            {
                srcDestinationMap.remove(srcDestination.consistentHash());
                frameHandler.close();
            }
        }
        catch (final ControlProtocolException ex)
        {
            throw ex; // rethrow up for handling as normal
        }
        catch (final Exception ex)
        {
            // convert into generic error
            // TODO: log this
            ex.printStackTrace();
            throw new ControlProtocolException(ErrorCode.GENERIC_ERROR_CHANNEL_MESSAGE, ex.getMessage());
        }
    }

    public void onAddConsumer(final ConsumerMessageFlyweight consumerMessage)
    {
        // instruct receiver thread of new framehandler and new channelIdlist for such
        receiverThreadCursor.addNewConsumerEvent(consumerMessage.destination(), consumerMessage.channelIds());

        // this thread does not add buffers. The RcvFrameHandler handle methods will send an event for this thread
        // to create buffers as needed
    }

    public void onRemoveConsumer(final ConsumerMessageFlyweight consumerMessage)
    {
        // instruct receiver thread to get rid of channels and possibly destination
        receiverThreadCursor.addRemoveReceiverEvent(consumerMessage.destination(), consumerMessage.channelIds());
    }

    public void onRequestTerm(final long sessionId, final long channelId, final long termId)
    {

    }

    private void onCreateConsumerTermBufferEvent(
            final CompletelyIdentifiedMessageFlyweight completelyIdentifiedMessageFlyweight)
    {
        final String destination = completelyIdentifiedMessageFlyweight.destination();
        final long sessionId = completelyIdentifiedMessageFlyweight.sessionId();
        final long channelId = completelyIdentifiedMessageFlyweight.channelId();
        final long termId = completelyIdentifiedMessageFlyweight.termId();

        try
        {
            final UdpDestination rcvDestination = UdpDestination.parse(destination);
            final BufferRotator buffer =
                bufferManagementStrategy.addConsumerChannel(rcvDestination, sessionId, channelId);

            // inform receiver thread of new buffer, destination, etc.
            final RcvBufferState bufferState = new RcvBufferState(rcvDestination, sessionId, channelId, termId, buffer);
            while (!receiverThread.sendBuffer(bufferState))
            {
                // TODO: count errors
            }
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
            // TODO: handle errors by logging
        }
    }

    private void onRemoveConsumerTermBufferEvent(final CompletelyIdentifiedMessageFlyweight message)
    {
        final String destination = completelyIdentifiedMessageFlyweight.destination();
        final long sessionId = completelyIdentifiedMessageFlyweight.sessionId();
        final long channelId = completelyIdentifiedMessageFlyweight.channelId();
        try
        {
            final UdpDestination rcvDestination = UdpDestination.parse(destination);

            bufferManagementStrategy.removeConsumerChannel(rcvDestination, sessionId, channelId);
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
            // TODO: handle errors by logging
        }
    }
}
