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

import uk.co.real_logic.aeron.mediadriver.buffer.BufferManagement;
import uk.co.real_logic.aeron.mediadriver.buffer.BufferRotator;
import uk.co.real_logic.aeron.util.*;
import uk.co.real_logic.aeron.util.collections.Long2ObjectHashMap;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.ClientFacade;
import uk.co.real_logic.aeron.util.command.QualifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.command.SubscriberMessageFlyweight;
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
 * Admin thread to take commands from Publishers and Subscribers as well as handle NAKs and retransmissions
 */
public class MediaConductor extends Agent implements ClientFacade
{
    public static final int WRITE_BUFFER_CAPACITY = 256;
    public static final int HEADER_LENGTH = BASE_HEADER_LENGTH + SIZE_OF_INT;

    public static final int HEARTBEAT_TIMEOUT_MILLISECONDS = 100;

    private final RingBuffer commandBuffer;
    private final ReceiverCursor receiverCursor;
    private final NioSelector nioSelector;
    private final Receiver receiver;
    private final Sender sender;
    private final BufferManagement bufferManagement;
    private final RingBuffer adminReceiveBuffer;
    private final RingBuffer adminSendBuffer;
    private final Long2ObjectHashMap<ControlFrameHandler> srcDestinationMap;
    private final AtomicBuffer writeBuffer;
    private final TimerWheel timerWheel;

    private final Supplier<SenderControlStrategy> senderFlowControl;

    private final ThreadLocalRandom rng = ThreadLocalRandom.current();
    private final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
    private final SubscriberMessageFlyweight receiverMessageFlyweight = new SubscriberMessageFlyweight();
    private final ErrorHeaderFlyweight errorHeaderFlyweight = new ErrorHeaderFlyweight();
    private final QualifiedMessageFlyweight qualifiedMessageFlyweight = new QualifiedMessageFlyweight();

    private final int mtuLength;
    private final ConductorByteBuffers adminBufferStrategy;
    private TimerWheel.Timer heartbeatTimer;

    public MediaConductor(final Context ctx, final Receiver receiver, final Sender sender)
    {
        super(SELECT_TIMEOUT);

        this.commandBuffer = ctx.conductorCommandBuffer();
        this.receiverCursor = new ReceiverCursor(ctx.receiverCommandBuffer(), ctx.rcvNioSelector());
        this.bufferManagement = ctx.bufferManagement();
        this.nioSelector = ctx.adminNioSelector();
        this.mtuLength = ctx.mtuLength();
        this.receiver = receiver;
        this.sender = sender;
        this.senderFlowControl = ctx.senderFlowControl();
        this.srcDestinationMap = new Long2ObjectHashMap<>();
        this.writeBuffer = new AtomicBuffer(ByteBuffer.allocateDirect(WRITE_BUFFER_CAPACITY));
        this.timerWheel = ctx.conductorTimerWheel() != null ?
                              ctx.conductorTimerWheel() :
                              new TimerWheel(MEDIA_CONDUCTOR_TICK_DURATION_MICROS,
                                             TimeUnit.MICROSECONDS,
                                             MEDIA_CONDUCTOR_TICKS_PER_WHEEL);

        heartbeatTimer = newTimeout(HEARTBEAT_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS, this::onHeartbeatCheck);

        try
        {
            adminBufferStrategy = ctx.conductorByteBuffers();
            ByteBuffer toMediaDriver = adminBufferStrategy.toMediaDriver();
            ByteBuffer toApi = adminBufferStrategy.toClient();
            this.adminReceiveBuffer = new ManyToOneRingBuffer(new AtomicBuffer(toMediaDriver));
            this.adminSendBuffer = new ManyToOneRingBuffer(new AtomicBuffer(toApi));
        }
        catch (final Exception ex)
        {
            throw new IllegalStateException("Unable to create the conductor media buffers", ex);
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

        sender.processBufferRotation();
        receiver.processBufferRotation();
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
                      case CREATE_TERM_BUFFER:
                          qualifiedMessageFlyweight.wrap(buffer, index);
                          onCreateSubscriberTermBufferEvent(qualifiedMessageFlyweight);
                          return;

                      case REMOVE_TERM_BUFFER:
                          qualifiedMessageFlyweight.wrap(buffer, index);
                          onRemoveSubscriberTermBufferEvent(qualifiedMessageFlyweight);
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

                        case ADD_SUBSCRIBER:
                            receiverMessageFlyweight.wrap(buffer, index);
                            flyweight = receiverMessageFlyweight;
                            onAddSubscriber(receiverMessageFlyweight);
                            return;

                        case REMOVE_SUBSCRIBER:
                            receiverMessageFlyweight.wrap(buffer, index);
                            flyweight = receiverMessageFlyweight;
                            onRemoveSubscriber(receiverMessageFlyweight);
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
        if (timerWheel.calculateDelayInMs() <= 0)
        {
            timerWheel.expireTimers();
        }
    }

    public void close()
    {
        stop();
        wakeup();

        srcDestinationMap.forEach((hash, frameHandler) -> frameHandler.close());

        adminBufferStrategy.close();
    }

    public void wakeup()
    {
        // TODO
    }

    /**
     * Return the {@link NioSelector} in use by this conductor thread.
     *
     * @return the {@link NioSelector} in use by this conductor thread
     */
    public NioSelector nioSelector()
    {
        return nioSelector;
    }

    public TimerWheel.Timer newTimeout(final long delay, final TimeUnit timeUnit, final Runnable task)
    {
        return timerWheel.newTimeout(delay, timeUnit, task);
    }

    public void rescheduleTimeout(final long delay, final TimeUnit timeUnit, final TimerWheel.Timer timer)
    {
        timerWheel.rescheduleTimeout(delay, timeUnit, timer);
    }

    public long currentTime()
    {
        return timerWheel.now();
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
        qualifiedMessageFlyweight.wrap(writeBuffer, 0);
        qualifiedMessageFlyweight.sessionId(sessionId)
                                            .channelId(channelId)
                                            .termId(termId)
                                            .destination(destination);

        if (isSender)
        {
            adminSendBuffer.write(NEW_SEND_BUFFER_NOTIFICATION, writeBuffer,
                                  0, qualifiedMessageFlyweight.length());
        }
        else
        {
            adminSendBuffer.write(NEW_RECEIVE_BUFFER_NOTIFICATION, writeBuffer,
                                  0, qualifiedMessageFlyweight.length());
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
                bufferManagement.addPublisherChannel(srcDestination, sessionId, channelId);

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

            // tell the client conductor thread of the new buffer
            sendNewBufferNotification(sessionId, channelId, initialTermId, true, destination);

            // add channel to sender thread atomic array so it can be integrated in
            sender.addChannel(channel);
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
            bufferManagement.removePublisherChannel(srcDestination, sessionId, channelId);

            sender.removeChannel(channel);

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

    public void onAddSubscriber(final SubscriberMessageFlyweight subscriberMessage)
    {
        // instruct receiver thread of new framehandler and new channelIdlist for such
        receiverCursor.addNewSubscriberEvent(subscriberMessage.destination(), subscriberMessage.channelIds());

        // this thread does not add buffers. The RcvFrameHandler handle methods will send an event for this thread
        // to create buffers as needed
    }

    public void onRemoveSubscriber(final SubscriberMessageFlyweight subscriberMessage)
    {
        // instruct receiver thread to get rid of channels and possibly destination
        receiverCursor.addRemoveSubscriberEvent(subscriberMessage.destination(), subscriberMessage.channelIds());
    }

    public void onRequestTerm(final long sessionId, final long channelId, final long termId)
    {
    }

    private void onCreateSubscriberTermBufferEvent(
        final QualifiedMessageFlyweight qualifiedMessageFlyweight)
    {
        final String destination = qualifiedMessageFlyweight.destination();
        final long sessionId = qualifiedMessageFlyweight.sessionId();
        final long channelId = qualifiedMessageFlyweight.channelId();
        final long termId = qualifiedMessageFlyweight.termId();

        try
        {
            final UdpDestination rcvDestination = UdpDestination.parse(destination);
            final BufferRotator buffer =
                bufferManagement.addSubscriberChannel(rcvDestination, sessionId, channelId);

            // inform receiver thread of new buffer, destination, etc.
            final RcvBufferState bufferState = new RcvBufferState(rcvDestination, sessionId, channelId, termId, buffer);
            while (!receiver.sendBuffer(bufferState))
            {
                // TODO: count errors
                System.out.println("Error adding to buffer");
            }
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
            // TODO: handle errors by logging
        }
    }

    private void onRemoveSubscriberTermBufferEvent(final QualifiedMessageFlyweight message)
    {
        final String destination = qualifiedMessageFlyweight.destination();
        final long sessionId = qualifiedMessageFlyweight.sessionId();
        final long channelId = qualifiedMessageFlyweight.channelId();
        try
        {
            final UdpDestination rcvDestination = UdpDestination.parse(destination);

            bufferManagement.removeSubscriberChannel(rcvDestination, sessionId, channelId);
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
            // TODO: handle errors by logging
        }
    }

    private void onHeartbeatCheck()
    {
        sender.heartbeatChecks();

        rescheduleTimeout(HEARTBEAT_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS, heartbeatTimer);
    }
}
