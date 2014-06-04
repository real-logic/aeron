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
import uk.co.real_logic.aeron.util.command.*;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.ErrorHeaderFlyweight;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.nio.ByteBuffer.allocateDirect;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.*;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;

/**
 * Media Conductor to take commands from Publishers and Subscribers as well as handle NAKs and retransmissions
 */
public class MediaConductor extends Agent
{
    public static final int WRITE_BUFFER_CAPACITY = 512;
    public static final int HEADER_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;
    public static final int HEARTBEAT_TIMEOUT_MS = 100;

    private final RingBuffer mediaCommandBuffer;
    private final ReceiverProxy receiverProxy;
    private final NioSelector nioSelector;
    private final Receiver receiver;
    private final Sender sender;
    private final BufferManagement bufferManagement;
    private final RingBuffer toMediaDriverBuffer;
    private final RingBuffer toClientBuffer;
    private final Long2ObjectHashMap<ControlFrameHandler> srcDestinationMap = new Long2ObjectHashMap<>();
    private final AtomicBuffer writeBuffer = new AtomicBuffer(allocateDirect(WRITE_BUFFER_CAPACITY));
    private final TimerWheel timerWheel;

    private final Supplier<SenderControlStrategy> senderFlowControl;

    private final ThreadLocalRandom rng = ThreadLocalRandom.current();
    private final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
    private final SubscriberMessageFlyweight receiverMessage = new SubscriberMessageFlyweight();
    private final ErrorHeaderFlyweight errorHeader = new ErrorHeaderFlyweight();
    private final QualifiedMessageFlyweight qualifiedMessage = new QualifiedMessageFlyweight();
    private final NewBufferMessageFlyweight newBufferMessage = new NewBufferMessageFlyweight();

    private final int mtuLength;
    private final ConductorByteBuffers conductorByteBuffers;
    private final TimerWheel.Timer heartbeatTimer;

    public MediaConductor(final Context ctx, final Receiver receiver, final Sender sender)
    {
        super(SELECT_TIMEOUT);

        this.mediaCommandBuffer = ctx.mediaCommandBuffer();
        this.receiverProxy = new ReceiverProxy(ctx.receiverCommandBuffer(), ctx.receiverNioSelector());
        this.bufferManagement = ctx.bufferManagement();
        this.nioSelector = ctx.conductorNioSelector();
        this.mtuLength = ctx.mtuLength();
        this.receiver = receiver;
        this.sender = sender;
        this.senderFlowControl = ctx.senderFlowControl();

        newBufferMessage.wrap(writeBuffer, 0);

        timerWheel = ctx.conductorTimerWheel() != null ?
            ctx.conductorTimerWheel() :
            new TimerWheel(MEDIA_CONDUCTOR_TICK_DURATION_US,
                           TimeUnit.MICROSECONDS,
                           MEDIA_CONDUCTOR_TICKS_PER_WHEEL);

        heartbeatTimer = newTimeout(HEARTBEAT_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onHeartbeatCheck);

        conductorByteBuffers = ctx.conductorByteBuffers();
        toMediaDriverBuffer = new ManyToOneRingBuffer(new AtomicBuffer(conductorByteBuffers.toMediaDriver()));
        toClientBuffer = new ManyToOneRingBuffer(new AtomicBuffer(conductorByteBuffers.toClient()));
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
        mediaCommandBuffer.read(
            (eventTypeId, buffer, index, length) ->
            {
                switch (eventTypeId)
                {
                    case CREATE_TERM_BUFFER:
                        qualifiedMessage.wrap(buffer, index);
                        onCreateSubscriberTermBufferEvent(qualifiedMessage);
                        break;

                    case REMOVE_TERM_BUFFER:
                        qualifiedMessage.wrap(buffer, index);
                        onRemoveSubscriberTermBufferEvent(qualifiedMessage);
                        break;

                    case ERROR_RESPONSE:
                        errorHeader.wrap(buffer, index);
                        toClientBuffer.write(eventTypeId, buffer, index, length);
                        break;
                }
            });
    }

    private void processReceiveBuffer()
    {
        toMediaDriverBuffer.read(
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
                            break;

                        case REMOVE_CHANNEL:
                            channelMessage.wrap(buffer, index);
                            flyweight = channelMessage;
                            onRemoveChannel(channelMessage);
                            break;

                        case ADD_SUBSCRIBER:
                            receiverMessage.wrap(buffer, index);
                            flyweight = receiverMessage;
                            onAddSubscriber(receiverMessage);
                            break;

                        case REMOVE_SUBSCRIBER:
                            receiverMessage.wrap(buffer, index);
                            flyweight = receiverMessage;
                            onRemoveSubscriber(receiverMessage);
                            break;
                    }
                }
                catch (final ControlProtocolException ex)
                {
                    final byte[] err = ex.getMessage().getBytes();
                    final int len = ErrorHeaderFlyweight.HEADER_LENGTH + length + err.length;

                    errorHeader.wrap(writeBuffer, 0);
                    errorHeader.errorCode(ex.errorCode())
                               .offendingFlyweight(flyweight, length)
                               .errorString(err)
                               .frameLength(len);

                    toClientBuffer.write(ERROR_RESPONSE, writeBuffer, 0, errorHeader.frameLength());
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

        conductorByteBuffers.close();
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

    public void sendNewBufferNotification(final long sessionId,
                                          final long channelId,
                                          final long termId,
                                          final boolean isSender,
                                          final String destination,
                                          final BufferRotator buffers)
    {
        newBufferMessage.sessionId(sessionId)
                        .channelId(channelId)
                        .termId(termId);
        buffers.bufferInformation(newBufferMessage);
        newBufferMessage.destination(destination);

        final int eventTypeId = isSender ? NEW_SEND_BUFFER_NOTIFICATION : NEW_RECEIVE_BUFFER_NOTIFICATION;

        if (!toClientBuffer.write(eventTypeId, writeBuffer, 0, newBufferMessage.length()))
        {
            System.err.println("Error occurred writing new buffer notification");
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
            sendNewBufferNotification(sessionId, channelId, initialTermId, true, destination, buffers);

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
        receiverProxy.addNewSubscriberEvent(subscriberMessage.destination(), subscriberMessage.channelIds());

        // this thread does not add buffers. The RcvFrameHandler handle methods will send an event for this thread
        // to create buffers as needed
    }

    public void onRemoveSubscriber(final SubscriberMessageFlyweight subscriberMessage)
    {
        // instruct receiver thread to get rid of channels and possibly destination
        receiverProxy.addRemoveSubscriberEvent(subscriberMessage.destination(), subscriberMessage.channelIds());
    }

    private void onCreateSubscriberTermBufferEvent(final QualifiedMessageFlyweight qualifiedMessage)
    {
        final String destination = qualifiedMessage.destination();
        final long sessionId = qualifiedMessage.sessionId();
        final long channelId = qualifiedMessage.channelId();
        final long termId = qualifiedMessage.termId();

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

    private void onRemoveSubscriberTermBufferEvent(final QualifiedMessageFlyweight qualifiedMessage)
    {
        final String destination = qualifiedMessage.destination();
        final long sessionId = qualifiedMessage.sessionId();
        final long channelId = qualifiedMessage.channelId();

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

        rescheduleTimeout(HEARTBEAT_TIMEOUT_MS, TimeUnit.MILLISECONDS, heartbeatTimer);
    }
}
