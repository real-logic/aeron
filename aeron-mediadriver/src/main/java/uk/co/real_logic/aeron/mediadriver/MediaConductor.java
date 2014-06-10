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
    public static final int MSG_BUFFER_CAPACITY = 4096;
    public static final int HEADER_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;
    public static final int HEARTBEAT_TIMEOUT_MS = 100;

    private final RingBuffer localCommandBuffer;
    private final ReceiverProxy receiverProxy;
    private final NioSelector nioSelector;
    private final Receiver receiver;
    private final Sender sender;
    private final BufferManagement bufferManagement;
    private final RingBuffer clientCommandBuffer;
    private final RingBuffer toClientBuffer;
    private final Long2ObjectHashMap<ControlFrameHandler> srcDestinationMap = new Long2ObjectHashMap<>();
    private final AtomicBuffer msgBuffer = new AtomicBuffer(allocateDirect(MSG_BUFFER_CAPACITY));
    private final TimerWheel timerWheel;

    private final Supplier<SenderControlStrategy> senderFlowControl;

    private final ThreadLocalRandom rng = ThreadLocalRandom.current();
    private final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
    private final SubscriberMessageFlyweight receiverMessage = new SubscriberMessageFlyweight();
    private final ErrorHeaderFlyweight errorHeader = new ErrorHeaderFlyweight();
    private final QualifiedMessageFlyweight qualifiedMessage = new QualifiedMessageFlyweight();
    private final NewBufferMessageFlyweight newBufferMessage = new NewBufferMessageFlyweight();

    private final int mtuLength;
    private final ConductorShmBuffers conductorShmBuffers;
    private final TimerWheel.Timer heartbeatTimer;

    public MediaConductor(final Context ctx, final Receiver receiver, final Sender sender)
    {
        super(SELECT_TIMEOUT);

        this.localCommandBuffer = ctx.mediaCommandBuffer();
        this.receiverProxy = ctx.receiverProxy();
        this.bufferManagement = ctx.bufferManagement();
        this.nioSelector = ctx.conductorNioSelector();
        this.mtuLength = ctx.mtuLength();
        this.receiver = receiver;
        this.sender = sender;
        this.senderFlowControl = ctx.senderFlowControl();

        timerWheel = ctx.conductorTimerWheel();
        heartbeatTimer = newTimeout(HEARTBEAT_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onHeartbeatCheck);

        conductorShmBuffers = ctx.conductorShmBuffers();
        clientCommandBuffer = new ManyToOneRingBuffer(new AtomicBuffer(conductorShmBuffers.toDriver()));
        toClientBuffer = new ManyToOneRingBuffer(new AtomicBuffer(conductorShmBuffers.toClient()));

        newBufferMessage.wrap(msgBuffer, 0);
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
        processClientCommandBuffer();
        processCommandBuffer();
        processTimers();
    }

    public void close()
    {
        stop();
        wakeup();

        srcDestinationMap.forEach((hash, frameHandler) -> frameHandler.close());

        conductorShmBuffers.close();
    }

    public void wakeup()
    {
        // TODO
    }

    private void processCommandBuffer()
    {
        localCommandBuffer.read(
            (msgTypeId, buffer, index, length) ->
            {
                switch (msgTypeId)
                {
                    case CREATE_TERM_BUFFER:
                        qualifiedMessage.wrap(buffer, index);
                        onCreateSubscriberTermBuffer(qualifiedMessage);
                        break;

                    case REMOVE_TERM_BUFFER:
                        qualifiedMessage.wrap(buffer, index);
                        onRemoveSubscriberTermBuffer(qualifiedMessage);
                        break;

                    case ERROR_RESPONSE:
                        errorHeader.wrap(buffer, index);
                        toClientBuffer.write(msgTypeId, buffer, index, length);
                        break;
                }
            });
    }

    private void processClientCommandBuffer()
    {
        clientCommandBuffer.read(
            (msgTypeId, buffer, index, length) ->
            {
                Flyweight flyweight = channelMessage;

                try
                {
                    switch (msgTypeId)
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

                    errorHeader.wrap(msgBuffer, 0);
                    errorHeader.errorCode(ex.errorCode())
                               .offendingFlyweight(flyweight, length)
                               .errorString(err)
                               .frameLength(len);

                    toClientBuffer.write(ERROR_RESPONSE, msgBuffer, 0, errorHeader.frameLength());
                }
                catch (final Exception ex)
                {
                    // TODO: log this instead
                    ex.printStackTrace();
                }
            });
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

    public long currentTime()
    {
        return timerWheel.now();
    }

    private void processTimers()
    {
        if (timerWheel.calculateDelayInMs() <= 0)
        {
            timerWheel.expireTimers();
        }
    }

    private TimerWheel.Timer newTimeout(final long delay, final TimeUnit timeUnit, final Runnable task)
    {
        return timerWheel.newTimeout(delay, timeUnit, task);
    }

    private void rescheduleTimeout(final long delay, final TimeUnit timeUnit, final TimerWheel.Timer timer)
    {
        timerWheel.rescheduleTimeout(delay, timeUnit, timer);
    }

    private void sendNewBufferNotification(final long sessionId,
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

        final int msgTypeId = isSender ? NEW_SEND_BUFFER_NOTIFICATION : NEW_RECEIVE_BUFFER_NOTIFICATION;

        if (!toClientBuffer.write(msgTypeId, msgBuffer, 0, newBufferMessage.length()))
        {
            System.err.println("Error occurred writing new buffer notification");
        }
    }

    private void onAddChannel(final ChannelMessageFlyweight channelMessage)
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
            else if (!frameHandler.destination().equals(srcDestination))
            {
                throw new ControlProtocolException(ErrorCode.CHANNEL_ALREADY_EXISTS,
                                                   "destinations hash same, but destinations different");
            }

            SenderChannel channel = frameHandler.findChannel(sessionId, channelId);
            if (null != channel)
            {
                throw new ControlProtocolException(ErrorCode.CHANNEL_ALREADY_EXISTS,
                                                   "channel and session already exist on destination");
            }

            final long initialTermId = rng.nextLong();
            final BufferRotator buffers =
                bufferManagement.addPublisherChannel(srcDestination, sessionId, channelId);

            channel = new SenderChannel(frameHandler,
                                        senderFlowControl.get(),
                                        buffers,
                                        sessionId,
                                        channelId,
                                        initialTermId,
                                        HEADER_LENGTH,
                                        mtuLength,
                                        frameHandler::sendTo,
                                        timerWheel::now
                    );

            frameHandler.addChannel(channel);
            sendNewBufferNotification(sessionId, channelId, initialTermId, true, destination, buffers);
            sender.addChannel(channel);
        }
        catch (final ControlProtocolException ex)
        {
            throw ex;
        }
        catch (final Exception ex)
        {
            // TODO: log this
            ex.printStackTrace();
            throw new ControlProtocolException(ErrorCode.GENERIC_ERROR_CHANNEL_MESSAGE, ex.getMessage());
        }
    }

    private void onRemoveChannel(final ChannelMessageFlyweight channelMessage)
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

            bufferManagement.removePublisherChannel(srcDestination, sessionId, channelId);

            sender.removeChannel(channel);

            if (frameHandler.numSessions() == 0)
            {
                srcDestinationMap.remove(srcDestination.consistentHash());
                frameHandler.close();
            }
        }
        catch (final ControlProtocolException ex)
        {
            throw ex;
        }
        catch (final Exception ex)
        {
            // TODO: log this
            ex.printStackTrace();
            throw new ControlProtocolException(ErrorCode.GENERIC_ERROR_CHANNEL_MESSAGE, ex.getMessage());
        }
    }

    private void onAddSubscriber(final SubscriberMessageFlyweight subscriberMessage)
    {
        receiverProxy.newSubscriber(subscriberMessage.destination(), subscriberMessage.channelIds());
    }

    private void onRemoveSubscriber(final SubscriberMessageFlyweight subscriberMessage)
    {
        receiverProxy.removeSubscriber(subscriberMessage.destination(), subscriberMessage.channelIds());
    }

    private void onCreateSubscriberTermBuffer(final QualifiedMessageFlyweight qualifiedMessage)
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

            sendNewBufferNotification(sessionId, channelId, termId, false, destination, buffer);

            final NewReceiveBufferEvent event =
                new NewReceiveBufferEvent(rcvDestination, sessionId, channelId, termId, buffer);
            while (!receiverProxy.newReceiveBuffer(event))
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

    private void onRemoveSubscriberTermBuffer(final QualifiedMessageFlyweight qualifiedMessage)
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
