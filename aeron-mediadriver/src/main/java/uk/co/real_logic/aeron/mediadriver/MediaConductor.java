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
import uk.co.real_logic.aeron.util.event.EventCode;
import uk.co.real_logic.aeron.util.event.EventLogger;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.ErrorHeaderFlyweight;

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
    public static final EventLogger logger = new EventLogger(MediaConductor.class);

    public static final int MSG_BUFFER_CAPACITY = 4096;
    public static final int HEADER_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;
    public static final int HEARTBEAT_TIMEOUT_MS = 100;

    /**
     * Unicast NAK delay is immediate initial with delayed subsequent delay
     */
    public static final StaticDelayGenerator NAK_UNICAST_DELAY_GENERATOR =
        new StaticDelayGenerator(TimeUnit.MILLISECONDS.toNanos(NAK_UNICAST_DELAY_DEFAULT_NS), true);

    public static final FeedbackDelayGenerator RETRANS_UNICAST_DELAY_GENERATOR =
        () -> RETRANS_UNICAST_DELAY_DEFAULT_NS;
    public static final FeedbackDelayGenerator RETRANS_UNICAST_LINGER_GENERATOR =
        () -> RETRANS_UNICAST_LINGER_DEFAULT_NS;

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

    private final PublisherMessageFlyweight publisherMessage = new PublisherMessageFlyweight();
    private final SubscriberMessageFlyweight subscriberMessage = new SubscriberMessageFlyweight();
    private final ErrorHeaderFlyweight errorHeader = new ErrorHeaderFlyweight();
    private final QualifiedMessageFlyweight qualifiedMessage = new QualifiedMessageFlyweight();
    private final NewBufferMessageFlyweight newBufferMessage = new NewBufferMessageFlyweight();

    private final int mtuLength;
    private final ConductorShmBuffers conductorShmBuffers;
    private final TimerWheel.Timer heartbeatTimer;

    public MediaConductor(final Context ctx, final Receiver receiver, final Sender sender)
    {
        super(AGENT_SLEEP_NS);

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

    public boolean doWork()
    {
        boolean hasDoneWork = false;

        try
        {
            hasDoneWork = nioSelector.processKeys();
        }
        catch (final Exception ex)
        {
            // TODO: error
            ex.printStackTrace();
        }

        hasDoneWork |= sender.processBufferRotation();
        hasDoneWork |= receiver.processBufferRotation();
        hasDoneWork |= receiver.scanForGaps();

        hasDoneWork |= processClientCommandBuffer();
        hasDoneWork |= processLocalCommandBuffer();
        hasDoneWork |= processTimers();

        return hasDoneWork;
    }

    public void close()
    {
        stop();

        srcDestinationMap.forEach((hash, frameHandler) -> frameHandler.close());

        conductorShmBuffers.close();
    }

    private boolean processLocalCommandBuffer()
    {
        final int messagesRead = localCommandBuffer.read(
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

        return messagesRead > 0;
    }

    private boolean processClientCommandBuffer()
    {
        final int messagesRead = clientCommandBuffer.read(
            (msgTypeId, buffer, index, length) ->
            {
                Flyweight flyweight = publisherMessage;

                try
                {
                    switch (msgTypeId)
                    {
                        case ADD_CHANNEL:
                            publisherMessage.wrap(buffer, index);
                            logger.emit(EventCode.CMD_IN_ADD_CHANNEL, buffer, index, length);
                            flyweight = publisherMessage;
                            onAddChannel(publisherMessage);
                            break;

                        case REMOVE_CHANNEL:
                            publisherMessage.wrap(buffer, index);
                            logger.emit(EventCode.CMD_IN_REMOVE_CHANNEL, buffer, index, length);
                            flyweight = publisherMessage;
                            onRemoveChannel(publisherMessage);
                            break;

                        case ADD_SUBSCRIBER:
                            subscriberMessage.wrap(buffer, index);
                            logger.emit(EventCode.CMD_IN_ADD_SUBSCRIBER, buffer, index, length);
                            flyweight = subscriberMessage;
                            onAddSubscriber(subscriberMessage);
                            break;

                        case REMOVE_SUBSCRIBER:
                            subscriberMessage.wrap(buffer, index);
                            logger.emit(EventCode.CMD_IN_REMOVE_SUBSCRIBER, buffer, index, length);
                            flyweight = subscriberMessage;
                            onRemoveSubscriber(subscriberMessage);
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

        return messagesRead > 0;
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

    private boolean processTimers()
    {
        return timerWheel.calculateDelayInMs() <= 0 && timerWheel.expireTimers();
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

        logger.emit((isSender ? EventCode.CMD_OUT_NEW_SEND_BUFFER_NOTIFICATION :
                        EventCode.CMD_OUT_NEW_RECEIVE_BUFFER_NOTIFICATION),
                msgBuffer, 0, newBufferMessage.length());

        if (!toClientBuffer.write(msgTypeId, msgBuffer, 0, newBufferMessage.length()))
        {
            System.err.println("Error occurred writing new buffer notification");
        }
    }

    private void onAddChannel(final PublisherMessageFlyweight publisherMessage)
    {
        final String destination = publisherMessage.destination();
        final long sessionId = publisherMessage.sessionId();
        final long channelId = publisherMessage.channelId();

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

            final long initialTermId = generateTermId();
            final BufferRotator buffers =
                bufferManagement.addPublisherChannel(srcDestination, sessionId, channelId);

            channel = new SenderChannel(frameHandler,
                                        timerWheel,
                                        senderFlowControl.get(),
                                        buffers,
                                        sessionId,
                                        channelId,
                                        initialTermId,
                                        HEADER_LENGTH,
                                        mtuLength,
                                        frameHandler::sendTo);

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

    private void onRemoveChannel(final PublisherMessageFlyweight publisherMessage)
    {
        final String destination = publisherMessage.destination();
        final long sessionId = publisherMessage.sessionId();
        final long channelId = publisherMessage.channelId();

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

    private long generateTermId()
    {
        // term Id can be psuedo-random. Doesn't have to be perfect. But must be in the range.
        return (int)(Math.random() * (double)0x7FFFFFFF);
    }
}
