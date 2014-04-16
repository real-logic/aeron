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
package uk.co.real_logic.aeron.admin;

import uk.co.real_logic.aeron.Channel;
import uk.co.real_logic.aeron.ProducerControlFactory;
import uk.co.real_logic.aeron.ReceiverChannel;
import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.ClosableThread;
import uk.co.real_logic.aeron.util.collections.ChannelMap;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.CompletelyIdentifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.command.MediaDriverFacade;
import uk.co.real_logic.aeron.util.command.ReceiverMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;


/**
 * Admin thread to take responses and notifications from mediadriver and act on them. As well as pass commands
 * to the mediadriver.
 */
public final class ClientAdminThread extends ClosableThread implements MediaDriverFacade
{
    /** Maximum size of the write buffer */
    public static final int WRITE_BUFFER_CAPACITY = 256;
    /** Incoming message buffer from media driver */
    private final RingBuffer recvBuffer;
    /** Incoming message buffer from other Core threads */
    private final RingBuffer commandBuffer;
    /** Outgoing message buffer to media driver */
    private final RingBuffer sendBuffer;

    private final BufferUsageStrategy bufferUsage;
    private final AtomicArray<Channel> channels;
    private final AtomicArray<ReceiverChannel> receivers;
    private final AdminErrorHandler errorHandler;
    private final ProducerControlFactory producerControl;
    private final ChannelMap<String, Channel> sendNotifiers;
    private final ReceiverMap recvNotifiers;

    /** Atomic buffer to write message flyweights into before they get sent */
    private final AtomicBuffer writeBuffer = new AtomicBuffer(ByteBuffer.allocate(WRITE_BUFFER_CAPACITY));

    // Control protocol Flyweights
    private final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
    private final ReceiverMessageFlyweight receiverMessage = new ReceiverMessageFlyweight();
    private final CompletelyIdentifiedMessageFlyweight requestTermMessage = new CompletelyIdentifiedMessageFlyweight();

    private final CompletelyIdentifiedMessageFlyweight bufferNotificationMessage = new CompletelyIdentifiedMessageFlyweight();

    public ClientAdminThread(final RingBuffer commandBuffer,
                             final RingBuffer recvBuffer,
                             final RingBuffer sendBuffer,
                             final BufferUsageStrategy bufferUsage,
                             final AtomicArray<Channel> channels,
                             final AtomicArray<ReceiverChannel> receivers,
                             final AdminErrorHandler errorHandler,
                             final ProducerControlFactory producerControl)
    {
        this.commandBuffer = commandBuffer;
        this.recvBuffer = recvBuffer;
        this.sendBuffer = sendBuffer;
        this.bufferUsage = bufferUsage;
        this.channels = channels;
        this.receivers = receivers;
        this.errorHandler = errorHandler;
        this.producerControl = producerControl;
        this.sendNotifiers = new ChannelMap<>();
        this.recvNotifiers = new ReceiverMap();

        channelMessage.wrap(writeBuffer, 0);
        receiverMessage.wrap(writeBuffer, 0);
        requestTermMessage.wrap(writeBuffer, 0);
    }

    public void process()
    {
        handleReceiveBuffer();
        handleCommandBuffer();
    }

    private void handleCommandBuffer()
    {
        commandBuffer.read((eventTypeId, buffer, index, length) ->
        {
            switch (eventTypeId)
            {
                case ADD_CHANNEL:
                case REMOVE_CHANNEL:
                {
                    channelMessage.wrap(buffer, index);
                    final String destination = channelMessage.destination();
                    final long channelId = channelMessage.channelId();
                    final long sessionId = channelMessage.sessionId();
                    if (eventTypeId == ADD_CHANNEL)
                    {
                        addSender(destination, channelId, sessionId);
                    }
                    else
                    {
                        removeSender(destination, channelId, sessionId);
                    }
                    sendBuffer.write(eventTypeId, buffer, index, length);
                    return;
                }
                case ADD_RECEIVER:
                case REMOVE_RECEIVER:
                {
                    receiverMessage.wrap(buffer, index);
                    final long[] channelIds = receiverMessage.channelIds();
                    final String destination = receiverMessage.destination();
                    if (eventTypeId == ADD_RECEIVER)
                    {
                        addReceiver(destination, channelIds);
                    }
                    else
                    {
                        removeReceiver(destination, channelIds);
                    }
                    sendBuffer.write(eventTypeId, buffer, index, length);
                    return;
                }
            }
        });
    }

    private void addReceiver(final String destination, final long[] channelIds)
    {
        // Not efficient but only happens once per channel ever
        // and is during setup not a latency critical path
        for (final long channelId : channelIds)
        {
            receivers.forEach(receiver ->
            {
                if (receiver.matches(destination, channelId))
                {
                    recvNotifiers.put(destination, channelId, receiver);
                }
            });
        }
    }

    private void removeReceiver(final String destination, final long[] channelIds)
    {
        for (final long channelId : channelIds)
        {
            recvNotifiers.remove(destination, channelId);
        }
    }

    private void addSender(final String destination, final long channelId, final long sessionId)
    {
        // see addReceiver re efficiency
        channels.forEach(channel ->
        {
            if (channel.matches(destination, sessionId, channelId))
            {
                sendNotifiers.put(destination, sessionId, channelId, channel);
            }
        });
    }

    private void removeSender(final String destination, final long channelId, final long sessionId)
    {
        if (sendNotifiers.remove(destination, channelId, sessionId) == null)
        {
            // TODO: log an error
        }
    }

    private void handleReceiveBuffer()
    {
        recvBuffer.read((eventTypeId, buffer, index, length) ->
        {
            switch (eventTypeId)
            {
                case NEW_RECEIVE_BUFFER_NOTIFICATION:
                case NEW_SEND_BUFFER_NOTIFICATION:
                    bufferNotificationMessage.wrap(buffer, index);
                    final boolean isSender = eventTypeId == NEW_SEND_BUFFER_NOTIFICATION;
                    onNewBufferNotification(bufferNotificationMessage.sessionId(),
                                            bufferNotificationMessage.channelId(),
                                            bufferNotificationMessage.termId(),
                                            isSender,
                                            bufferNotificationMessage.destination());
                    return;
                case ERROR_RESPONSE:
                    errorHandler.onErrorResponse(buffer, index, length);
                    return;
            }
        });
    }

    /* commands to MediaDriver */

    public void sendAddChannel(final String destination, final long sessionId, final long channelId)
    {

    }

    public void sendRemoveChannel(final String destination, final long sessionId, final long channelId)
    {

    }

    public void sendRemoveTerm(final String destination,
                               final long sessionId,
                               final long channelId,
                               final long termId)
    {

    }

    public void sendAddReceiver(final String destination, final long[] channelIdList)
    {

    }

    public void sendRemoveReceiver(final String destination, final long[] channelIdList)
    {

    }

    public void sendRequestTerm(final long sessionId, final long channelId, final long termId)
    {

    }

    /* callbacks from MediaDriver */

    public void onErrorResponse(final int code, final byte[] message)
    {
    }

    public void onError(final int code, final byte[] message)
    {
    }

    public void onNewBufferNotification(final long sessionId,
                                        final long channelId,
                                        final long termId,
                                        final boolean isSender,
                                        final String destination)
    {
        try
        {
            ChannelNotifiable channel = isSender ? sendNotifiers.get(destination, sessionId, channelId)
                                                 : recvNotifiers.get(destination, channelId);
            if (channel == null)
            {
                // The new buffer refers to another client process,
                // We can safely ignore it
                return;
            }

            // TODO: fix the appender adding

            final ByteBuffer buffer = bufferUsage.onTermAdded(destination, sessionId, channelId, termId, isSender);
            channel.newTermBufferMapped(termId, buffer);
        }
        catch (Exception e)
        {
            // TODO: establish correct client error handling strategy
            e.printStackTrace();
        }
    }
}