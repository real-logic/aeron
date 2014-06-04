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
package uk.co.real_logic.aeron.conductor;

import uk.co.real_logic.aeron.Channel;
import uk.co.real_logic.aeron.PublisherControlFactory;
import uk.co.real_logic.aeron.SubscriberChannel;
import uk.co.real_logic.aeron.util.Agent;
import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.collections.ChannelMap;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.NewBufferMessageFlyweight;
import uk.co.real_logic.aeron.util.command.SubscriberMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.status.PositionIndicator;
import uk.co.real_logic.aeron.util.status.PositionReporter;
import uk.co.real_logic.aeron.util.status.StatusBufferMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;

import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.BUFFER_COUNT;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.BASE_HEADER_LENGTH;

/**
 * Client conductor takes responses and notifications from media driver and acts on them. As well as pass commands
 * to the media driver.
 */
public final class ClientConductor extends Agent
{
    /**
     * Maximum size of the write buffer.
     */
    public static final int WRITE_BUFFER_CAPACITY = 256;

    // TODO: DI this
    private static final byte[] DEFAULT_HEADER = new byte[BASE_HEADER_LENGTH + SIZE_OF_INT];
    private static final int MAX_FRAME_LENGTH = 1024;
    private static final int SLEEP_PERIOD_MS = 1;

    private final RingBuffer clientCommandBuffer;
    private final RingBuffer fromMediaDriverBuffer;
    private final RingBuffer toMediaDriverBuffer;

    private final BufferUsageStrategy bufferUsage;
    private final AtomicArray<Channel> publishers;
    private final AtomicArray<SubscriberChannel> subscriberChannels;

    private final ChannelMap<String, Channel> sendNotifiers = new ChannelMap<>();
    private final SubscriberMap rcvNotifiers = new SubscriberMap();
    private final StatusBufferMapper statusCounters = new StatusBufferMapper();

    private final ConductorErrorHandler errorHandler;
    private final PublisherControlFactory publisherControlFactory;

    private final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
    private final SubscriberMessageFlyweight receiverMessage = new SubscriberMessageFlyweight();
    private final NewBufferMessageFlyweight bufferNotificationMessage = new NewBufferMessageFlyweight();

    public ClientConductor(final RingBuffer clientCommandBuffer,
                           final RingBuffer toMediaDriverBuffer,
                           final RingBuffer fromMediaDriverBuffer,
                           final BufferUsageStrategy bufferUsage,
                           final AtomicArray<Channel> publishers,
                           final AtomicArray<SubscriberChannel> subscriberChannels,
                           final ConductorErrorHandler errorHandler,
                           final PublisherControlFactory publisherControlFactory)
    {
        super(SLEEP_PERIOD_MS);

        this.clientCommandBuffer = clientCommandBuffer;
        this.fromMediaDriverBuffer = fromMediaDriverBuffer;
        this.toMediaDriverBuffer = toMediaDriverBuffer;
        this.bufferUsage = bufferUsage;
        this.publishers = publishers;
        this.subscriberChannels = subscriberChannels;
        this.errorHandler = errorHandler;
        this.publisherControlFactory = publisherControlFactory;

        final AtomicBuffer writeBuffer = new AtomicBuffer(ByteBuffer.allocate(WRITE_BUFFER_CAPACITY));
        channelMessage.wrap(writeBuffer, 0);
        receiverMessage.wrap(writeBuffer, 0);
    }

    public void process()
    {
        handleClientCommandBuffer();
        handleMessagesFromMediaDriver();
        processBufferCleaningScan();
    }

    public void close()
    {
        bufferUsage.close();
        statusCounters.close();
    }

    private void processBufferCleaningScan()
    {
        publishers.forEach(Channel::processBufferScan);
        subscriberChannels.forEach(SubscriberChannel::processBufferScan);
    }

    private void handleClientCommandBuffer()
    {
        clientCommandBuffer.read(
            (eventTypeId, buffer, index, length) ->
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
                            addPublisher(destination, channelId, sessionId);
                        }
                        else
                        {
                            removePublisher(destination, channelId, sessionId);
                        }

                        toMediaDriverBuffer.write(eventTypeId, buffer, index, length);
                        break;
                    }

                    case ADD_SUBSCRIBER:
                    case REMOVE_SUBSCRIBER:
                    {
                        receiverMessage.wrap(buffer, index);
                        final long[] channelIds = receiverMessage.channelIds();
                        final String destination = receiverMessage.destination();
                        if (eventTypeId == ADD_SUBSCRIBER)
                        {
                            addReceiver(destination, channelIds);
                        }
                        else
                        {
                            removeReceiver(destination, channelIds);
                        }

                        toMediaDriverBuffer.write(eventTypeId, buffer, index, length);
                        break;
                    }

                    case REQUEST_CLEANED_TERM:
                        toMediaDriverBuffer.write(eventTypeId, buffer, index, length);
                        break;
                }
            }
        );
    }

    private void addReceiver(final String destination, final long[] channelIds)
    {
        // Not efficient but only happens once per channel ever
        // and is during setup and not a latency critical path
        for (final long channelId : channelIds)
        {
            subscriberChannels.forEach(
                (receiver) ->
                {
                    if (receiver.matches(destination, channelId))
                    {
                        rcvNotifiers.put(destination, channelId, receiver);
                    }
                }
            );
        }
    }

    private void removeReceiver(final String destination, final long[] channelIds)
    {
        for (final long channelId : channelIds)
        {
            rcvNotifiers.remove(destination, channelId);
        }
        // TOOD: release buffers
    }

    private void addPublisher(final String destination, final long channelId, final long sessionId)
    {
        // see addReceiver re efficiency
        publishers.forEach(
            (channel) ->
            {
                if (channel.matches(destination, sessionId, channelId))
                {
                    sendNotifiers.put(destination, sessionId, channelId, channel);
                }
            }
        );
    }

    private void removePublisher(final String destination, final long channelId, final long sessionId)
    {
        if (sendNotifiers.remove(destination, channelId, sessionId) == null)
        {
            // TODO: log an error
        }

        // TODO
        // bufferUsage.releasePublisherBuffers(destination, channelId, sessionId);
    }

    private void handleMessagesFromMediaDriver()
    {
        fromMediaDriverBuffer.read(
            (eventTypeId, buffer, index, length) ->
            {
                switch (eventTypeId)
                {
                    case NEW_RECEIVE_BUFFER_NOTIFICATION:
                    case NEW_SEND_BUFFER_NOTIFICATION:
                        bufferNotificationMessage.wrap(buffer, index);

                        final long sessionId = bufferNotificationMessage.sessionId();
                        final long channelId = bufferNotificationMessage.channelId();
                        final long termId = bufferNotificationMessage.termId();
                        final String destination = bufferNotificationMessage.destination();

                        if (eventTypeId == NEW_SEND_BUFFER_NOTIFICATION)
                        {
                            onNewSenderBuffer(sessionId, channelId, termId, destination);
                        }
                        else
                        {
                            onNewReceiverBuffer(destination, channelId, sessionId, termId);
                        }
                        break;

                    case ERROR_RESPONSE:
                        errorHandler.onErrorResponse(buffer, index, length);
                        break;

                    default:
                        break;
                }
            }
        );
    }

    private void onNewReceiverBuffer(final String destination, final long channelId,
                                     final long sessionId, final long termId)
    {
        onNewBuffer(sessionId,
                    rcvNotifiers.get(destination, channelId),
                    this::newReader,
                    LogReader[]::new,
                    (chan, buffers) ->
                    {
                        // TODO: get the counter id
                        final PositionReporter reporter = statusCounters.reporter(0);
                        chan.onBuffersMapped(sessionId, termId, buffers, reporter);
                    });
    }

    private void onNewSenderBuffer(final long sessionId, final long channelId,
                                   final long termId, final String destination)
    {
        onNewBuffer(sessionId,
                    sendNotifiers.get(destination, sessionId, channelId),
                    this::newAppender,
                    LogAppender[]::new,
                    (chan, buffers) ->
                    {
                        // TODO: get the counter id
                        final PositionIndicator indicator = statusCounters.indicator(0);
                        chan.onBuffersMapped(termId, buffers, indicator);
                    });
    }

    private interface LogFactory<L>
    {
        public L make(int index) throws IOException;
    }

    private <C extends ChannelNotifiable, L> void onNewBuffer(final long sessionId,
                                                              final C channel,
                                                              final LogFactory<L> logFactory,
                                                              final IntFunction<L[]> logArray,
                                                              final BiConsumer<C, L[]> notifier)
    {
        try
        {
            if (channel == null)
            {
                // The new buffer refers to another client process,
                // We can safely ignore it
                return;
            }

            if (!channel.hasTerm(sessionId))
            {
                final L[] logs = logArray.apply(BUFFER_COUNT);
                for (int i = 0; i < BUFFER_COUNT; i++)
                {
                    logs[i] = logFactory.make(i);
                }

                notifier.accept(channel, logs);
            }
            else
            {
                // TODO is this an error, or a reasonable case?
            }
        }
        catch (final Exception ex)
        {
            // TODO: establish correct client error handling strategy
            ex.printStackTrace();
        }
    }

    public LogAppender newAppender(final int index) throws IOException
    {
        final AtomicBuffer logBuffer = bufferUsage.newBuffer(bufferNotificationMessage, index);
        final AtomicBuffer stateBuffer = bufferUsage.newBuffer(bufferNotificationMessage, index + BUFFER_COUNT);

        return new LogAppender(logBuffer, stateBuffer, DEFAULT_HEADER, MAX_FRAME_LENGTH);
    }

    private LogReader newReader(final int index) throws IOException
    {
        final AtomicBuffer logBuffer = bufferUsage.newBuffer(bufferNotificationMessage, index);
        final AtomicBuffer stateBuffer = bufferUsage.newBuffer(bufferNotificationMessage, index + BUFFER_COUNT);

        return new LogReader(logBuffer, stateBuffer);
    }
}