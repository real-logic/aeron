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
import uk.co.real_logic.aeron.SubscriberChannel;
import uk.co.real_logic.aeron.util.Agent;
import uk.co.real_logic.aeron.util.AgentIdleStrategy;
import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.collections.ChannelMap;
import uk.co.real_logic.aeron.util.command.NewBufferMessageFlyweight;
import uk.co.real_logic.aeron.util.command.PublicationMessageFlyweight;
import uk.co.real_logic.aeron.util.command.SubscriptionMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.broadcast.BroadcastReceiver;
import uk.co.real_logic.aeron.util.concurrent.broadcast.CopyBroadcastReceiver;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.status.PositionIndicator;
import uk.co.real_logic.aeron.util.status.PositionReporter;
import uk.co.real_logic.aeron.util.status.StatusBufferMapper;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;

import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.BUFFER_COUNT;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;

/**
 * Client conductor takes responses and notifications from media driver and acts on them. As well as pass commands
 * to the media driver.
 */
public final class ClientConductor extends Agent
{
    private static final int MAX_FRAME_LENGTH = 1024;

    public static final long AGENT_IDLE_MAX_SPINS = 5000;
    public static final long AGENT_IDLE_MAX_YIELDS = 100;
    public static final long AGENT_IDLE_MIN_PARK_NANOS = TimeUnit.NANOSECONDS.toNanos(10);
    public static final long AGENT_IDLE_MAX_PARK_NANOS = TimeUnit.MICROSECONDS.toNanos(100);

    private final RingBuffer commandBuffer;
    private final CopyBroadcastReceiver toClientBuffer;
    private final RingBuffer toDriverBuffer;

    private final BufferUsageStrategy bufferUsage;
    private final AtomicArray<Channel> publishers;
    private final AtomicArray<SubscriberChannel> subscriberChannels;

    private final ChannelMap<String, Channel> sendNotifiers = new ChannelMap<>();
    private final SubscriberMap rcvNotifiers = new SubscriberMap();
    private final StatusBufferMapper statusCounters = new StatusBufferMapper();

    private final ConductorErrorHandler errorHandler;

    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();
    private final NewBufferMessageFlyweight newBufferMessage = new NewBufferMessageFlyweight();

    public ClientConductor(final RingBuffer commandBuffer,
                           final CopyBroadcastReceiver toClientBuffer,
                           final RingBuffer toDriverBuffer,
                           final BufferUsageStrategy bufferUsage,
                           final AtomicArray<Channel> publishers,
                           final AtomicArray<SubscriberChannel> subscriberChannels,
                           final ConductorErrorHandler errorHandler)
    {
        super(new AgentIdleStrategy(AGENT_IDLE_MAX_SPINS, AGENT_IDLE_MAX_YIELDS,
                AGENT_IDLE_MIN_PARK_NANOS, AGENT_IDLE_MAX_PARK_NANOS));

        this.commandBuffer = commandBuffer;
        this.toClientBuffer = toClientBuffer;
        this.toDriverBuffer = toDriverBuffer;
        this.bufferUsage = bufferUsage;
        this.publishers = publishers;
        this.subscriberChannels = subscriberChannels;
        this.errorHandler = errorHandler;
    }

    public boolean doWork()
    {
        boolean hasDoneWork = handleClientCommandBuffer();
        hasDoneWork |= handleMessagesFromMediaDriver();
        performBufferMaintenance();

        return hasDoneWork;
    }

    public void close()
    {
        stop();
        bufferUsage.close();
        statusCounters.close();
    }

    private void performBufferMaintenance()
    {
        publishers.forEach(Channel::processBufferScan);
        subscriberChannels.forEach(SubscriberChannel::processBufferScan);
    }

    private boolean handleClientCommandBuffer()
    {
        final int messagesRead = commandBuffer.read(
            (msgTypeId, buffer, index, length) ->
            {
                switch (msgTypeId)
                {
                    case ADD_PUBLICATION:
                    case REMOVE_PUBLICATION:
                    {
                        publicationMessage.wrap(buffer, index);
                        final String destination = publicationMessage.destination();
                        final long channelId = publicationMessage.channelId();
                        final long sessionId = publicationMessage.sessionId();

                        if (msgTypeId == ADD_PUBLICATION)
                        {
                            addPublisher(destination, channelId, sessionId);
                        }
                        else
                        {
                            removePublisher(destination, channelId, sessionId);
                        }

                        toDriverBuffer.write(msgTypeId, buffer, index, length);
                        break;
                    }

                    case ADD_SUBSCRIPTION:
                    case REMOVE_SUBSCRIPTION:
                    {
                        subscriptionMessage.wrap(buffer, index);
                        final long[] channelIds = subscriptionMessage.channelIds();
                        final String destination = subscriptionMessage.destination();
                        if (msgTypeId == ADD_SUBSCRIPTION)
                        {
                            addReceiver(destination, channelIds);
                        }
                        else
                        {
                            removeReceiver(destination, channelIds);
                        }

                        toDriverBuffer.write(msgTypeId, buffer, index, length);
                        break;
                    }

                    case CLEAN_TERM_BUFFER:
                        toDriverBuffer.write(msgTypeId, buffer, index, length);
                        break;
                }
            }
        );

        return messagesRead > 0;
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

    private boolean handleMessagesFromMediaDriver()
    {

        final int messagesRead = toClientBuffer.receive(
            (msgTypeId, buffer, index, length) ->
            {
                switch (msgTypeId)
                {
                    case NEW_SUBSCRIPTION_BUFFER_NOTIFICATION:
                    case NEW_PUBLICATION_BUFFER_NOTIFICATION:
                        newBufferMessage.wrap(buffer, index);

                        final long sessionId = newBufferMessage.sessionId();
                        final long channelId = newBufferMessage.channelId();
                        final long termId = newBufferMessage.termId();
                        final String destination = newBufferMessage.destination();

                        if (msgTypeId == NEW_PUBLICATION_BUFFER_NOTIFICATION)
                        {
                            onNewSenderBuffer(destination, sessionId, channelId, termId);
                        }
                        else
                        {
                            onNewReceiverBuffer(destination, sessionId, channelId, termId);
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

        return messagesRead > 0;
    }

    private void onNewReceiverBuffer(final String destination, final long sessionId,
                                     final long channelId, final long termId)
    {
        onNewBuffer(sessionId,
                    channelId,
                    termId,
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

    private void onNewSenderBuffer(final String destination, final long sessionId,
                                   final long channelId, final long termId)
    {
        onNewBuffer(sessionId,
                    channelId,
                    termId,
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
        public L make(final int index, final long sessionId,
                      final long channelId, final long termId) throws IOException;
    }

    private <C extends ChannelNotifiable, L> void onNewBuffer(final long sessionId,
                                                              final long channelId,
                                                              final long termId,
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
                    logs[i] = logFactory.make(i, sessionId, channelId, termId);
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

    public LogAppender newAppender(final int index, final long sessionId,
                                   final long channelId, final long termId) throws IOException
    {
        final AtomicBuffer logBuffer = bufferUsage.newBuffer(newBufferMessage, index);
        final AtomicBuffer stateBuffer = bufferUsage.newBuffer(newBufferMessage, index + BUFFER_COUNT);
        final byte[] header = DataHeaderFlyweight.createDefaultHeader(sessionId, channelId, termId);

        return new LogAppender(logBuffer, stateBuffer, header, MAX_FRAME_LENGTH);
    }

    private LogReader newReader(final int index, final long sessionId,
                                final long channelId, final long termId) throws IOException
    {
        final AtomicBuffer logBuffer = bufferUsage.newBuffer(newBufferMessage, index);
        final AtomicBuffer stateBuffer = bufferUsage.newBuffer(newBufferMessage, index + BUFFER_COUNT);

        return new LogReader(logBuffer, stateBuffer);
    }
}