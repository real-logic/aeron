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

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Channel;
import uk.co.real_logic.aeron.SubscriberChannel;
import uk.co.real_logic.aeron.util.Agent;
import uk.co.real_logic.aeron.util.AgentIdleStrategy;
import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.BufferRotationDescriptor;
import uk.co.real_logic.aeron.util.collections.ChannelMap;
import uk.co.real_logic.aeron.util.command.NewBufferMessageFlyweight;
import uk.co.real_logic.aeron.util.command.PublicationMessageFlyweight;
import uk.co.real_logic.aeron.util.command.SubscriptionMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.broadcast.CopyBroadcastReceiver;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.status.BufferPositionIndicator;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;

import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.BUFFER_COUNT;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;

/**
 * Client conductor takes responses and notifications from media driver and acts on them. As well as passes commands
 * to the media driver.
 */
public final class ClientConductor extends Agent
{
    private static final int MAX_FRAME_LENGTH = 1024;

    public static final long AGENT_IDLE_MAX_SPINS = 5000;
    public static final long AGENT_IDLE_MAX_YIELDS = 100;
    public static final long AGENT_IDLE_MIN_PARK_NS = TimeUnit.NANOSECONDS.toNanos(10);
    public static final long AGENT_IDLE_MAX_PARK_NS = TimeUnit.MICROSECONDS.toNanos(100);

    private final RingBuffer commandBuffer;
    private final CopyBroadcastReceiver toClientBuffer;
    private final RingBuffer toDriverBuffer;

    private final BufferUsageStrategy bufferUsage;
    private final AtomicArray<Channel> publishers;
    private final AtomicArray<SubscriberChannel> subscriberChannels;

    private final ChannelMap<String, Channel> sendNotifiers = new ChannelMap<>();
    private final SubscriptionMap subscriptionMap = new SubscriptionMap();

    private final ConductorErrorHandler errorHandler;

    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();
    private final NewBufferMessageFlyweight newBufferMessage = new NewBufferMessageFlyweight();
    private final AtomicBuffer counterValuesBuffer;

    public ClientConductor(final RingBuffer commandBuffer,
                           final CopyBroadcastReceiver toClientBuffer,
                           final RingBuffer toDriverBuffer,
                           final AtomicArray<Channel> publishers,
                           final AtomicArray<SubscriberChannel> subscriberChannels,
                           final ConductorErrorHandler errorHandler,
                           final Aeron.ClientContext ctx)
    {
        super(new AgentIdleStrategy(AGENT_IDLE_MAX_SPINS, AGENT_IDLE_MAX_YIELDS,
                                    AGENT_IDLE_MIN_PARK_NS, AGENT_IDLE_MAX_PARK_NS));

        counterValuesBuffer = ctx.counterValuesBuffer();

        this.commandBuffer = commandBuffer;
        this.toClientBuffer = toClientBuffer;
        this.toDriverBuffer = toDriverBuffer;
        this.bufferUsage = ctx.bufferUsageStrategy();
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
    }

    private void performBufferMaintenance()
    {
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
                            addSubscription(destination, channelIds);
                        }
                        else
                        {
                            removeSubscription(destination, channelIds);
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

    private void addSubscription(final String destination, final long[] channelIds)
    {
        for (final long channelId : channelIds)
        {
            subscriberChannels.forEach(
                (subscription) ->
                {
                    if (subscription.matches(destination, channelId))
                    {
                        subscriptionMap.put(destination, channelId, subscription);
                    }
                }
            );
        }
    }

    private void removeSubscription(final String destination, final long[] channelIds)
    {
        for (final long channelId : channelIds)
        {
            subscriptionMap.remove(destination, channelId);
        }
        // TODO: release buffers
    }

    private void addPublisher(final String destination, final long channelId, final long sessionId)
    {
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

        // TODO:
        // bufferUsage.releasePublisherBuffers(destination, channelId, sessionId);
    }

    private boolean handleMessagesFromMediaDriver()
    {
        final int messagesRead = toClientBuffer.receive(
            (msgTypeId, buffer, index, length) ->
            {
                switch (msgTypeId)
                {
                    case NEW_SUBSCRIPTION_BUFFER_EVENT:
                    case NEW_PUBLICATION_BUFFER_EVENT:
                        newBufferMessage.wrap(buffer, index);

                        final long sessionId = newBufferMessage.sessionId();
                        final long channelId = newBufferMessage.channelId();
                        final long termId = newBufferMessage.termId();
                        final String destination = newBufferMessage.destination();

                        if (msgTypeId == NEW_PUBLICATION_BUFFER_EVENT)
                        {
                            onNewPublicationBuffers(destination, sessionId, channelId, termId);
                        }
                        else
                        {
                            onNewSubscriptionBuffers(destination, sessionId, channelId, termId);
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

    private void onNewSubscriptionBuffers(final String destination, final long sessionId,
                                          final long channelId, final long termId)
    {
        onNewBuffers(sessionId,
                     channelId,
                     termId,
                     subscriptionMap.get(destination, channelId),
                     this::newReader,
                     LogReader[]::new,
                     (chan, buffers) ->
                     {
                         // TODO: get the counter id
                         chan.onBuffersMapped(sessionId, termId, buffers);
                     });
    }

    private void onNewPublicationBuffers(final String destination, final long sessionId,
                                         final long channelId, final long termId)
    {
        onNewBuffers(sessionId,
                     channelId,
                     termId,
                     sendNotifiers.get(destination, sessionId, channelId),
                     this::newAppender,
                     LogAppender[]::new,
                     (chan, buffers) ->
                     {
                         // TODO: get the counter id
                         chan.onBuffersMapped(termId, buffers, new BufferPositionIndicator(counterValuesBuffer, 0));
                     });
    }

    private interface LogFactory<L>
    {
        public L make(final int index, final long sessionId,
                      final long channelId, final long termId) throws IOException;
    }

    private <C extends ChannelEndpoint, L> void onNewBuffers(final long sessionId,
                                                             final long channelId,
                                                             final long termId,
                                                             final C channelEndpoint,
                                                             final LogFactory<L> logFactory,
                                                             final IntFunction<L[]> logArray,
                                                             final BiConsumer<C, L[]> notifier)
    {
        try
        {
            if (channelEndpoint == null)
            {
                // The new newBuffer refers to another client process, we can safely ignore it
                return;
            }

            if (!channelEndpoint.hasTerm(sessionId))
            {
                final L[] logs = logArray.apply(BUFFER_COUNT);
                for (int i = 0; i < BUFFER_COUNT; i++)
                {
                    logs[i] = logFactory.make(i, sessionId, channelId, termId);
                }

                notifier.accept(channelEndpoint, logs);
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

    private AtomicBuffer newBuffer(final NewBufferMessageFlyweight newBufferMessage, int index) throws IOException
    {
        final String location = newBufferMessage.location(index);
        final int offset = newBufferMessage.bufferOffset(index);
        final int length = newBufferMessage.bufferLength(index);

        return bufferUsage.newBuffer(location, offset, length);
    }

    private LogAppender newAppender(final int index, final long sessionId,
                                    final long channelId, final long termId) throws IOException
    {
        final AtomicBuffer logBuffer = newBuffer(newBufferMessage, index);
        final AtomicBuffer stateBuffer = newBuffer(newBufferMessage, index + BufferRotationDescriptor.BUFFER_COUNT);
        final byte[] header = DataHeaderFlyweight.createDefaultHeader(sessionId, channelId, termId);

        return new LogAppender(logBuffer, stateBuffer, header, MAX_FRAME_LENGTH);
    }

    private LogReader newReader(final int index, final long sessionId,
                                final long channelId, final long termId) throws IOException
    {
        final AtomicBuffer logBuffer = newBuffer(newBufferMessage, index);
        final AtomicBuffer stateBuffer = newBuffer(newBufferMessage, index + BufferRotationDescriptor.BUFFER_COUNT);

        return new LogReader(logBuffer, stateBuffer);
    }
}