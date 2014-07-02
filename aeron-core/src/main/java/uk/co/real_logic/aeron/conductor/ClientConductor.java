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

import uk.co.real_logic.aeron.MediaDriverTimeoutException;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.util.Agent;
import uk.co.real_logic.aeron.util.AgentIdleStrategy;
import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.BufferRotationDescriptor;
import uk.co.real_logic.aeron.util.collections.ConnectionMap;
import uk.co.real_logic.aeron.util.command.NewBufferMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.broadcast.CopyBroadcastReceiver;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.status.BufferPositionIndicator;
import uk.co.real_logic.aeron.util.status.LimitBarrier;
import uk.co.real_logic.aeron.util.status.WindowedLimitBarrier;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.BUFFER_COUNT;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;

/**
 * Client conductor takes responses and notifications from media driver and acts on them. As well as passes commands
 * to the media driver.
 */
public class ClientConductor extends Agent
{
    private static final int MAX_FRAME_LENGTH = 1024;

    public static final long AGENT_IDLE_MAX_SPINS = 5000;
    public static final long AGENT_IDLE_MAX_YIELDS = 100;
    public static final long AGENT_IDLE_MIN_PARK_NS = TimeUnit.NANOSECONDS.toNanos(10);
    public static final long AGENT_IDLE_MAX_PARK_NS = TimeUnit.MICROSECONDS.toNanos(100);

    private static final long NO_CORRELATION_ID = -1;

    private final CopyBroadcastReceiver toClientBuffer;

    private final BufferUsageStrategy bufferUsage;
    private final AtomicArray<Publication> publications = new AtomicArray<>();
    private final AtomicArray<Subscription> subscriptions = new AtomicArray<>();

    private final ConnectionMap<String, Publication> publicationMap = new ConnectionMap<>(); // Guarded by this
    private final SubscriptionMap subscriptionMap = new SubscriptionMap();

    private final ConductorErrorHandler errorHandler;
    private final long awaitTimeout;
    private final long publicationWindow;

    private final NewBufferMessageFlyweight newBufferMessage = new NewBufferMessageFlyweight();
    private final AtomicBuffer counterValuesBuffer;
    private final MediaDriverProxy mediaDriverProxy;
    private final Signal correlationSignal;

    private long activeCorrelationId; // Guarded by this
    private Publication addedPublication; // Guarded by this

    public ClientConductor(final CopyBroadcastReceiver toClientBuffer,
                           final ConductorErrorHandler errorHandler,
                           final BufferUsageStrategy bufferUsageStrategy,
                           final AtomicBuffer counterValuesBuffer,
                           final MediaDriverProxy mediaDriverProxy,
                           final Signal correlationSignal,
                           final long awaitTimeout,
                           final long publicationWindow)
    {
        super(new AgentIdleStrategy(AGENT_IDLE_MAX_SPINS, AGENT_IDLE_MAX_YIELDS,
                                    AGENT_IDLE_MIN_PARK_NS, AGENT_IDLE_MAX_PARK_NS));

        this.counterValuesBuffer = counterValuesBuffer;

        this.correlationSignal = correlationSignal;
        this.mediaDriverProxy = mediaDriverProxy;
        this.toClientBuffer = toClientBuffer;
        this.bufferUsage = bufferUsageStrategy;
        this.errorHandler = errorHandler;
        this.awaitTimeout = awaitTimeout;
        this.publicationWindow = publicationWindow;
    }

    public boolean doWork()
    {
        boolean hasDoneWork = handleMessagesFromMediaDriver();
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
        publications.forEach(
            (publication) ->
            {
                final long dirtyTermId = publication.dirtyTermId();
                if (dirtyTermId != Publication.NO_DIRTY_TERM)
                {
                    mediaDriverProxy.requestTerm(publication.destination(),
                                                 publication.sessionId(),
                                                 publication.channelId(),
                                                 dirtyTermId);
                }
            });

        subscriptions.forEach(Subscription::processBufferScan);
    }

    private boolean handleMessagesFromMediaDriver()
    {
        final int messagesRead = toClientBuffer.receive(
            (msgTypeId, buffer, index, length) ->
            {
                try
                {
                    switch (msgTypeId)
                    {
                        case NEW_SUBSCRIPTION_BUFFER_EVENT:
                        case NEW_PUBLICATION_BUFFER_EVENT:
                            newBufferMessage.wrap(buffer, index);

                            final String destination = newBufferMessage.destination();
                            final long sessionId = newBufferMessage.sessionId();
                            final long channelId = newBufferMessage.channelId();
                            final long termId = newBufferMessage.termId();
                            final int positionIndicatorId = newBufferMessage.positionCounterId();

                            if (msgTypeId == NEW_PUBLICATION_BUFFER_EVENT)
                            {
                                if (newBufferMessage.correlationId() != activeCorrelationId)
                                {
                                    break;
                                }

                                onNewPublicationBuffers(destination, sessionId, channelId, termId, positionIndicatorId);
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
                catch (final IOException ex)
                {
                    ex.printStackTrace();
                }
            }
        );

        return messagesRead > 0;
    }

    private LimitBarrier limitBarrier(final int positionIndicatorId)
    {
        final BufferPositionIndicator indicator = new BufferPositionIndicator(counterValuesBuffer, positionIndicatorId);
        return new WindowedLimitBarrier(indicator, publicationWindow);
    }

    public synchronized Publication addPublication(final String destination, final long channelId, final long sessionId)
    {
        Publication publication = publicationMap.get(destination, sessionId, channelId);

        if (publication == null)
        {
            activeCorrelationId = mediaDriverProxy.addPublication(destination, channelId, sessionId);

            final long startTime = System.currentTimeMillis();
            while (addedPublication == null)
            {
                correlationSignal.await(awaitTimeout);

                checkMediaDriverTimeout(startTime);
            }

            publication = addedPublication;
            publicationMap.put(destination, sessionId, channelId, publication);
            addedPublication = null;
            activeCorrelationId = NO_CORRELATION_ID;
        }

        publication.incRef();

        return publication;
    }

    public synchronized void releasePublication(final Publication publication)
    {
        final String destination = publication.destination();
        final long channelId = publication.channelId();
        final long sessionId = publication.sessionId();

        activeCorrelationId = mediaDriverProxy.removePublication(destination, channelId, sessionId);

        // TODO: wait for response from media driver

        // TODO:
        // bufferUsage.releasePublisherBuffers(destination, channelId, sessionId);
    }

    public synchronized Subscription addSubscription(final String destination,
                                                     final long channelId,
                                                     final Subscription.DataHandler handler)
    {

        Subscription subscription = subscriptionMap.get(destination, channelId);

        if (null == subscription)
        {
            subscription = new Subscription(this, handler, destination, channelId);

            subscriptions.add(subscription);
            subscriptionMap.put(destination, channelId, subscription);

            mediaDriverProxy.addSubscription(destination, channelId);
        }

        return subscription;
    }

    public synchronized void releaseSubscription(final Subscription subscription)
    {
        mediaDriverProxy.removeSubscription(subscription.destination(), subscription.channelId());

        subscriptionMap.remove(subscription.destination(), subscription.channelId());
        subscriptions.remove(subscription);

        // TODO: clean up logs
    }

    private void onNewPublicationBuffers(final String destination,
                                         final long sessionId,
                                         final long channelId,
                                         final long termId,
                                         final int positionIndicatorId) throws IOException
    {
        final LogAppender[] logs = new LogAppender[BUFFER_COUNT];
        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            logs[i] = newAppender(i, sessionId, channelId, termId);
        }

        final LimitBarrier limit = limitBarrier(positionIndicatorId);
        final Publication publication = new Publication(this, destination, channelId, sessionId, termId, logs, limit);
        publications.add(publication);
        addedPublication = publication;

        correlationSignal.signal();
    }

    private void onNewSubscriptionBuffers(final String destination,
                                          final long sessionId,
                                          final long channelId,
                                          final long currentTermId) throws IOException
    {
        final Subscription subscription = subscriptionMap.get(destination, channelId);
        if (null != subscription && !subscription.isConnected(sessionId))
        {
            final LogReader[] logs = new LogReader[BUFFER_COUNT];
            for (int i = 0; i < BUFFER_COUNT; i++)
            {
                logs[i] = newReader(i);
            }

            subscription.onBuffersMapped(sessionId, currentTermId, logs);
        }
    }

    private void checkMediaDriverTimeout(final long startTime)
        throws MediaDriverTimeoutException
    {
        if ((System.currentTimeMillis() - startTime) > awaitTimeout)
        {
            String msg = String.format("No response from media driver within %d ms", awaitTimeout);
            throw new MediaDriverTimeoutException(msg);
        }
    }

    private AtomicBuffer newBuffer(final NewBufferMessageFlyweight newBufferMessage, final int index)
        throws IOException
    {
        final String location = newBufferMessage.location(index);
        final int offset = newBufferMessage.bufferOffset(index);
        final int length = newBufferMessage.bufferLength(index);

        return bufferUsage.newBuffer(location, offset, length);
    }

    private LogAppender newAppender(final int index, final long sessionId, final long channelId, final long termId)
        throws IOException
    {
        final AtomicBuffer logBuffer = newBuffer(newBufferMessage, index);
        final AtomicBuffer stateBuffer = newBuffer(newBufferMessage, index + BufferRotationDescriptor.BUFFER_COUNT);
        final byte[] header = DataHeaderFlyweight.createDefaultHeader(sessionId, channelId, termId);

        return new LogAppender(logBuffer, stateBuffer, header, MAX_FRAME_LENGTH);
    }

    private LogReader newReader(final int index) throws IOException
    {
        final AtomicBuffer logBuffer = newBuffer(newBufferMessage, index);
        final AtomicBuffer stateBuffer = newBuffer(newBufferMessage, index + BufferRotationDescriptor.BUFFER_COUNT);

        return new LogReader(logBuffer, stateBuffer);
    }
}