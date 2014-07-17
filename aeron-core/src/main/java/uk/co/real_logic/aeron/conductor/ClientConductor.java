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

import uk.co.real_logic.aeron.*;
import uk.co.real_logic.aeron.util.Agent;
import uk.co.real_logic.aeron.util.BackoffIdleStrategy;
import uk.co.real_logic.aeron.util.ErrorCode;
import uk.co.real_logic.aeron.util.TermHelper;
import uk.co.real_logic.aeron.util.collections.ConnectionMap;
import uk.co.real_logic.aeron.util.command.LogBuffersMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.util.event.EventLogger;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.status.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static uk.co.real_logic.aeron.util.TermHelper.BUFFER_COUNT;

/**
 * Client conductor takes responses and notifications from media driver and acts on them. As well as passes commands
 * to the media driver.
 */
public class ClientConductor extends Agent implements MediaDriverListener
{
    private static final EventLogger LOGGER = new EventLogger(ClientConductor.class);

    private static final int MAX_FRAME_LENGTH = 1024;

    public static final long AGENT_IDLE_MAX_SPINS = 5000;
    public static final long AGENT_IDLE_MAX_YIELDS = 100;
    public static final long AGENT_IDLE_MIN_PARK_NS = TimeUnit.NANOSECONDS.toNanos(10);
    public static final long AGENT_IDLE_MAX_PARK_NS = TimeUnit.MICROSECONDS.toNanos(100);

    private static final long NO_CORRELATION_ID = -1;

    private final MediaDriverBroadcastReceiver mediaDriverBroadcastReceiver;
    private final BufferLifecycleStrategy bufferUsage;
    private final long awaitTimeout;
    private final ConnectionMap<String, Publication> publicationMap = new ConnectionMap<>(); // Guarded by this
    private final SubscriptionMap subscriptionMap = new SubscriptionMap();

    private final AtomicBuffer counterValuesBuffer;
    private final MediaDriverProxy mediaDriverProxy;
    private final Signal correlationSignal;

    private long activeCorrelationId; // Guarded by this
    private Publication addedPublication; // Guarded by this
    private boolean operationSucceeded = false; // Guarded by this
    private RegistrationException registrationException; // Guarded by this
    private BufferPositionIndicator senderPostionLimit;

    public ClientConductor(final MediaDriverBroadcastReceiver mediaDriverBroadcastReceiver,
                           final BufferLifecycleStrategy bufferLifecycleStrategy,
                           final AtomicBuffer counterValuesBuffer,
                           final MediaDriverProxy mediaDriverProxy,
                           final Signal correlationSignal,
                           final long awaitTimeout)
    {
        super(new BackoffIdleStrategy(AGENT_IDLE_MAX_SPINS, AGENT_IDLE_MAX_YIELDS,
                                      AGENT_IDLE_MIN_PARK_NS, AGENT_IDLE_MAX_PARK_NS),
              LOGGER::logException);

        this.counterValuesBuffer = counterValuesBuffer;
        this.correlationSignal = correlationSignal;
        this.mediaDriverProxy = mediaDriverProxy;
        this.mediaDriverBroadcastReceiver = mediaDriverBroadcastReceiver;
        this.bufferUsage = bufferLifecycleStrategy;
        this.awaitTimeout = awaitTimeout;
    }

    public int doWork()
    {
        return mediaDriverBroadcastReceiver.receive(this, activeCorrelationId);
    }

    public void close()
    {
        stop();
        bufferUsage.close();
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
                await(startTime);
            }

            publication = addedPublication;
            publicationMap.put(destination, sessionId, channelId, publication);
            addedPublication = null;
            activeCorrelationId = NO_CORRELATION_ID;
        }

        publication.incRef();

        return publication;
    }

    private void await(final long startTime)
    {
        correlationSignal.await(awaitTimeout);
        checkMediaDriverTimeout(startTime);
        checkRegistrationException();
    }

    public synchronized void releasePublication(final Publication publication)
    {
        final String destination = publication.destination();
        final long channelId = publication.channelId();
        final long sessionId = publication.sessionId();

        activeCorrelationId = mediaDriverProxy.removePublication(destination, sessionId, channelId);

        // TODO: wait for response from media driver
        final long startTime = System.currentTimeMillis();
        while (!operationSucceeded)
        {
            await(startTime);
        }

        operationSucceeded = false;

        publicationMap.remove(destination, sessionId, channelId);
    }

    public synchronized Subscription addSubscription(final String destination,
                                                     final long channelId,
                                                     final DataHandler handler)
    {

        Subscription subscription = subscriptionMap.get(destination, channelId);

        if (null == subscription)
        {
            subscription = new Subscription(this, handler, destination, channelId);

            subscriptionMap.put(destination, channelId, subscription);

            mediaDriverProxy.addSubscription(destination, channelId);
        }

        return subscription;
    }

    public synchronized void releaseSubscription(final Subscription subscription)
    {
        mediaDriverProxy.removeSubscription(subscription.destination(), subscription.channelId());

        subscriptionMap.remove(subscription.destination(), subscription.channelId());

        // TODO: clean up logs
    }

    public void onNewPublication(final String destination,
                                 final long sessionId,
                                 final long channelId,
                                 final long termId,
                                 final int limitPositionIndicatorOffset,
                                 final LogBuffersMessageFlyweight logBuffersMessage) throws IOException
    {
        final LogAppender[] logs = new LogAppender[BUFFER_COUNT];
        final LogInformation[] logInformation = new LogInformation[BUFFER_COUNT * 2];

        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            final LogInformation logBuffer = mapBuffer(logBuffersMessage, i);
            final LogInformation stateBuffer = mapBuffer(logBuffersMessage, i + TermHelper.BUFFER_COUNT);
            final byte[] header = DataHeaderFlyweight.createDefaultHeader(sessionId, channelId, termId);

            logs[i] = new LogAppender(logBuffer.buffer(), stateBuffer.buffer(), header, MAX_FRAME_LENGTH);
            logInformation[i * 2] = logBuffer;
            logInformation[i * 2 + 1] = stateBuffer;
        }

        final PositionIndicator limit = new BufferPositionIndicator(counterValuesBuffer, limitPositionIndicatorOffset);

        addedPublication = new Publication(this, destination, channelId, sessionId, termId, logs, limit, logInformation);

        correlationSignal.signal();
    }

    public void onNewConnectedSubscription(final String destination,
                                           final long sessionId,
                                           final long channelId,
                                           final long initialTermId,
                                           final LogBuffersMessageFlyweight message) throws IOException
    {
        final Subscription subscription = subscriptionMap.get(destination, channelId);
        if (null != subscription && !subscription.isConnected(sessionId))
        {
            final LogReader[] logs = new LogReader[BUFFER_COUNT];
            // TODO: put logInformation into subscriptions
            final LogInformation[] logInformation = new LogInformation[BUFFER_COUNT * 2];

            for (int i = 0; i < BUFFER_COUNT; i++)
            {
                final LogInformation logBuffer = mapBuffer(message, i);
                final LogInformation stateBuffer = mapBuffer(message, i + TermHelper.BUFFER_COUNT);

                logs[i] = new LogReader(logBuffer.buffer(), stateBuffer.buffer());
                logInformation[i * 2] = logBuffer;
                logInformation[i * 2 + 1] = stateBuffer;
            }

            subscription.onLogBufferMapped(sessionId, initialTermId, logs);
        }
    }

    public void onError(final ErrorCode errorCode, final String message)
    {
        registrationException = new RegistrationException(errorCode, message);
        correlationSignal.signal();
    }

    public void operationSucceeded()
    {
        operationSucceeded = true;
        correlationSignal.signal();
    }

    private void checkRegistrationException()
    {
        if (registrationException != null)
        {
            final RegistrationException exception = registrationException;
            registrationException = null;
            throw exception;
        }
    }

    private void checkMediaDriverTimeout(final long startTime)
        throws MediaDriverTimeoutException
    {
        if ((System.currentTimeMillis() - startTime) > awaitTimeout)
        {
            final String msg = String.format("No response from media driver within %d ms", awaitTimeout);
            throw new MediaDriverTimeoutException(msg);
        }
    }

    private LogInformation mapBuffer(final LogBuffersMessageFlyweight newBufferMessage, final int index)
        throws IOException
    {
        final String location = newBufferMessage.location(index);
        final int offset = newBufferMessage.bufferOffset(index);
        final int length = newBufferMessage.bufferLength(index);

        return bufferUsage.newBuffer(location, offset, length);
    }
}
