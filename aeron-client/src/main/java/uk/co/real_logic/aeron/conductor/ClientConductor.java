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

import uk.co.real_logic.aeron.DataHandler;
import uk.co.real_logic.aeron.NewConnectionHandler;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.Agent;
import uk.co.real_logic.aeron.common.BackoffIdleStrategy;
import uk.co.real_logic.aeron.common.ErrorCode;
import uk.co.real_logic.aeron.common.TermHelper;
import uk.co.real_logic.aeron.common.collections.ConnectionMap;
import uk.co.real_logic.aeron.common.command.LogBuffersMessageFlyweight;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.status.BufferPositionIndicator;
import uk.co.real_logic.aeron.common.status.BufferPositionReporter;
import uk.co.real_logic.aeron.common.status.PositionIndicator;
import uk.co.real_logic.aeron.common.status.PositionReporter;
import uk.co.real_logic.aeron.exceptions.MediaDriverTimeoutException;
import uk.co.real_logic.aeron.exceptions.RegistrationException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static uk.co.real_logic.aeron.common.TermHelper.BUFFER_COUNT;

/**
 * Client conductor takes responses and notifications from media driver and acts on them. As well as passes commands
 * to the media driver.
 */
public class ClientConductor extends Agent implements DriverListener
{
    public static final long AGENT_IDLE_MAX_SPINS = 100;
    public static final long AGENT_IDLE_MAX_YIELDS = 100;
    public static final long AGENT_IDLE_MIN_PARK_NS = TimeUnit.NANOSECONDS.toNanos(10);
    public static final long AGENT_IDLE_MAX_PARK_NS = TimeUnit.MICROSECONDS.toNanos(100);

    private static final long NO_CORRELATION_ID = -1;

    private final DriverBroadcastReceiver driverBroadcastReceiver;
    private final BufferManager bufferManager;
    private final long awaitTimeout;
    private final ConnectionMap<String, Publication> publicationMap = new ConnectionMap<>(); // Guarded by this
    private final SubscriptionMap subscriptionMap = new SubscriptionMap();

    private final AtomicBuffer counterValuesBuffer;
    private final DriverProxy driverProxy;
    private final Signal correlationSignal;

    private final NewConnectionHandler newConnectionHandler;
    private final int mtuLength;

    private long activeCorrelationId = -1; // Guarded by this
    private Publication addedPublication; // Guarded by this
    private boolean operationSucceeded = false; // Guarded by this
    private RegistrationException registrationException; // Guarded by this

    public ClientConductor(final DriverBroadcastReceiver driverBroadcastReceiver,
                           final BufferManager bufferManager,
                           final AtomicBuffer counterValuesBuffer,
                           final DriverProxy driverProxy,
                           final Signal correlationSignal,
                           final Consumer<Exception> errorHandler,
                           final NewConnectionHandler newConnectionHandler,
                           final long awaitTimeout,
                           final int mtuLength)
    {
        super(new BackoffIdleStrategy(AGENT_IDLE_MAX_SPINS, AGENT_IDLE_MAX_YIELDS,
                                      AGENT_IDLE_MIN_PARK_NS, AGENT_IDLE_MAX_PARK_NS),
            errorHandler);

        this.counterValuesBuffer = counterValuesBuffer;
        this.correlationSignal = correlationSignal;
        this.driverProxy = driverProxy;
        this.driverBroadcastReceiver = driverBroadcastReceiver;
        this.bufferManager = bufferManager;
        this.newConnectionHandler = newConnectionHandler;
        this.awaitTimeout = awaitTimeout;
        this.mtuLength = mtuLength;
    }

    public int doWork()
    {
        return driverBroadcastReceiver.receive(this, activeCorrelationId);
    }

    public void close()
    {
        stop();
    }

    public synchronized Publication addPublication(final String channel, final int sessionId, final int streamId)
    {
        Publication publication = publicationMap.get(channel, sessionId, streamId);

        if (publication == null)
        {
            activeCorrelationId = driverProxy.addPublication(channel, sessionId, streamId);

            final long startTime = System.currentTimeMillis();
            while (addedPublication == null)
            {
                await(startTime);
            }

            publication = addedPublication;
            publicationMap.put(channel, sessionId, streamId, publication);
            addedPublication = null;
            activeCorrelationId = NO_CORRELATION_ID;
        }
        else
        {
            publication.incRef();
        }

        return publication;
    }

    public synchronized void releasePublication(final Publication publication)
    {
        final String channel = publication.channel();
        final int sessionId = publication.sessionId();
        final int streamId = publication.streamId();

        activeCorrelationId = driverProxy.removePublication(channel, sessionId, streamId);

        awaitOperationSucceeded();

        publicationMap.remove(channel, sessionId, streamId);
    }

    public synchronized Subscription addSubscription(final String channel, final int streamId, final DataHandler handler)
    {
        Subscription subscription = subscriptionMap.get(channel, streamId);

        if (null == subscription)
        {
            subscription = new Subscription(this, handler, channel, streamId);

            subscriptionMap.put(channel, streamId, subscription);

            activeCorrelationId = driverProxy.addSubscription(channel, streamId);
            awaitOperationSucceeded();
        }

        return subscription;
    }

    public synchronized void releaseSubscription(final Subscription subscription)
    {
        activeCorrelationId = driverProxy.removeSubscription(subscription.channel(), subscription.streamId());

        subscriptionMap.remove(subscription.channel(), subscription.streamId());

        awaitOperationSucceeded();
    }

    public void onNewPublication(final String channel,
                                 final int sessionId,
                                 final int streamId,
                                 final int termId,
                                 final int limitPositionIndicatorOffset,
                                 final LogBuffersMessageFlyweight logBuffersMessage) throws IOException
    {
        final LogAppender[] logs = new LogAppender[BUFFER_COUNT];
        final ManagedBuffer[] managedBuffers = new ManagedBuffer[BUFFER_COUNT * 2];

        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            final ManagedBuffer logBuffer = mapBuffer(logBuffersMessage, i);
            final ManagedBuffer stateBuffer = mapBuffer(logBuffersMessage, i + TermHelper.BUFFER_COUNT);
            final byte[] header = DataHeaderFlyweight.createDefaultHeader(sessionId, streamId, termId);

            logs[i] = new LogAppender(logBuffer.buffer(), stateBuffer.buffer(), header, mtuLength);
            managedBuffers[i * 2] = logBuffer;
            managedBuffers[i * 2 + 1] = stateBuffer;
        }

        final PositionIndicator senderLimit =
            new BufferPositionIndicator(counterValuesBuffer, limitPositionIndicatorOffset);

        addedPublication = new Publication(this, channel, streamId, sessionId,
                                           termId, logs, senderLimit, managedBuffers);

        correlationSignal.signal();
    }

    public void onNewConnection(final String channel,
                                final int sessionId,
                                final int streamId,
                                final int initialTermId,
                                final LogBuffersMessageFlyweight message)
        throws IOException
    {
        final Subscription subscription = subscriptionMap.get(channel, streamId);
        if (null != subscription && !subscription.isConnected(sessionId))
        {
            final LogReader[] logs = new LogReader[BUFFER_COUNT];
            final ManagedBuffer[] managedBuffers = new ManagedBuffer[BUFFER_COUNT * 2];

            for (int i = 0; i < BUFFER_COUNT; i++)
            {
                final ManagedBuffer logBuffer = mapBuffer(message, i);
                final ManagedBuffer stateBuffer = mapBuffer(message, i + TermHelper.BUFFER_COUNT);

                logs[i] = new LogReader(logBuffer.buffer(), stateBuffer.buffer());
                managedBuffers[i * 2] = logBuffer;
                managedBuffers[i * 2 + 1] = stateBuffer;
            }

            final PositionReporter positionReporter =
                new BufferPositionReporter(counterValuesBuffer, message.positionCounterId());
            subscription.onTermBuffersMapped(sessionId, initialTermId, logs, positionReporter, managedBuffers);

            if (null != newConnectionHandler)
            {
                newConnectionHandler.onNewConnection(channel, sessionId, streamId);
            }
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

    private void await(final long startTime)
    {
        correlationSignal.await(awaitTimeout);
        checkMediaDriverTimeout(startTime);
        checkRegistrationException();
    }

    private void awaitOperationSucceeded()
    {
        final long startTime = System.currentTimeMillis();
        while (!operationSucceeded)
        {
            await(startTime);
        }

        operationSucceeded = false;
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

    private ManagedBuffer mapBuffer(final LogBuffersMessageFlyweight logBuffersMessage, final int index)
        throws IOException
    {
        final String location = logBuffersMessage.location(index);
        final int offset = logBuffersMessage.bufferOffset(index);
        final int length = logBuffersMessage.bufferLength(index);

        return bufferManager.newBuffer(location, offset, length);
    }
}
