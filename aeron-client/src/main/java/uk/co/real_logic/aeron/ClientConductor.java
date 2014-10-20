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
package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.common.*;
import uk.co.real_logic.aeron.common.collections.ConnectionMap;
import uk.co.real_logic.aeron.common.command.ConnectionMessageFlyweight;
import uk.co.real_logic.aeron.common.command.ConnectionReadyFlyweight;
import uk.co.real_logic.aeron.common.command.ReadyFlyweight;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.broadcast.CopyBroadcastReceiver;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.status.BufferPositionIndicator;
import uk.co.real_logic.aeron.common.status.BufferPositionReporter;
import uk.co.real_logic.aeron.common.status.PositionIndicator;
import uk.co.real_logic.aeron.common.status.PositionReporter;
import uk.co.real_logic.aeron.exceptions.DriverTimeoutException;
import uk.co.real_logic.aeron.exceptions.RegistrationException;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static uk.co.real_logic.aeron.common.TermHelper.BUFFER_COUNT;

/**
 * Client conductor takes responses and notifications from media driver and acts on them. As well as passes commands
 * to the media driver.
 */
class ClientConductor extends Agent implements DriverListener
{
    private static final int KEEPALIVE_TIMEOUT_MS = 500;
    private static final long NO_CORRELATION_ID = -1;

    private final DriverListenerAdapter driverListenerAdapter;
    private final BufferManager bufferManager;
    private final long driverTimeoutMs;
    private final long driverTimeoutNs;
    private final ConnectionMap<String, Publication> publicationMap = new ConnectionMap<>(); // Guarded by this
    private final ActiveSubscriptions activeSubscriptions = new ActiveSubscriptions();

    private final AtomicBuffer counterValuesBuffer;
    private final DriverProxy driverProxy;
    private final Signal correlationSignal;
    private final TimerWheel timerWheel;

    private final NewConnectionHandler newConnectionHandler;
    private final InactiveConnectionHandler inactiveConnectionHandler;

    private long activeCorrelationId = -1; // Guarded by this
    private Publication addedPublication; // Guarded by this
    private boolean operationSucceeded = false; // Guarded by this
    private RegistrationException registrationException; // Guarded by this

    private final TimerWheel.Timer keepaliveTimer;
    private volatile boolean driverActive = true;

    public ClientConductor(
        final IdleStrategy idleStrategy,
        final CopyBroadcastReceiver broadcastReceiver,
        final BufferManager bufferManager,
        final AtomicBuffer counterValuesBuffer,
        final DriverProxy driverProxy,
        final Signal correlationSignal,
        final TimerWheel timerWheel,
        final Consumer<Exception> errorHandler,
        final NewConnectionHandler newConnectionHandler,
        final InactiveConnectionHandler inactiveConnectionHandler,
        final long driverTimeoutMs)
    {
        super(idleStrategy, errorHandler, null);

        this.counterValuesBuffer = counterValuesBuffer;
        this.correlationSignal = correlationSignal;
        this.driverProxy = driverProxy;
        this.bufferManager = bufferManager;
        this.timerWheel = timerWheel;
        this.newConnectionHandler = newConnectionHandler;
        this.inactiveConnectionHandler = inactiveConnectionHandler;
        this.driverTimeoutMs = driverTimeoutMs;
        this.driverTimeoutNs = TimeUnit.MILLISECONDS.toNanos(driverTimeoutMs);

        this.driverListenerAdapter = new DriverListenerAdapter(broadcastReceiver, this);
        this.keepaliveTimer = timerWheel.newTimeout(KEEPALIVE_TIMEOUT_MS, TimeUnit.MILLISECONDS, this::onKeepalive);
    }

    public int doWork()
    {
        int workCount = 0;

        workCount += processTimers();
        workCount += driverListenerAdapter.receiveMessages(activeCorrelationId);

        return workCount;
    }

    public synchronized Publication addPublication(final String channel, final int streamId, final int sessionId)
    {
        verifyDriverIsActive();

        Publication publication = publicationMap.get(channel, sessionId, streamId);

        if (publication == null)
        {
            activeCorrelationId = driverProxy.addPublication(channel, streamId, sessionId);

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
        verifyDriverIsActive();

        activeCorrelationId = driverProxy.removePublication(publication.registrationId());
        publicationMap.remove(publication.channel(), publication.sessionId(), publication.streamId());

        awaitOperationSucceeded();
    }

    public synchronized Subscription addSubscription(final String channel, final int streamId, final DataHandler handler)
    {
        verifyDriverIsActive();

        final Subscription subscription;
        synchronized (activeSubscriptions)
        {
            activeCorrelationId = driverProxy.addSubscription(channel, streamId);
            subscription = new Subscription(this, handler, channel, streamId, activeCorrelationId);
            activeSubscriptions.add(subscription);
        }

        awaitOperationSucceeded();

        return subscription;
    }

    public synchronized void releaseSubscription(final Subscription subscription)
    {
        verifyDriverIsActive();

        activeSubscriptions.remove(subscription);
        activeCorrelationId = driverProxy.removeSubscription(subscription.registrationId());

        awaitOperationSucceeded();
    }

    public void onNewPublication(
            final String channel,
            final int streamId,
            final int sessionId,
            final int termId,
            final int limitPositionIndicatorOffset,
            final ReadyFlyweight message,
            final long correlationId,
            final int mtuLength)
    {
        final LogAppender[] logs = new LogAppender[BUFFER_COUNT];
        final ManagedBuffer[] managedBuffers = new ManagedBuffer[BUFFER_COUNT * 2];

        for (int i = 0; i < BUFFER_COUNT; i++)
        {
            final ManagedBuffer logBuffer = mapBuffer(message, i);
            final ManagedBuffer stateBuffer = mapBuffer(message, i + TermHelper.BUFFER_COUNT);
            final byte[] header = DataHeaderFlyweight.createDefaultHeader(sessionId, streamId, termId);

            logs[i] = new LogAppender(logBuffer.buffer(), stateBuffer.buffer(), header, mtuLength);
            managedBuffers[i * 2] = logBuffer;
            managedBuffers[i * 2 + 1] = stateBuffer;
        }

        final PositionIndicator limit = new BufferPositionIndicator(counterValuesBuffer, limitPositionIndicatorOffset);
        addedPublication = new Publication(
            this, channel, streamId, sessionId, termId, logs, limit, managedBuffers, correlationId);

        correlationSignal.signal();
    }

    public void onNewConnection(
        final String channel,
        final int streamId,
        final int sessionId,
        final int initialTermId,
        final long initialPosition,
        final ConnectionReadyFlyweight message,
        final long correlationId)
    {
        activeSubscriptions.forEach(channel, streamId,
            (subscription) ->
            {
                if (null != subscription && !subscription.isConnected(sessionId))
                {
                    PositionReporter positionReporter = null;
                    for (int i = 0, size = message.positionIndicatorCount(); i < size; i++)
                    {
                        if (subscription.registrationId() == message.positionIndicatorRegistrationId(i))
                        {
                            positionReporter = new BufferPositionReporter(
                                counterValuesBuffer, message.positionIndicatorCounterId(i));

                            break;
                        }
                    }

                    if (null != positionReporter)
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

                        subscription.onConnectionReady(
                            sessionId, initialTermId, initialPosition, correlationId, logs, positionReporter, managedBuffers);

                        if (null != newConnectionHandler)
                        {
                            newConnectionHandler.onNewConnection(channel, streamId, sessionId, message.sourceInfo());
                        }
                    }
                }
            });
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

    public void onInactiveConnection(
        final String channel,
        final int streamId,
        final int sessionId,
        final ConnectionMessageFlyweight message,
        final long correlationId)
    {
        activeSubscriptions.forEach(channel, streamId,
            (subscription) ->
            {
                if (subscription.removeConnection(sessionId, correlationId))
                {
                    if (null != inactiveConnectionHandler)
                    {
                        inactiveConnectionHandler.onInactiveConnection(channel, streamId, sessionId);
                    }
                }
            });
    }

    private void await(final long startTime)
    {
        correlationSignal.await(driverTimeoutMs);
        checkDriverTimeout(startTime);
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

    private void checkDriverTimeout(final long startTime)
    {
        if ((System.currentTimeMillis() - startTime) > driverTimeoutMs)
        {
            final String msg = String.format("No response from media driver within %d ms", driverTimeoutMs);
            throw new DriverTimeoutException(msg);
        }
    }

    private ManagedBuffer mapBuffer(final ReadyFlyweight logBuffersMessage, final int index)
    {
        final String location = logBuffersMessage.location(index);
        final int offset = logBuffersMessage.bufferOffset(index);
        final int length = logBuffersMessage.bufferLength(index);

        return bufferManager.newBuffer(location, offset, length);
    }

    private int processTimers()
    {
        int workCount = 0;

        if (timerWheel.calculateDelayInMs() <= 0)
        {
            workCount = timerWheel.expireTimers();
        }

        return workCount;
    }

    private void onKeepalive()
    {
        driverProxy.sendClientKeepalive();
        checkDriverHeartbeat();

        timerWheel.rescheduleTimeout(KEEPALIVE_TIMEOUT_MS, TimeUnit.MILLISECONDS, keepaliveTimer);
    }

    private void checkDriverHeartbeat()
    {
        final long now = timerWheel.clock().time();
        final long currentDriverKeepaliveTime = driverProxy.timeOfLastDriverKeepaliveNs();

        if (driverActive && (now > (currentDriverKeepaliveTime + driverTimeoutNs)))
        {
            driverActive = false;

            final String msg = String.format("Driver has been inactive for over %dms", driverTimeoutMs);
            exceptionHandler().accept(new DriverTimeoutException(msg));
        }
    }

    private void verifyDriverIsActive()
    {
        if (!driverActive)
        {
            throw new DriverTimeoutException("Driver is inactive");
        }
    }
}
