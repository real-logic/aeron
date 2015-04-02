/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

import uk.co.real_logic.aeron.common.ErrorCode;
import uk.co.real_logic.aeron.common.collections.ConnectionMap;
import uk.co.real_logic.aeron.common.command.ConnectionBuffersReadyFlyweight;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.TermReader;
import uk.co.real_logic.aeron.exceptions.DriverTimeoutException;
import uk.co.real_logic.aeron.exceptions.RegistrationException;
import uk.co.real_logic.agrona.TimerWheel;
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.agrona.concurrent.Signal;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.broadcast.CopyBroadcastReceiver;
import uk.co.real_logic.agrona.status.BufferPositionIndicator;
import uk.co.real_logic.agrona.status.BufferPositionReporter;
import uk.co.real_logic.agrona.status.PositionIndicator;
import uk.co.real_logic.agrona.status.PositionReporter;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.PARTITION_COUNT;

/**
 * Client conductor takes responses and notifications from media driver and acts on them.
 * As well as passes commands to the media driver.
 */
class ClientConductor implements Agent, DriverListener
{
    private static final int KEEPALIVE_TIMEOUT_MS = 500;
    private static final long NO_CORRELATION_ID = -1;

    private final DriverListenerAdapter driverListenerAdapter;
    private final LogBuffersFactory logBuffersFactory;
    private final long driverTimeoutMs;
    private final long driverTimeoutNs;
    private final ConnectionMap<String, Publication> publicationMap = new ConnectionMap<>(); // Guarded by this
    private final ActiveSubscriptions activeSubscriptions = new ActiveSubscriptions();

    private final UnsafeBuffer counterValuesBuffer;
    private final DriverProxy driverProxy;
    private final Signal correlationSignal;
    private final TimerWheel timerWheel;
    private final Consumer<Throwable> errorHandler;

    private final NewConnectionHandler newConnectionHandler;
    private final InactiveConnectionHandler inactiveConnectionHandler;

    private long activeCorrelationId = -1; // Guarded by this
    private Publication addedPublication; // Guarded by this
    private boolean operationSucceeded = false; // Guarded by this
    private RegistrationException registrationException; // Guarded by this

    private final TimerWheel.Timer keepaliveTimer;
    private volatile boolean driverActive = true;

    public ClientConductor(
        final CopyBroadcastReceiver broadcastReceiver,
        final LogBuffersFactory logBuffersFactory,
        final UnsafeBuffer counterValuesBuffer,
        final DriverProxy driverProxy,
        final Signal correlationSignal,
        final TimerWheel timerWheel,
        final Consumer<Throwable> errorHandler,
        final NewConnectionHandler newConnectionHandler,
        final InactiveConnectionHandler inactiveConnectionHandler,
        final long driverTimeoutMs)
    {
        this.errorHandler = errorHandler;
        this.counterValuesBuffer = counterValuesBuffer;
        this.correlationSignal = correlationSignal;
        this.driverProxy = driverProxy;
        this.logBuffersFactory = logBuffersFactory;
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

    public String roleName()
    {
        return "client-conductor";
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
        final int limitPositionIndicatorOffset,
        final int mtuLength,
        final String logFileName,
        final long correlationId)
    {
        final LogBuffers logBuffers = logBuffersFactory.map(logFileName);
        final UnsafeBuffer[] buffers = logBuffers.atomicBuffers();
        final LogAppender[] appenders = new LogAppender[PARTITION_COUNT];
        final UnsafeBuffer logMetaDataBuffer = logBuffers.atomicBuffers()[LogBufferDescriptor.LOG_META_DATA_SECTION_INDEX];
        final UnsafeBuffer[] defaultFrameHeaders = LogBufferDescriptor.defaultFrameHeaders(logMetaDataBuffer);

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            appenders[i] = new LogAppender(buffers[i], buffers[i + PARTITION_COUNT], defaultFrameHeaders[i], mtuLength);
        }

        final PositionIndicator limit = new BufferPositionIndicator(counterValuesBuffer, limitPositionIndicatorOffset);

        addedPublication = new Publication(
            this, channel, streamId, sessionId, appenders, limit, logBuffers, logMetaDataBuffer, correlationId);

        correlationSignal.signal();
    }

    public void onNewConnection(
        final String channel,
        final int streamId,
        final int sessionId,
        final long initialPosition,
        final String logFileName,
        final ConnectionBuffersReadyFlyweight message,
        final long correlationId)
    {
        activeSubscriptions.forEach(
            channel,
            streamId,
            (subscription) ->
            {
                if (!subscription.isConnected(sessionId))
                {
                    for (int i = 0, size = message.positionIndicatorCount(); i < size; i++)
                    {
                        if (subscription.registrationId() == message.positionIndicatorRegistrationId(i))
                        {
                            final PositionReporter positionReporter = new BufferPositionReporter(
                                counterValuesBuffer, message.positionIndicatorCounterId(i));

                            final LogBuffers logBuffers = logBuffersFactory.map(logFileName);
                            final UnsafeBuffer[] buffers = logBuffers.atomicBuffers();
                            final TermReader[] readers = new TermReader[PARTITION_COUNT];

                            for (int p = 0; p < PARTITION_COUNT; p++)
                            {
                                readers[p] = new TermReader(buffers[p]);
                            }

                            subscription.onConnectionReady(
                                sessionId, initialPosition, correlationId, readers, positionReporter, logBuffers);

                            if (null != newConnectionHandler)
                            {
                                newConnectionHandler.onNewConnection(channel, streamId, sessionId, message.sourceInfo());
                            }

                            break;
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

    public void onInactiveConnection(final String channel, final int streamId, final int sessionId, final long correlationId)
    {
        activeSubscriptions.forEach(
            channel,
            streamId,
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

    private int processTimers()
    {
        int workCount = 0;

        if (timerWheel.computeDelayInMs() <= 0)
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
            errorHandler.accept(new DriverTimeoutException(msg));
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
