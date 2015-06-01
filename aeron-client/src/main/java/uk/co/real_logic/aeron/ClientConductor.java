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
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.TermAppender;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.TermReader;
import uk.co.real_logic.aeron.exceptions.DriverTimeoutException;
import uk.co.real_logic.aeron.exceptions.RegistrationException;
import uk.co.real_logic.agrona.TimerWheel;
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.broadcast.CopyBroadcastReceiver;
import uk.co.real_logic.agrona.concurrent.status.UnsafeBufferPosition;

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

    private final long driverTimeoutMs;
    private final long driverTimeoutNs;
    private final DriverListenerAdapter driverListenerAdapter;
    private final LogBuffersFactory logBuffersFactory;
    private final ConnectionMap<String, Publication> publicationMap = new ConnectionMap<>();
    private final ActiveSubscriptions activeSubscriptions = new ActiveSubscriptions();

    private final UnsafeBuffer counterValuesBuffer;
    private final DriverProxy driverProxy;
    private final TimerWheel timerWheel;
    private final TimerWheel.Timer keepaliveTimer;
    private final Consumer<Throwable> errorHandler;
    private final NewConnectionHandler newConnectionHandler;
    private final InactiveConnectionHandler inactiveConnectionHandler;

    private long lastReceivedCorrelationId = NO_CORRELATION_ID;
    private long activeCorrelationId = NO_CORRELATION_ID;

    private volatile boolean driverActive = true;
    private Publication addedPublication; // Guarded by this
    private String addedChannel; // Guarded by this
    private RegistrationException registrationException; // Guarded by this

    public ClientConductor(
        final CopyBroadcastReceiver broadcastReceiver,
        final LogBuffersFactory logBuffersFactory,
        final UnsafeBuffer counterValuesBuffer,
        final DriverProxy driverProxy,
        final TimerWheel timerWheel,
        final Consumer<Throwable> errorHandler,
        final NewConnectionHandler newConnectionHandler,
        final InactiveConnectionHandler inactiveConnectionHandler,
        final long driverTimeoutMs)
    {
        this.errorHandler = errorHandler;
        this.counterValuesBuffer = counterValuesBuffer;
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

    public synchronized int doWork()
    {
        int workCount = 0;

        workCount += processTimers();
        workCount += driverListenerAdapter.pollMessage();

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
            addedChannel = channel;
            activeCorrelationId = driverProxy.addPublication(channel, streamId, sessionId);
            final long timeout = timerWheel.clock().time() + driverTimeoutNs;

            doWorkUntil(activeCorrelationId, timeout);

            /*
             * TODO: avoid having addedPublication by storing it in the publicationMap and using a get
             * (only if correlationId was used)
             */

            publication = addedPublication;
            publicationMap.put(channel, sessionId, streamId, publication);
            addedChannel = null;
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

        final long correlationId = driverProxy.removePublication(publication.registrationId());
        publicationMap.remove(publication.channel(), publication.sessionId(), publication.streamId());
        final long timeout = timerWheel.clock().time() + driverTimeoutNs;

        doWorkUntil(correlationId, timeout);
    }

    public synchronized Subscription addSubscription(final String channel, final int streamId, final DataHandler handler)
    {
        verifyDriverIsActive();

        final long correlationId = driverProxy.addSubscription(channel, streamId);
        final long timeout = timerWheel.clock().time() + driverTimeoutNs;

        final Subscription subscription = new Subscription(this, handler, channel, streamId, correlationId);
        activeSubscriptions.add(subscription);

        doWorkUntil(correlationId, timeout);

        return subscription;
    }

    public synchronized void releaseSubscription(final Subscription subscription)
    {
        verifyDriverIsActive();

        final long correlationId = driverProxy.removeSubscription(subscription.registrationId());
        final long timeout = timerWheel.clock().time() + driverTimeoutNs;

        doWorkUntil(correlationId, timeout);

        activeSubscriptions.remove(subscription);
    }

    public void onNewPublication(
        final int streamId,
        final int sessionId,
        final int publicationLimitId,
        final String logFileName,
        final long correlationId)
    {
        if (correlationId == activeCorrelationId)
        {
            final LogBuffers logBuffers = logBuffersFactory.map(logFileName);
            final UnsafeBuffer[] buffers = logBuffers.atomicBuffers();
            final TermAppender[] appenders = new TermAppender[PARTITION_COUNT];
            final UnsafeBuffer logMetaDataBuffer = logBuffers.atomicBuffers()[LogBufferDescriptor.LOG_META_DATA_SECTION_INDEX];
            final UnsafeBuffer[] defaultFrameHeaders = LogBufferDescriptor.defaultFrameHeaders(logMetaDataBuffer);
            final int mtuLength = LogBufferDescriptor.mtuLength(logMetaDataBuffer);

            for (int i = 0; i < PARTITION_COUNT; i++)
            {
                appenders[i] = new TermAppender(buffers[i], buffers[i + PARTITION_COUNT], defaultFrameHeaders[i], mtuLength);
            }

            final UnsafeBufferPosition publicationLimit = new UnsafeBufferPosition(counterValuesBuffer, publicationLimitId);

            addedPublication = new Publication(
                this,
                addedChannel,
                streamId,
                sessionId,
                appenders,
                publicationLimit,
                logBuffers,
                logMetaDataBuffer,
                correlationId);
        }

        lastReceivedCorrelationId = correlationId;
    }

    public void onNewConnection(
        final int streamId,
        final int sessionId,
        final long joiningPosition,
        final String logFileName,
        final ConnectionBuffersReadyFlyweight msg,
        final long correlationId)
    {
        activeSubscriptions.forEach(
            streamId,
            (subscription) ->
            {
                if (!subscription.isConnected(sessionId))
                {
                    for (int i = 0, size = msg.subscriberPositionCount(); i < size; i++)
                    {
                        if (subscription.registrationId() == msg.positionIndicatorRegistrationId(i))
                        {
                            final UnsafeBufferPosition position = new UnsafeBufferPosition(
                                counterValuesBuffer, msg.subscriberPositionId(i));

                            final LogBuffers logBuffers = logBuffersFactory.map(logFileName);
                            final UnsafeBuffer[] buffers = logBuffers.atomicBuffers();
                            final TermReader[] readers = new TermReader[PARTITION_COUNT];
                            final int initialTermId = LogBufferDescriptor.initialTermId(buffers[buffers.length - 1]);

                            for (int p = 0; p < PARTITION_COUNT; p++)
                            {
                                readers[p] = new TermReader(initialTermId, buffers[p]);
                            }

                            subscription.onConnectionReady(
                                sessionId, joiningPosition, correlationId, readers, position, logBuffers);

                            if (null != newConnectionHandler)
                            {
                                final String info = msg.sourceInfo();
                                newConnectionHandler.onNewConnection(
                                    subscription.channel(), streamId, sessionId, joiningPosition, info);
                            }

                            break;
                        }
                    }
                }
            });
    }

    public void onError(final ErrorCode errorCode, final String message, final long correlationId)
    {
        registrationException = new RegistrationException(errorCode, message);
        lastReceivedCorrelationId = correlationId;
    }

    public void operationSucceeded(final long correlationId)
    {
        lastReceivedCorrelationId = correlationId;
    }

    public void onInactiveConnection(
        final int streamId,
        final int sessionId,
        final long position,
        final long correlationId)
    {
        activeSubscriptions.forEach(
            streamId,
            (subscription) ->
            {
                if (subscription.removeConnection(sessionId, correlationId))
                {
                    if (null != inactiveConnectionHandler)
                    {
                        inactiveConnectionHandler.onInactiveConnection(subscription.channel(), streamId, sessionId, position);
                    }
                }
            });
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

    private void doWorkUntil(final long correlationId, final long timeout)
    {
        registrationException = null;
        lastReceivedCorrelationId = NO_CORRELATION_ID;

        do
        {
            doWork();

            if (lastReceivedCorrelationId == correlationId)
            {
                if (null != registrationException)
                {
                    throw registrationException;
                }

                return;
            }

        } while (timerWheel.clock().time() < timeout);

        throw new DriverTimeoutException("No response from driver within timeout");
    }
}
