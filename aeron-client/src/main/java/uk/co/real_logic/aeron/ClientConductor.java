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
import uk.co.real_logic.aeron.common.command.ConnectionBuffersReadyFlyweight;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.TermAppender;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.TermReader;
import uk.co.real_logic.aeron.exceptions.DriverTimeoutException;
import uk.co.real_logic.aeron.exceptions.RegistrationException;
import uk.co.real_logic.agrona.ManagedResource;
import uk.co.real_logic.agrona.TimerWheel;
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.broadcast.CopyBroadcastReceiver;
import uk.co.real_logic.agrona.concurrent.status.UnsafeBufferPosition;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.PARTITION_COUNT;

/**
 * Client conductor takes responses and notifications from media driver and acts on them.
 * As well as passes commands to the media driver.
 */
class ClientConductor implements Agent, DriverListener
{
    private static final long NO_CORRELATION_ID = -1;
    private static final long KEEPALIVE_TIMEOUT_MS = 500;
    private static final long RESOURCE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(1);
    private static final long RESOURCE_LINGER_NS = TimeUnit.SECONDS.toNanos(5);

    private final long driverTimeoutNs;
    private volatile boolean driverActive = true;

    private final DriverListenerAdapter driverListenerAdapter;
    private final LogBuffersFactory logBuffersFactory;
    private final ActivePublications activePublications = new ActivePublications();
    private final ActiveSubscriptions activeSubscriptions = new ActiveSubscriptions();
    private final ArrayList<ManagedResource> managedResources = new ArrayList<>();
    private final UnsafeBuffer counterValuesBuffer;
    private final DriverProxy driverProxy;
    private final TimerWheel timerWheel;
    private final TimerWheel.Timer keepaliveTimer;
    private final TimerWheel.Timer managedResourceTimer;
    private final Consumer<Throwable> errorHandler;
    private final NewConnectionHandler newConnectionHandler;
    private final InactiveConnectionHandler inactiveConnectionHandler;

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
        this.driverTimeoutNs = MILLISECONDS.toNanos(driverTimeoutMs);

        this.driverListenerAdapter = new DriverListenerAdapter(broadcastReceiver, this);
        this.keepaliveTimer = timerWheel.newTimeout(KEEPALIVE_TIMEOUT_MS, MILLISECONDS, this::onKeepalive);
        this.managedResourceTimer = timerWheel.newTimeout(RESOURCE_TIMEOUT_MS, MILLISECONDS, this::checkManagedResources);
    }

    public void onClose()
    {
        managedResources.forEach(ManagedResource::delete);
    }

    public synchronized int doWork()
    {
        return doWork(NO_CORRELATION_ID, null);
    }

    public String roleName()
    {
        return "client-conductor";
    }

    public synchronized Publication addPublication(final String channel, final int streamId, final int sessionId)
    {
        verifyDriverIsActive();

        Publication publication = activePublications.get(channel, sessionId, streamId);
        if (publication == null)
        {
            final long correlationId = driverProxy.addPublication(channel, streamId, sessionId);
            final long timeout = timerWheel.clock().nanoTime() + driverTimeoutNs;

            doWorkUntil(correlationId, timeout, channel);

            publication = activePublications.get(channel, sessionId, streamId);
        }

        publication.incRef();

        return publication;
    }

    public synchronized void releasePublication(final Publication publication)
    {
        verifyDriverIsActive();

        final long correlationId = driverProxy.removePublication(publication.registrationId());
        activePublications.remove(publication.channel(), publication.sessionId(), publication.streamId());
        final long timeout = timerWheel.clock().nanoTime() + driverTimeoutNs;

        doWorkUntil(correlationId, timeout, publication.channel());
    }

    public synchronized Subscription addSubscription(final String channel, final int streamId, final DataHandler handler)
    {
        verifyDriverIsActive();

        final long correlationId = driverProxy.addSubscription(channel, streamId);
        final long timeout = timerWheel.clock().nanoTime() + driverTimeoutNs;

        final Subscription subscription = new Subscription(this, handler, channel, streamId, correlationId);
        activeSubscriptions.add(subscription);

        doWorkUntil(correlationId, timeout, channel);

        return subscription;
    }

    public synchronized void releaseSubscription(final Subscription subscription)
    {
        verifyDriverIsActive();

        final long correlationId = driverProxy.removeSubscription(subscription.registrationId());
        final long timeout = timerWheel.clock().nanoTime() + driverTimeoutNs;

        doWorkUntil(correlationId, timeout, subscription.channel());

        activeSubscriptions.remove(subscription);
    }

    public void onNewPublication(
        final String channel,
        final int streamId,
        final int sessionId,
        final int publicationLimitId,
        final String logFileName,
        final long correlationId)
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

        final Publication publication = new Publication(
            this,
            channel,
            streamId,
            sessionId,
            appenders,
            publicationLimit,
            logBuffers,
            logMetaDataBuffer,
            correlationId);

        activePublications.put(channel, sessionId, streamId, publication);
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
                if (subscription.removeConnection(correlationId))
                {
                    if (null != inactiveConnectionHandler)
                    {
                        inactiveConnectionHandler.onInactiveConnection(subscription.channel(), streamId, sessionId, position);
                    }
                }
            });
    }

    public DriverListenerAdapter driverListenerAdapter()
    {
        return driverListenerAdapter;
    }

    public void lingerResource(final ManagedResource managedResource)
    {
        managedResource.timeOfLastStateChange(timerWheel.clock().nanoTime());
        managedResources.add(managedResource);
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

        timerWheel.rescheduleTimeout(KEEPALIVE_TIMEOUT_MS, MILLISECONDS, keepaliveTimer);
    }

    private void checkManagedResources()
    {
        final long now = timerWheel.clock().nanoTime();

        for (int i = managedResources.size() - 1; i >= 0; i--)
        {
            final ManagedResource resource = managedResources.get(i);
            if (now > (resource.timeOfLastStateChange() + RESOURCE_LINGER_NS))
            {
                managedResources.remove(i);
                resource.delete();
            }
        }

        timerWheel.rescheduleTimeout(RESOURCE_TIMEOUT_MS, MILLISECONDS, managedResourceTimer);
    }

    private void checkDriverHeartbeat()
    {
        final long now = timerWheel.clock().nanoTime();
        final long currentDriverKeepaliveTime = driverProxy.timeOfLastDriverKeepalive();

        if (driverActive && (now > (currentDriverKeepaliveTime + driverTimeoutNs)))
        {
            driverActive = false;

            final String msg = String.format(
                "Driver has been inactive for over %dms", TimeUnit.NANOSECONDS.toMillis(driverTimeoutNs));
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

    private int doWork(final long correlationId, final String expectedChannel)
    {
        int workCount = 0;

        workCount += processTimers();
        workCount += driverListenerAdapter.pollMessage(correlationId, expectedChannel);

        return workCount;
    }

    private void doWorkUntil(final long correlationId, final long timeout, final String expectedChannel)
    {
        registrationException = null;

        do
        {
            doWork(correlationId, expectedChannel);

            if (driverListenerAdapter.lastReceivedCorrelationId() == correlationId)
            {
                if (null != registrationException)
                {
                    throw registrationException;
                }

                return;
            }
        }
        while (timerWheel.clock().nanoTime() < timeout);

        throw new DriverTimeoutException("No response from driver within timeout");
    }
}
