/*
 * Copyright 2014-2017 Real Logic Ltd.
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
package io.aeron;

import io.aeron.exceptions.ConductorServiceTimeoutException;
import io.aeron.exceptions.DriverTimeoutException;
import io.aeron.exceptions.RegistrationException;
import org.agrona.ErrorHandler;
import org.agrona.ManagedResource;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.UnsafeBufferPosition;

import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Client conductor takes responses and notifications from media driver and acts on them.
 * As well as passes commands to the media driver.
 */
class ClientConductor implements Agent, DriverListener
{
    private static final long NO_CORRELATION_ID = -1;
    private static final long RESOURCE_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);
    private static final long RESOURCE_LINGER_NS = TimeUnit.SECONDS.toNanos(5);

    private final long keepAliveIntervalNs;
    private final long driverTimeoutMs;
    private final long driverTimeoutNs;
    private final long interServiceTimeoutNs;
    private final long publicationConnectionTimeoutMs;
    private long timeOfLastKeepalive;
    private long timeOfLastCheckResources;
    private long timeOfLastWork;
    private boolean driverActive = true;
    private boolean clientActive = true;

    private final Lock lock = new ReentrantLock();
    private final Aeron.Context ctx;
    private final EpochClock epochClock;
    private final FileChannel.MapMode imageMapMode;
    private final NanoClock nanoClock;
    private final DriverListenerAdapter driverListener;
    private final LogBuffersFactory logBuffersFactory;
    private final ActivePublications activePublications = new ActivePublications();
    private final ActiveSubscriptions activeSubscriptions = new ActiveSubscriptions();
    private final ArrayList<ManagedResource> lingeringResources = new ArrayList<>();
    private final UnsafeBuffer counterValuesBuffer;
    private final DriverProxy driverProxy;
    private final ErrorHandler errorHandler;
    private final AvailableImageHandler availableImageHandler;
    private final UnavailableImageHandler unavailableImageHandler;

    private RegistrationException driverException;

    ClientConductor(final Aeron.Context ctx)
    {
        this.ctx = ctx;
        this.epochClock = ctx.epochClock();
        this.nanoClock = ctx.nanoClock();
        this.timeOfLastKeepalive = nanoClock.nanoTime();
        this.timeOfLastCheckResources = nanoClock.nanoTime();
        this.timeOfLastWork = nanoClock.nanoTime();
        this.errorHandler = ctx.errorHandler();
        this.counterValuesBuffer = ctx.countersValuesBuffer();
        this.driverProxy = ctx.driverProxy();
        this.logBuffersFactory = ctx.logBuffersFactory();
        this.availableImageHandler = ctx.availableImageHandler();
        this.unavailableImageHandler = ctx.unavailableImageHandler();
        this.imageMapMode = ctx.imageMapMode();
        this.keepAliveIntervalNs = ctx.keepAliveInterval();
        this.driverTimeoutMs = ctx.driverTimeoutMs();
        this.driverTimeoutNs = MILLISECONDS.toNanos(driverTimeoutMs);
        this.interServiceTimeoutNs = ctx.interServiceTimeout();
        this.publicationConnectionTimeoutMs = ctx.publicationConnectionTimeout();
        this.driverListener = new DriverListenerAdapter(ctx.toClientBuffer(), this);
    }

    public void onClose()
    {
        if (clientActive)
        {
            activePublications.close();
            activeSubscriptions.close();

            Thread.yield();

            lingeringResources.forEach(ManagedResource::delete);
            ctx.close();
            clientActive = false;
        }
    }

    public int doWork()
    {
        int workCount = 0;

        if (lock.tryLock())
        {
            try
            {
                if (clientActive)
                {
                    workCount = doWork(NO_CORRELATION_ID, null);
                }
            }
            finally
            {
                lock.unlock();
            }
        }

        return workCount;
    }

    public String roleName()
    {
        return "aeron-client-conductor";
    }

    Lock clientLock()
    {
        return lock;
    }

    void handleError(final Throwable ex)
    {
        errorHandler.onError(ex);
    }

    Publication addPublication(final String channel, final int streamId)
    {
        verifyActive();

        Publication publication = activePublications.get(channel, streamId);
        if (null == publication)
        {
            awaitResponse(driverProxy.addPublication(channel, streamId), channel);
            publication = activePublications.get(channel, streamId);
        }

        publication.incRef();

        return publication;
    }

    void releasePublication(final Publication publication)
    {
        verifyActive();

        if (publication == activePublications.remove(publication.channel(), publication.streamId()))
        {
            lingerResource(publication.managedResource());
            awaitResponse(driverProxy.removePublication(publication.registrationId()), publication.channel());
        }
    }

    Subscription addSubscription(final String channel, final int streamId)
    {
        verifyActive();

        final long correlationId = driverProxy.addSubscription(channel, streamId);
        final Subscription subscription = new Subscription(this, channel, streamId, correlationId);
        activeSubscriptions.add(subscription);

        awaitResponse(correlationId, channel);

        return subscription;
    }

    void releaseSubscription(final Subscription subscription)
    {
        verifyActive();

        awaitResponse(driverProxy.removeSubscription(subscription.registrationId()), subscription.channel());

        activeSubscriptions.remove(subscription);
    }

    void addDestination(final Publication publication, final String endpointChannel)
    {
        verifyActive();

        awaitResponse(driverProxy.addDestination(publication.registrationId(), endpointChannel), endpointChannel);
    }

    void removeDestination(final Publication publication, final String endpointChannel)
    {
        verifyActive();

        awaitResponse(driverProxy.removeDestination(publication.registrationId(), endpointChannel), endpointChannel);
    }

    public void onNewPublication(
        final String channel,
        final int streamId,
        final int sessionId,
        final int publicationLimitId,
        final String logFileName,
        final long correlationId)
    {
        final Publication publication = new Publication(
            this,
            channel,
            streamId,
            sessionId,
            new UnsafeBufferPosition(counterValuesBuffer, publicationLimitId),
            logBuffersFactory.map(logFileName, FileChannel.MapMode.READ_WRITE),
            correlationId);

        activePublications.put(channel, streamId, publication);
    }

    public void onAvailableImage(
        final int streamId,
        final int sessionId,
        final Long2LongHashMap subscriberPositionMap,
        final String logFileName,
        final String sourceIdentity,
        final long correlationId)
    {
        activeSubscriptions.forEach(
            streamId,
            (subscription) ->
            {
                if (!subscription.hasImage(correlationId))
                {
                    final long positionId = subscriberPositionMap.get(subscription.registrationId());

                    if (DriverListenerAdapter.MISSING_REGISTRATION_ID != positionId)
                    {
                        final Image image = new Image(
                            subscription,
                            sessionId,
                            new UnsafeBufferPosition(counterValuesBuffer, (int)positionId),
                            logBuffersFactory.map(logFileName, imageMapMode),
                            errorHandler,
                            sourceIdentity,
                            correlationId);

                        subscription.addImage(image);

                        try
                        {
                            availableImageHandler.onAvailableImage(image);
                        }
                        catch (final Throwable ex)
                        {
                            errorHandler.onError(ex);
                        }
                    }
                }
            });
    }

    public void onError(final ErrorCode errorCode, final String message, final long correlationId)
    {
        driverException = new RegistrationException(errorCode, message);
    }

    public void onUnavailableImage(final int streamId, final long correlationId)
    {
        activeSubscriptions.forEach(
            streamId,
            (subscription) ->
            {
                final Image image = subscription.removeImage(correlationId);
                if (null != image)
                {
                    try
                    {
                        unavailableImageHandler.onUnavailableImage(image);
                    }
                    catch (final Throwable ex)
                    {
                        errorHandler.onError(ex);
                    }
                }
            });
    }

    DriverListenerAdapter driverListenerAdapter()
    {
        return driverListener;
    }

    void lingerResource(final ManagedResource managedResource)
    {
        managedResource.timeOfLastStateChange(nanoClock.nanoTime());
        lingeringResources.add(managedResource);
    }

    boolean isPublicationConnected(final long timeOfLastStatusMessage)
    {
        return epochClock.time() <= (timeOfLastStatusMessage + publicationConnectionTimeoutMs);
    }

    UnavailableImageHandler unavailableImageHandler()
    {
        return unavailableImageHandler;
    }

    private void checkDriverHeartbeat()
    {
        final long now = epochClock.time();
        final long currentDriverKeepaliveTime = driverProxy.timeOfLastDriverKeepalive();

        if (driverActive && (now > (currentDriverKeepaliveTime + driverTimeoutMs)))
        {
            driverActive = false;

            final String msg = "Driver has been inactive for over " + driverTimeoutMs + "ms";
            errorHandler.onError(new DriverTimeoutException(msg));
        }
    }

    private void verifyActive()
    {
        if (!driverActive)
        {
            throw new DriverTimeoutException("MediaDriver is inactive");
        }

        if (!clientActive)
        {
            throw new IllegalStateException("Aeron client is closed");
        }
    }

    private int doWork(final long correlationId, final String expectedChannel)
    {
        int workCount = 0;

        try
        {
            workCount += onCheckTimeouts();
            workCount += driverListener.pollMessage(correlationId, expectedChannel);
        }
        catch (final Throwable throwable)
        {
            errorHandler.onError(throwable);
        }

        return workCount;
    }

    private void awaitResponse(final long correlationId, final String expectedChannel)
    {
        driverException = null;
        final long timeout = nanoClock.nanoTime() + driverTimeoutNs;

        do
        {
            LockSupport.parkNanos(1);

            doWork(correlationId, expectedChannel);

            if (driverListener.lastReceivedCorrelationId() == correlationId)
            {
                if (null != driverException)
                {
                    throw driverException;
                }

                return;
            }
        }
        while (nanoClock.nanoTime() < timeout);

        throw new DriverTimeoutException("No response within driver timeout");
    }

    private int onCheckTimeouts()
    {
        final long now = nanoClock.nanoTime();
        int result = 0;

        if (now > (timeOfLastWork + interServiceTimeoutNs))
        {
            onClose();

            throw new ConductorServiceTimeoutException(
                "Timeout between service calls over " + interServiceTimeoutNs + "ns");
        }

        timeOfLastWork = now;

        if (now > (timeOfLastKeepalive + keepAliveIntervalNs))
        {
            driverProxy.sendClientKeepalive();
            checkDriverHeartbeat();

            timeOfLastKeepalive = now;
            result++;
        }

        if (now > (timeOfLastCheckResources + RESOURCE_TIMEOUT_NS))
        {
            final ArrayList<ManagedResource> lingeringResources = this.lingeringResources;
            for (int lastIndex = lingeringResources.size() - 1, i = lastIndex; i >= 0; i--)
            {
                final ManagedResource resource = lingeringResources.get(i);
                if (now > (resource.timeOfLastStateChange() + RESOURCE_LINGER_NS))
                {
                    ArrayListUtil.fastUnorderedRemove(lingeringResources, i, lastIndex);
                    lastIndex--;
                    resource.delete();
                }
            }

            timeOfLastCheckResources = now;
            result++;
        }

        return result;
    }
}
