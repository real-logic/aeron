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

import io.aeron.exceptions.*;
import org.agrona.*;
import org.agrona.collections.*;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.UnsafeBufferPosition;

import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.*;

import static io.aeron.ClientConductor.Status.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

/**
 * Client conductor takes responses and notifications from Media Driver and acts on them in addition to forwarding
 * commands from the various Client APIs to the Media Driver.
 */
class ClientConductor implements Agent, DriverListener
{
    enum Status
    {
        ACTIVE, CLOSING, CLOSED
    }

    private static final long NO_CORRELATION_ID = -1;
    private static final long RESOURCE_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);
    private static final long RESOURCE_LINGER_NS = TimeUnit.SECONDS.toNanos(5);

    private final long keepAliveIntervalNs;
    private final long driverTimeoutMs;
    private final long driverTimeoutNs;
    private final long interServiceTimeoutNs;
    private final long publicationConnectionTimeoutMs;
    private long timeOfLastKeepaliveNs;
    private long timeOfLastCheckResourcesNs;
    private long timeOfLastWorkNs;
    private boolean isDriverActive = true;
    private Status status = ACTIVE;

    private final Lock lock = new ReentrantLock();
    private final Aeron.Context ctx;
    private final EpochClock epochClock;
    private final FileChannel.MapMode imageMapMode;
    private final NanoClock nanoClock;
    private final DriverListenerAdapter driverListener;
    private final LogBuffersFactory logBuffersFactory;
    private final ActivePublications activePublications = new ActivePublications();
    private final Long2ObjectHashMap<ExclusivePublication> activeExclusivePublications = new Long2ObjectHashMap<>();
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

        epochClock = ctx.epochClock();
        nanoClock = ctx.nanoClock();
        errorHandler = ctx.errorHandler();
        counterValuesBuffer = ctx.countersValuesBuffer();
        driverProxy = ctx.driverProxy();
        logBuffersFactory = ctx.logBuffersFactory();
        availableImageHandler = ctx.availableImageHandler();
        unavailableImageHandler = ctx.unavailableImageHandler();
        imageMapMode = ctx.imageMapMode();
        keepAliveIntervalNs = ctx.keepAliveInterval();
        driverTimeoutMs = ctx.driverTimeoutMs();
        driverTimeoutNs = MILLISECONDS.toNanos(driverTimeoutMs);
        interServiceTimeoutNs = ctx.interServiceTimeout();
        publicationConnectionTimeoutMs = ctx.publicationConnectionTimeout();
        driverListener = new DriverListenerAdapter(ctx.toClientBuffer(), this);

        final long nowNs = nanoClock.nanoTime();
        timeOfLastKeepaliveNs = nowNs;
        timeOfLastCheckResourcesNs = nowNs;
        timeOfLastWorkNs = nowNs;
    }

    public void onClose()
    {
        if (ACTIVE == status)
        {
            status = CLOSING;

            activeExclusivePublications
                .values()
                .stream()
                .collect(toList())
                .forEach(ExclusivePublication::release);

            activePublications.close();
            activeSubscriptions.close();

            Thread.yield();

            lingeringResources.forEach(ManagedResource::delete);
            ctx.close();

            status = CLOSED;
        }
    }

    public int doWork()
    {
        int workCount = 0;

        if (lock.tryLock())
        {
            try
            {
                if (CLOSED != status)
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

    ExclusivePublication addExclusivePublication(final String channel, final int streamId)
    {
        verifyActive();

        final long registrationId = driverProxy.addExclusivePublication(channel, streamId);
        awaitResponse(registrationId, channel);

        return activeExclusivePublications.get(registrationId);
    }

    void releasePublication(final Publication publication)
    {
        verifyActive();

        if (publication == activePublications.remove(publication.channel(), publication.streamId()))
        {
            lingerResource(publication.managedResource());
            awaitResponse(driverProxy.removePublication(publication.registrationId()), null);
        }
    }

    void releasePublication(final ExclusivePublication publication)
    {
        verifyActive();

        if (publication == activeExclusivePublications.remove(publication.registrationId()))
        {
            lingerResource(publication.managedResource());
            awaitResponse(driverProxy.removePublication(publication.registrationId()), null);
        }
    }

    // TODO: remove redundant method?
    Subscription addSubscription(
        final String channel,
        final int streamId)
    {
        return addSubscription(channel, streamId, image -> {}, image -> {});
    }

    Subscription addSubscription(
        final String channel,
        final int streamId,
        final AvailableImageHandler availableImageHandler,
        final UnavailableImageHandler unavailableImageHandler)
    {
        verifyActive();

        final long correlationId = driverProxy.addSubscription(channel, streamId);
        final Subscription subscription =
            new Subscription(
                this,
                channel,
                streamId,
                correlationId,
                availableImageHandler,
                unavailableImageHandler);
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

    void addDestination(final long registrationId, final String endpointChannel)
    {
        verifyActive();

        awaitResponse(driverProxy.addDestination(registrationId, endpointChannel), null);
    }

    void removeDestination(final long registrationId, final String endpointChannel)
    {
        verifyActive();

        awaitResponse(driverProxy.removeDestination(registrationId, endpointChannel), null);
    }

    public void onError(final ErrorCode errorCode, final String message, final long correlationId)
    {
        driverException = new RegistrationException(errorCode, message);
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

    public void onNewExclusivePublication(
        final String channel,
        final int streamId,
        final int sessionId,
        final int publicationLimitId,
        final String logFileName,
        final long correlationId)
    {
        final ExclusivePublication publication = new ExclusivePublication(
            this,
            channel,
            streamId,
            sessionId,
            new UnsafeBufferPosition(counterValuesBuffer, publicationLimitId),
            logBuffersFactory.map(logFileName, FileChannel.MapMode.READ_WRITE),
            correlationId);

        activeExclusivePublications.put(correlationId, publication);
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

    boolean isPublicationConnected(final long timeOfLastStatusMessageMs)
    {
        return epochClock.time() <= (timeOfLastStatusMessageMs + publicationConnectionTimeoutMs);
    }

    UnavailableImageHandler unavailableImageHandler()
    {
        return unavailableImageHandler;
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
        final long timeoutDeadlineNs = nanoClock.nanoTime() + driverTimeoutNs;

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
        while (nanoClock.nanoTime() < timeoutDeadlineNs);

        throw new DriverTimeoutException("No response within driver timeout");
    }

    private void verifyActive()
    {
        if (!isDriverActive)
        {
            throw new DriverTimeoutException("MediaDriver is inactive");
        }

        if (CLOSED == status)
        {
            throw new IllegalStateException("Aeron client is closed");
        }
    }

    private int onCheckTimeouts()
    {
        final long nowNs = nanoClock.nanoTime();
        int result = 0;

        if (nowNs > (timeOfLastWorkNs + interServiceTimeoutNs))
        {
            onClose();

            throw new ConductorServiceTimeoutException(
                "Timeout between service calls over " + interServiceTimeoutNs + "ns");
        }

        timeOfLastWorkNs = nowNs;

        if (nowNs > (timeOfLastKeepaliveNs + keepAliveIntervalNs))
        {
            driverProxy.sendClientKeepalive();
            checkDriverHeartbeat();

            timeOfLastKeepaliveNs = nowNs;
            result++;
        }

        if (nowNs > (timeOfLastCheckResourcesNs + RESOURCE_TIMEOUT_NS))
        {
            final ArrayList<ManagedResource> lingeringResources = this.lingeringResources;
            for (int lastIndex = lingeringResources.size() - 1, i = lastIndex; i >= 0; i--)
            {
                final ManagedResource resource = lingeringResources.get(i);
                if (nowNs > (resource.timeOfLastStateChange() + RESOURCE_LINGER_NS))
                {
                    ArrayListUtil.fastUnorderedRemove(lingeringResources, i, lastIndex);
                    lastIndex--;
                    resource.delete();
                }
            }

            timeOfLastCheckResourcesNs = nowNs;
            result++;
        }

        return result;
    }

    private void checkDriverHeartbeat()
    {
        final long lastDriverKeepalive = driverProxy.timeOfLastDriverKeepalive();
        final long timeoutDeadlineMs = lastDriverKeepalive + driverTimeoutMs;
        if (isDriverActive && (epochClock.time() > timeoutDeadlineMs))
        {
            isDriverActive = false;

            final String msg = "MediaDriver has been inactive for over " + driverTimeoutMs + "ms";
            errorHandler.onError(new DriverTimeoutException(msg));
        }
    }
}
