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

import static io.aeron.Aeron.IDLE_SLEEP_NS;
import static io.aeron.Aeron.sleep;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Client conductor takes responses and notifications from Media Driver and acts on them in addition to forwarding
 * commands from the various Client APIs to the Media Driver.
 */
class ClientConductor implements Agent, DriverListener
{
    private static final long NO_CORRELATION_ID = -1;
    private static final long RESOURCE_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);
    private static final long RESOURCE_LINGER_NS = TimeUnit.SECONDS.toNanos(3);

    private final long keepAliveIntervalNs;
    private final long driverTimeoutMs;
    private final long driverTimeoutNs;
    private final long interServiceTimeoutNs;
    private final long publicationConnectionTimeoutMs;
    private long timeOfLastKeepaliveNs;
    private long timeOfLastCheckResourcesNs;
    private long timeOfLastWorkNs;
    private boolean isDriverActive = true;
    private volatile boolean isClosed;

    private final Lock clientLock;
    private final EpochClock epochClock;
    private final FileChannel.MapMode imageMapMode;
    private final NanoClock nanoClock;
    private final DriverListenerAdapter driverListener;
    private final LogBuffersFactory logBuffersFactory;
    private final ActivePublications activePublications = new ActivePublications();
    private final Long2ObjectHashMap<ExclusivePublication> activeExclusivePublications = new Long2ObjectHashMap<>();
    private final ActiveSubscriptions activeSubscriptions = new ActiveSubscriptions();
    private final ArrayList<ManagedResource> lingeringResources = new ArrayList<>();
    private final UnavailableImageHandler defaultUnavailableImageHandler;
    private final AvailableImageHandler defaultAvailableImageHandler;
    private final UnsafeBuffer counterValuesBuffer;
    private final DriverProxy driverProxy;
    private final ErrorHandler errorHandler;
    private final AgentInvoker driverAgentInvoker;

    private RegistrationException driverException;

    ClientConductor(final Aeron.Context ctx)
    {
        clientLock = ctx.clientLock();
        epochClock = ctx.epochClock();
        nanoClock = ctx.nanoClock();
        errorHandler = ctx.errorHandler();
        counterValuesBuffer = ctx.countersValuesBuffer();
        driverProxy = ctx.driverProxy();
        logBuffersFactory = ctx.logBuffersFactory();
        imageMapMode = ctx.imageMapMode();
        keepAliveIntervalNs = ctx.keepAliveInterval();
        driverTimeoutMs = ctx.driverTimeoutMs();
        driverTimeoutNs = MILLISECONDS.toNanos(driverTimeoutMs);
        interServiceTimeoutNs = ctx.interServiceTimeout();
        publicationConnectionTimeoutMs = ctx.publicationConnectionTimeout();
        defaultAvailableImageHandler = ctx.availableImageHandler();
        defaultUnavailableImageHandler = ctx.unavailableImageHandler();
        driverListener = new DriverListenerAdapter(ctx.toClientBuffer(), this);
        driverAgentInvoker = ctx.driverAgentInvoker();

        final long nowNs = nanoClock.nanoTime();
        timeOfLastKeepaliveNs = nowNs;
        timeOfLastCheckResourcesNs = nowNs;
        timeOfLastWorkNs = nowNs;
    }

    public void onClose()
    {
        if (!isClosed)
        {
            isClosed = true;

            final int lingeringResourcesSize = lingeringResources.size();
            forceClosePublicationsAndSubscriptions();

            if (lingeringResources.size() > lingeringResourcesSize)
            {
                sleep(1);
            }

            for (int i = 0, size = lingeringResources.size(); i < size; i++)
            {
                lingeringResources.get(i).delete();
            }

            lingeringResources.clear();
        }
    }

    public int doWork()
    {
        int workCount = 0;

        if (clientLock.tryLock())
        {
            try
            {
                if (!isClosed)
                {
                    workCount = doWork(NO_CORRELATION_ID, null);
                }
            }
            finally
            {
                clientLock.unlock();
            }
        }

        return workCount;
    }

    public String roleName()
    {
        return "aeron-client-conductor";
    }

    boolean isClosed()
    {
        return isClosed;
    }

    Lock clientLock()
    {
        return clientLock;
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
        if (isClosed)
        {
            throw new IllegalStateException("Aeron client is closed");
        }

        if (publication == activePublications.remove(publication.channel(), publication.streamId()))
        {
            lingerResource(publication.managedResource());
            awaitResponse(driverProxy.removePublication(publication.registrationId()), null);
        }
    }

    void releasePublication(final ExclusivePublication publication)
    {
        if (isClosed)
        {
            throw new IllegalStateException("Aeron client is closed");
        }

        if (publication == activeExclusivePublications.remove(publication.registrationId()))
        {
            lingerResource(publication.managedResource());
            awaitResponse(driverProxy.removePublication(publication.registrationId()), null);
        }
    }

    void asyncReleasePublication(final long registrationId)
    {
        driverProxy.removePublication(registrationId);
    }

    Subscription addSubscription(final String channel, final int streamId)
    {
        verifyActive();

        final long correlationId = driverProxy.addSubscription(channel, streamId);
        final Subscription subscription = new Subscription(
            this, channel, streamId, correlationId, defaultAvailableImageHandler, defaultUnavailableImageHandler);
        activeSubscriptions.add(subscription);

        awaitResponse(correlationId, channel);

        return subscription;
    }

    Subscription addSubscription(
        final String channel,
        final int streamId,
        final AvailableImageHandler availableImageHandler,
        final UnavailableImageHandler unavailableImageHandler)
    {
        verifyActive();

        final long correlationId = driverProxy.addSubscription(channel, streamId);
        final Subscription subscription = new Subscription(
            this, channel, streamId, correlationId, availableImageHandler, unavailableImageHandler);
        activeSubscriptions.add(subscription);

        awaitResponse(correlationId, channel);

        return subscription;
    }

    void releaseSubscription(final Subscription subscription)
    {
        if (isClosed)
        {
            throw new IllegalStateException("Aeron client is closed");
        }

        awaitResponse(driverProxy.removeSubscription(subscription.registrationId()), null);

        activeSubscriptions.remove(subscription);
    }

    void asyncReleaseSubscription(final Subscription subscription)
    {
        driverProxy.removeSubscription(subscription.registrationId());
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

    public void onError(final long correlationId, final ErrorCode errorCode, final String message)
    {
        driverException = new RegistrationException(errorCode, message);
    }

    public void onNewPublication(
        final long correlationId,
        final long registrationId,
        final int streamId,
        final int sessionId,
        final int publicationLimitId,
        final String channel,
        final String logFileName)
    {
        final Publication publication = new Publication(
            this,
            channel,
            streamId,
            sessionId,
            new UnsafeBufferPosition(counterValuesBuffer, publicationLimitId),
            logBuffersFactory.map(logFileName, FileChannel.MapMode.READ_WRITE),
            registrationId,
            correlationId);

        activePublications.put(channel, streamId, publication);
    }

    public void onNewExclusivePublication(
        final long correlationId,
        final long registrationId,
        final int streamId,
        final int sessionId,
        final int publicationLimitId,
        final String channel,
        final String logFileName)
    {
        final ExclusivePublication publication = new ExclusivePublication(
            this,
            channel,
            streamId,
            sessionId,
            new UnsafeBufferPosition(counterValuesBuffer, publicationLimitId),
            logBuffersFactory.map(logFileName, FileChannel.MapMode.READ_WRITE),
            registrationId,
            correlationId);

        activeExclusivePublications.put(correlationId, publication);
    }

    public void onAvailableImage(
        final long correlationId,
        final int streamId,
        final int sessionId,
        final Long2LongHashMap subscriberPositionMap,
        final String logFileName,
        final String sourceIdentity)
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

                        try
                        {
                            final AvailableImageHandler handler = subscription.availableImageHandler();
                            if (null != handler)
                            {
                                handler.onAvailableImage(image);
                            }
                        }
                        catch (final Throwable ex)
                        {
                            errorHandler.onError(ex);
                        }

                        subscription.addImage(image);
                    }
                }
            });
    }

    public void onUnavailableImage(final long correlationId, final int streamId)
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
                        final UnavailableImageHandler handler = subscription.unavailableImageHandler();
                        if (null != handler)
                        {
                            handler.onUnavailableImage(image);
                        }
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

            if (correlationId != NO_CORRELATION_ID)
            {
                // has been called from a user thread and not the conductor duty cycle.
                throw throwable;
            }
        }

        return workCount;
    }

    private void awaitResponse(final long correlationId, final String expectedChannel)
    {
        driverException = null;
        final long deadlineNs = nanoClock.nanoTime() + driverTimeoutNs;

        do
        {
            if (null == driverAgentInvoker)
            {
                sleep(1);
            }
            else
            {
                driverAgentInvoker.invoke();
            }

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
        while (nanoClock.nanoTime() < deadlineNs);

        throw new DriverTimeoutException("No response from driver within timeout");
    }

    private void verifyActive()
    {
        if (isClosed)
        {
            throw new IllegalStateException("Aeron client is closed");
        }

        if (!isDriverActive)
        {
            throw new DriverTimeoutException("MediaDriver is inactive");
        }
    }

    private int onCheckTimeouts()
    {
        int workCount = 0;
        final long nowNs = nanoClock.nanoTime();

        if (nowNs < (timeOfLastWorkNs + IDLE_SLEEP_NS))
        {
            return workCount;
        }

        if (nowNs > (timeOfLastWorkNs + interServiceTimeoutNs))
        {
            final int lingeringResourcesSize = lingeringResources.size();
            forceClosePublicationsAndSubscriptions();
            if (lingeringResources.size() > lingeringResourcesSize)
            {
                sleep(1000);
            }

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
            workCount++;
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
            workCount++;
        }

        return workCount;
    }

    private void checkDriverHeartbeat()
    {
        if (isDriverActive)
        {
            final long deadlineMs = driverProxy.timeOfLastDriverKeepaliveMs() + driverTimeoutMs;
            if ((epochClock.time() > deadlineMs))
            {
                isDriverActive = false;

                try
                {
                    onClose();
                }
                finally
                {
                    errorHandler.onError(new DriverTimeoutException(
                        "MediaDriver has been inactive for over " + driverTimeoutMs + "ms"));
                }
            }
        }
    }

    private void forceClosePublicationsAndSubscriptions()
    {
        for (final ExclusivePublication publication : activeExclusivePublications.values())
        {
            publication.forceClose();
        }
        activeExclusivePublications.clear();

        activePublications.close();
        activeSubscriptions.close();
    }
}
