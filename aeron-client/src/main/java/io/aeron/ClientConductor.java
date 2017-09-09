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
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.UnsafeBufferPosition;

import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static io.aeron.Aeron.IDLE_SLEEP_NS;
import static io.aeron.Aeron.sleep;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Client conductor receives responses and notifications from Media Driver and acts on them in addition to forwarding
 * commands from the Client API to the Media Driver conductor.
 */
class ClientConductor implements Agent, DriverEventsListener
{
    private static final long NO_CORRELATION_ID = -1;
    private static final long RESOURCE_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);
    private static final long RESOURCE_LINGER_NS = TimeUnit.SECONDS.toNanos(3);

    private final long keepAliveIntervalNs;
    private final long driverTimeoutMs;
    private final long driverTimeoutNs;
    private final long interServiceTimeoutNs;
    private final long publicationConnectionTimeoutMs;
    private long timeOfLastKeepAliveNs;
    private long timeOfLastResourcesCheckNs;
    private long timeOfLastServiceNs;
    private volatile boolean isClosed;

    private final Lock clientLock;
    private final EpochClock epochClock;
    private final FileChannel.MapMode imageMapMode;
    private final NanoClock nanoClock;
    private final DriverEventsAdapter driverEventsAdapter;
    private final LogBuffersFactory logBuffersFactory;
    private final ActivePublications activePublications = new ActivePublications();
    private final Long2ObjectHashMap<ExclusivePublication> activeExclusivePublications = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<Subscription> activeSubscriptions = new Long2ObjectHashMap<>();
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
        driverEventsAdapter = new DriverEventsAdapter(ctx.toClientBuffer(), this);
        driverAgentInvoker = ctx.driverAgentInvoker();

        final long nowNs = nanoClock.nanoTime();
        timeOfLastKeepAliveNs = nowNs;
        timeOfLastResourcesCheckNs = nowNs;
        timeOfLastServiceNs = nowNs;
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
        }
    }

    public int doWork()
    {
        int workCount = 0;

        if (clientLock.tryLock())
        {
            try
            {
                if (isClosed)
                {
                    throw new AgentTerminationException();
                }

                workCount = service(NO_CORRELATION_ID, null);
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
        if (isClosed)
        {
            throw new IllegalStateException("Aeron client is closed");
        }

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
        if (isClosed)
        {
            throw new IllegalStateException("Aeron client is closed");
        }

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
        return addSubscription(channel, streamId, defaultAvailableImageHandler, defaultUnavailableImageHandler);
    }

    Subscription addSubscription(
        final String channel,
        final int streamId,
        final AvailableImageHandler availableImageHandler,
        final UnavailableImageHandler unavailableImageHandler)
    {
        if (isClosed)
        {
            throw new IllegalStateException("Aeron client is closed");
        }

        final long correlationId = driverProxy.addSubscription(channel, streamId);
        final Subscription subscription = new Subscription(
            this, channel, streamId, correlationId, availableImageHandler, unavailableImageHandler);

        activeSubscriptions.put(correlationId, subscription);

        awaitResponse(correlationId, channel);

        return subscription;
    }

    void releaseSubscription(final Subscription subscription)
    {
        if (isClosed)
        {
            throw new IllegalStateException("Aeron client is closed");
        }

        final long registrationId = subscription.registrationId();
        awaitResponse(driverProxy.removeSubscription(registrationId), null);
        activeSubscriptions.remove(registrationId);
    }

    void asyncReleaseSubscription(final Subscription subscription)
    {
        driverProxy.removeSubscription(subscription.registrationId());
    }

    void addDestination(final long registrationId, final String endpointChannel)
    {
        if (isClosed)
        {
            throw new IllegalStateException("Aeron client is closed");
        }

        awaitResponse(driverProxy.addDestination(registrationId, endpointChannel), null);
    }

    void removeDestination(final long registrationId, final String endpointChannel)
    {
        if (isClosed)
        {
            throw new IllegalStateException("Aeron client is closed");
        }

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
        final long subscriberRegistrationId,
        final int subscriberPositionId,
        final String logFileName,
        final String sourceIdentity)
    {
        final Subscription subscription = activeSubscriptions.get(subscriberRegistrationId);
        if (null != subscription && subscription.registrationId() == subscriberRegistrationId)
        {
            final Image image = new Image(
                subscription,
                sessionId,
                new UnsafeBufferPosition(counterValuesBuffer, subscriberPositionId),
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

    public void onUnavailableImage(final long correlationId, final int streamId)
    {
        for (final Subscription subscription : activeSubscriptions.values())
        {
            if (subscription.streamId() == streamId)
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
            }
        }
    }

    DriverEventsAdapter driverListenerAdapter()
    {
        return driverEventsAdapter;
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

    private int service(final long correlationId, final String expectedChannel)
    {
        int workCount = 0;

        try
        {
            workCount += onCheckTimeouts();
            workCount += driverEventsAdapter.receive(correlationId, expectedChannel);
        }
        catch (final Throwable throwable)
        {
            errorHandler.onError(throwable);

            if (isClientApiCall(correlationId))
            {
                throw throwable;
            }
        }

        return workCount;
    }

    private static boolean isClientApiCall(final long correlationId)
    {
        return correlationId != NO_CORRELATION_ID;
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

            service(correlationId, expectedChannel);

            if (driverEventsAdapter.lastReceivedCorrelationId() == correlationId)
            {
                if (null != driverException)
                {
                    throw driverException;
                }

                return;
            }
        }
        while (nanoClock.nanoTime() < deadlineNs);

        throw new DriverTimeoutException("No response from MediaDriver within (ns):" + driverTimeoutNs);
    }

    private int onCheckTimeouts()
    {
        int workCount = 0;
        final long nowNs = nanoClock.nanoTime();

        if (nowNs > (timeOfLastServiceNs + IDLE_SLEEP_NS))
        {
            checkServiceInterval(nowNs);
            timeOfLastServiceNs = nowNs;

            workCount += checkLiveness(nowNs);
            workCount += checkLingeringResources(nowNs);
        }

        return workCount;
    }

    private void checkServiceInterval(final long nowNs)
    {
        if (nowNs > (timeOfLastServiceNs + interServiceTimeoutNs))
        {
            final int lingeringResourcesSize = lingeringResources.size();

            forceClosePublicationsAndSubscriptions();

            if (lingeringResources.size() > lingeringResourcesSize)
            {
                sleep(1000);
            }

            onClose();

            throw new ConductorServiceTimeoutException("Exceeded (ns): " + interServiceTimeoutNs);
        }
    }

    private int checkLiveness(final long nowNs)
    {
        if (nowNs > (timeOfLastKeepAliveNs + keepAliveIntervalNs))
        {
            if (epochClock.time() > (driverProxy.timeOfLastDriverKeepaliveMs() + driverTimeoutMs))
            {
                onClose();

                throw new DriverTimeoutException("MediaDriver keepalive older than (ms): " + driverTimeoutMs);
            }

            driverProxy.sendClientKeepalive();
            timeOfLastKeepAliveNs = nowNs;

            return 1;
        }

        return 0;
    }

    private int checkLingeringResources(final long nowNs)
    {
        if (nowNs > (timeOfLastResourcesCheckNs + RESOURCE_TIMEOUT_NS))
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

            timeOfLastResourcesCheckNs = nowNs;

            return 1;
        }

        return 0;
    }

    private void forceClosePublicationsAndSubscriptions()
    {
        for (final ExclusivePublication publication : activeExclusivePublications.values())
        {
            publication.forceClose();
        }
        activeExclusivePublications.clear();

        activePublications.close();

        for (final Subscription subscription : activeSubscriptions.values())
        {
            subscription.forceClose();
        }
        activeSubscriptions.clear();
    }
}
