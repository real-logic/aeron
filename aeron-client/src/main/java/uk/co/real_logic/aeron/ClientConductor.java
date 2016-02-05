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

import uk.co.real_logic.aeron.exceptions.ConductorServiceTimeoutException;
import uk.co.real_logic.aeron.exceptions.DriverTimeoutException;
import uk.co.real_logic.aeron.exceptions.RegistrationException;
import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.ManagedResource;
import uk.co.real_logic.agrona.collections.Long2LongHashMap;
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.agrona.concurrent.EpochClock;
import uk.co.real_logic.agrona.concurrent.NanoClock;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.broadcast.CopyBroadcastReceiver;
import uk.co.real_logic.agrona.concurrent.status.UnsafeBufferPosition;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static uk.co.real_logic.aeron.DriverListenerAdapter.MISSING_REGISTRATION_ID;

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
    private long timeOfLastDoWork;
    private volatile boolean driverActive = true;

    private final EpochClock epochClock;
    private final NanoClock nanoClock;
    private final DriverListenerAdapter driverListener;
    private final LogBuffersFactory logBuffersFactory;
    private final ActivePublications activePublications = new ActivePublications();
    private final ActiveSubscriptions activeSubscriptions = new ActiveSubscriptions();
    private final ArrayList<ManagedResource> managedResources = new ArrayList<>();
    private final UnsafeBuffer counterValuesBuffer;
    private final DriverProxy driverProxy;
    private final ErrorHandler errorHandler;
    private final AvailableImageHandler availableImageHandler;
    private final UnavailableImageHandler unavailableImageHandler;

    private RegistrationException driverException;

    ClientConductor(
        final EpochClock epochClock,
        final NanoClock nanoClock,
        final CopyBroadcastReceiver broadcastReceiver,
        final LogBuffersFactory logBuffersFactory,
        final UnsafeBuffer counterValuesBuffer,
        final DriverProxy driverProxy,
        final ErrorHandler errorHandler,
        final AvailableImageHandler availableImageHandler,
        final UnavailableImageHandler unavailableImageHandler,
        final long keepAliveIntervalNs,
        final long driverTimeoutMs,
        final long interServiceTimeoutNs,
        final long publicationConnectionTimeoutMs)
    {
        this.epochClock = epochClock;
        this.nanoClock = nanoClock;
        this.timeOfLastKeepalive = nanoClock.nanoTime();
        this.timeOfLastCheckResources = nanoClock.nanoTime();
        this.timeOfLastDoWork = nanoClock.nanoTime();
        this.errorHandler = errorHandler;
        this.counterValuesBuffer = counterValuesBuffer;
        this.driverProxy = driverProxy;
        this.logBuffersFactory = logBuffersFactory;
        this.availableImageHandler = availableImageHandler;
        this.unavailableImageHandler = unavailableImageHandler;
        this.keepAliveIntervalNs = keepAliveIntervalNs;
        this.driverTimeoutMs = driverTimeoutMs;
        this.driverTimeoutNs = MILLISECONDS.toNanos(driverTimeoutMs);
        this.interServiceTimeoutNs = interServiceTimeoutNs;
        this.publicationConnectionTimeoutMs = publicationConnectionTimeoutMs;

        this.driverListener = new DriverListenerAdapter(broadcastReceiver, this);
    }

    public synchronized void onClose()
    {
        activePublications.close();
        activeSubscriptions.close();
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

    public synchronized Publication addPublication(final String channel, final int streamId)
    {
        verifyDriverIsActive();

        Publication publication = activePublications.get(channel, streamId);
        if (publication == null)
        {
            final long correlationId = driverProxy.addPublication(channel, streamId);
            final long timeout = nanoClock.nanoTime() + driverTimeoutNs;

            doWorkUntil(correlationId, timeout, channel);

            publication = activePublications.get(channel, streamId);
        }

        publication.incRef();

        return publication;
    }

    public synchronized void releasePublication(final Publication publication)
    {
        verifyDriverIsActive();

        final long correlationId = driverProxy.removePublication(publication.registrationId());
        activePublications.remove(publication.channel(), publication.streamId());
        final long timeout = nanoClock.nanoTime() + driverTimeoutNs;

        doWorkUntil(correlationId, timeout, publication.channel());
    }

    public synchronized Subscription addSubscription(final String channel, final int streamId)
    {
        verifyDriverIsActive();

        final long correlationId = driverProxy.addSubscription(channel, streamId);
        final long timeout = nanoClock.nanoTime() + driverTimeoutNs;

        final Subscription subscription = new Subscription(this, channel, streamId, correlationId);
        activeSubscriptions.add(subscription);

        doWorkUntil(correlationId, timeout, channel);

        return subscription;
    }

    public synchronized void releaseSubscription(final Subscription subscription)
    {
        verifyDriverIsActive();

        final long correlationId = driverProxy.removeSubscription(subscription.registrationId());
        final long timeout = nanoClock.nanoTime() + driverTimeoutNs;

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
        final Publication publication = new Publication(
            this,
            channel,
            streamId,
            sessionId,
            new UnsafeBufferPosition(counterValuesBuffer, publicationLimitId),
            logBuffersFactory.map(logFileName),
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
                if (!subscription.hasImage(sessionId))
                {
                    final long positionId = subscriberPositionMap.get(subscription.registrationId());

                    if (MISSING_REGISTRATION_ID != positionId)
                    {
                        final Image image = new Image(
                            subscription,
                            sessionId,
                            new UnsafeBufferPosition(counterValuesBuffer, (int)positionId),
                            logBuffersFactory.map(logFileName),
                            errorHandler,
                            sourceIdentity,
                            correlationId
                        );

                        subscription.addImage(image);

                        availableImageHandler.onAvailableImage(image);
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
                    unavailableImageHandler.onUnavailableImage(image);
                }
            });
    }

    public DriverListenerAdapter driverListenerAdapter()
    {
        return driverListener;
    }

    public void lingerResource(final ManagedResource managedResource)
    {
        managedResource.timeOfLastStateChange(nanoClock.nanoTime());
        managedResources.add(managedResource);
    }

    public boolean isPublicationConnected(final long timeOfLastSm)
    {
        return (epochClock.time() <= (timeOfLastSm + publicationConnectionTimeoutMs));
    }

    private void checkDriverHeartbeat()
    {
        final long now = epochClock.time();
        final long currentDriverKeepaliveTime = driverProxy.timeOfLastDriverKeepalive();

        if (driverActive && (now > (currentDriverKeepaliveTime + driverTimeoutMs)))
        {
            driverActive = false;

            final String msg = String.format("Driver has been inactive for over %dms", driverTimeoutMs);
            errorHandler.onError(new DriverTimeoutException(msg));
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

        try
        {
            workCount += onCheckTimeouts();
            workCount += driverListener.pollMessage(correlationId, expectedChannel);
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
        }

        return workCount;
    }

    private void doWorkUntil(final long correlationId, final long timeout, final String expectedChannel)
    {
        driverException = null;

        do
        {
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

        throw new DriverTimeoutException("No response from driver within timeout");
    }

    private int onCheckTimeouts()
    {
        final long now = nanoClock.nanoTime();
        int result = 0;

        if (now > (timeOfLastDoWork + interServiceTimeoutNs))
        {
            activePublications.close();
            activeSubscriptions.close();

            throw new ConductorServiceTimeoutException(
                String.format("Timeout between service calls over %dns", interServiceTimeoutNs));
        }

        timeOfLastDoWork = now;

        if (now > (timeOfLastKeepalive + keepAliveIntervalNs))
        {
            driverProxy.sendClientKeepalive();
            checkDriverHeartbeat();

            timeOfLastKeepalive = now;
            result++;
        }

        if (now > (timeOfLastCheckResources + RESOURCE_TIMEOUT_NS))
        {
            for (int i = managedResources.size() - 1; i >= 0; i--)
            {
                final ManagedResource resource = managedResources.get(i);
                if (now > (resource.timeOfLastStateChange() + RESOURCE_LINGER_NS))
                {
                    managedResources.remove(i);
                    resource.delete();
                }
            }

            timeOfLastCheckResources = now;
            result++;
        }

        return result;
    }
}
