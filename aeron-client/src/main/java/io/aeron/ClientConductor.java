/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;
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
    private volatile boolean driverActive = true;

    private final Lock lock = new ReentrantLock();
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
        final FileChannel.MapMode imageMapMode,
        final long keepAliveIntervalNs,
        final long driverTimeoutMs,
        final long interServiceTimeoutNs,
        final long publicationConnectionTimeoutMs)
    {
        this.epochClock = epochClock;
        this.nanoClock = nanoClock;
        this.timeOfLastKeepalive = nanoClock.nanoTime();
        this.timeOfLastCheckResources = nanoClock.nanoTime();
        this.timeOfLastWork = nanoClock.nanoTime();
        this.errorHandler = errorHandler;
        this.counterValuesBuffer = counterValuesBuffer;
        this.driverProxy = driverProxy;
        this.logBuffersFactory = logBuffersFactory;
        this.availableImageHandler = availableImageHandler;
        this.unavailableImageHandler = unavailableImageHandler;
        this.imageMapMode = imageMapMode;
        this.keepAliveIntervalNs = keepAliveIntervalNs;
        this.driverTimeoutMs = driverTimeoutMs;
        this.driverTimeoutNs = MILLISECONDS.toNanos(driverTimeoutMs);
        this.interServiceTimeoutNs = interServiceTimeoutNs;
        this.publicationConnectionTimeoutMs = publicationConnectionTimeoutMs;

        this.driverListener = new DriverListenerAdapter(broadcastReceiver, this);
    }

    public void onClose()
    {
        lock.lock();
        try
        {
            activePublications.close();
            activeSubscriptions.close();

            Thread.yield();

            lingeringResources.forEach(ManagedResource::delete);
        }
        finally
        {
            lock.unlock();
        }
    }

    public Lock mainLock()
    {
        return lock;
    }

    public int doWork()
    {
        if (!lock.tryLock())
        {
            return 0;
        }

        try
        {
            return doWork(NO_CORRELATION_ID, null);
        }
        finally
        {
            lock.unlock();
        }
    }

    public String roleName()
    {
        return "aeron-client-conductor";
    }

    Publication addPublication(final String channel, final int streamId)
    {
        verifyDriverIsActive();

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
        verifyDriverIsActive();

        if (publication == activePublications.remove(publication.channel(), publication.streamId()))
        {
            lingerResource(publication.managedResource());
            awaitResponse(driverProxy.removePublication(publication.registrationId()), publication.channel());
        }
    }

    Subscription addSubscription(final String channel, final int streamId)
    {
        verifyDriverIsActive();

        final long correlationId = driverProxy.addSubscription(channel, streamId);
        final Subscription subscription = new Subscription(this, channel, streamId, correlationId);
        activeSubscriptions.add(subscription);

        awaitResponse(correlationId, channel);

        return subscription;
    }

    void releaseSubscription(final Subscription subscription)
    {
        verifyDriverIsActive();

        awaitResponse(driverProxy.removeSubscription(subscription.registrationId()), subscription.channel());

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
