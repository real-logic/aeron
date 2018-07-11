/*
 * Copyright 2014-2018 Real Logic Ltd.
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
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.DirectBuffer;
import org.agrona.ManagedResource;
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.agrona.concurrent.status.UnsafeBufferPosition;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static io.aeron.Aeron.IDLE_SLEEP_NS;
import static io.aeron.Aeron.sleep;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Client conductor receives responses and notifications from Media Driver and acts on them in addition to forwarding
 * commands from the Client API to the Media Driver conductor.
 */
class ClientConductor implements Agent, DriverEventsListener
{
    private static final long NO_CORRELATION_ID = Aeron.NULL_VALUE;
    private static final long RESOURCE_CHECK_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);
    private static final long RESOURCE_LINGER_NS = TimeUnit.SECONDS.toNanos(3);

    private final long keepAliveIntervalNs;
    private final long driverTimeoutMs;
    private final long driverTimeoutNs;
    private final long interServiceTimeoutNs;
    private long timeOfLastKeepAliveNs;
    private long timeOfLastResourcesCheckNs;
    private long timeOfLastServiceNs;
    private boolean isClosed;
    private boolean isInCallback;
    private String stashedChannel;
    private RegistrationException driverException;

    private final Aeron.Context ctx;
    private final Lock clientLock;
    private final EpochClock epochClock;
    private final NanoClock nanoClock;
    private final DriverEventsAdapter driverEventsAdapter;
    private final LogBuffersFactory logBuffersFactory;
    private final Long2ObjectHashMap<LogBuffers> logBuffersByIdMap = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<Object> resourceByRegIdMap = new Long2ObjectHashMap<>();
    private final ArrayList<ManagedResource> lingeringResources = new ArrayList<>();
    private final AvailableImageHandler defaultAvailableImageHandler;
    private final UnavailableImageHandler defaultUnavailableImageHandler;
    private final AvailableCounterHandler availableCounterHandler;
    private final UnavailableCounterHandler unavailableCounterHandler;
    private final DriverProxy driverProxy;
    private final AgentInvoker driverAgentInvoker;
    private final UnsafeBuffer counterValuesBuffer;
    private final CountersReader countersReader;

    ClientConductor(final Aeron.Context ctx)
    {
        this.ctx = ctx;

        clientLock = ctx.clientLock();
        epochClock = ctx.epochClock();
        nanoClock = ctx.nanoClock();
        driverProxy = ctx.driverProxy();
        logBuffersFactory = ctx.logBuffersFactory();
        keepAliveIntervalNs = ctx.keepAliveInterval();
        driverTimeoutMs = ctx.driverTimeoutMs();
        driverTimeoutNs = MILLISECONDS.toNanos(driverTimeoutMs);
        interServiceTimeoutNs = ctx.interServiceTimeout();
        defaultAvailableImageHandler = ctx.availableImageHandler();
        defaultUnavailableImageHandler = ctx.unavailableImageHandler();
        availableCounterHandler = ctx.availableCounterHandler();
        unavailableCounterHandler = ctx.unavailableCounterHandler();
        driverEventsAdapter = new DriverEventsAdapter(ctx.toClientBuffer(), this);
        driverAgentInvoker = ctx.driverAgentInvoker();
        counterValuesBuffer = ctx.countersValuesBuffer();
        countersReader = new CountersReader(ctx.countersMetaDataBuffer(), ctx.countersValuesBuffer(), US_ASCII);

        final long nowNs = nanoClock.nanoTime();
        timeOfLastKeepAliveNs = nowNs;
        timeOfLastResourcesCheckNs = nowNs;
        timeOfLastServiceNs = nowNs;
    }

    public void onClose()
    {
        clientLock.lock();
        try
        {
            if (!isClosed)
            {
                isClosed = true;

                final int lingeringResourcesSize = lingeringResources.size();
                forceCloseResources();
                driverProxy.clientClose();

                if (lingeringResources.size() > lingeringResourcesSize)
                {
                    sleep(16);
                }

                for (int i = 0, size = lingeringResources.size(); i < size; i++)
                {
                    lingeringResources.get(i).delete();
                }
            }
        }
        finally
        {
            clientLock.unlock();
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

                workCount = service(NO_CORRELATION_ID);
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

    public void onError(final long correlationId, final int codeValue, final ErrorCode errorCode, final String message)
    {
        driverException = new RegistrationException(codeValue, errorCode, message);
    }

    public void onChannelEndpointError(final int statusIndicatorId, final String message)
    {
        for (final Object resource : resourceByRegIdMap.values())
        {
            if (resource instanceof Subscription)
            {
                final Subscription subscription = (Subscription)resource;

                if (subscription.channelStatusId() == statusIndicatorId)
                {
                    handleError(new ChannelEndpointException(statusIndicatorId, message));
                }
            }
            else if (resource instanceof Publication)
            {
                final Publication publication = (Publication)resource;

                if (publication.channelStatusId() == statusIndicatorId)
                {
                    handleError(new ChannelEndpointException(statusIndicatorId, message));
                }
            }
        }
    }

    public void onNewPublication(
        final long correlationId,
        final long registrationId,
        final int streamId,
        final int sessionId,
        final int publicationLimitId,
        final int statusIndicatorId,
        final String logFileName)
    {
        final ConcurrentPublication publication = new ConcurrentPublication(
            this,
            stashedChannel,
            streamId,
            sessionId,
            new UnsafeBufferPosition(counterValuesBuffer, publicationLimitId),
            statusIndicatorId,
            logBuffers(registrationId, logFileName),
            registrationId,
            correlationId);

        resourceByRegIdMap.put(correlationId, publication);
    }

    public void onNewExclusivePublication(
        final long correlationId,
        final long registrationId,
        final int streamId,
        final int sessionId,
        final int publicationLimitId,
        final int statusIndicatorId,
        final String logFileName)
    {
        final ExclusivePublication publication = new ExclusivePublication(
            this,
            stashedChannel,
            streamId,
            sessionId,
            new UnsafeBufferPosition(counterValuesBuffer, publicationLimitId),
            statusIndicatorId,
            logBuffers(registrationId, logFileName),
            registrationId,
            correlationId);

        resourceByRegIdMap.put(correlationId, publication);
    }

    public void onNewSubscription(final long correlationId, final int statusIndicatorId)
    {
        final Subscription subscription = (Subscription)resourceByRegIdMap.get(correlationId);
        subscription.channelStatusId(statusIndicatorId);
    }

    public void onAvailableImage(
        final long correlationId,
        final int streamId,
        final int sessionId,
        final long subscriptionRegistrationId,
        final int subscriberPositionId,
        final String logFileName,
        final String sourceIdentity)
    {
        final Subscription subscription = (Subscription)resourceByRegIdMap.get(subscriptionRegistrationId);
        if (null != subscription && !subscription.containsImage(correlationId))
        {
            final Image image = new Image(
                subscription,
                sessionId,
                new UnsafeBufferPosition(counterValuesBuffer, subscriberPositionId),
                logBuffers(correlationId, logFileName),
                ctx.errorHandler(),
                sourceIdentity,
                correlationId);

            final AvailableImageHandler handler = subscription.availableImageHandler();
            if (null != handler)
            {
                isInCallback = true;
                try
                {
                    handler.onAvailableImage(image);
                }
                catch (final Throwable ex)
                {
                    handleError(ex);
                }
                finally
                {
                    isInCallback = false;
                }
            }

            subscription.addImage(image);
        }
    }

    public void onUnavailableImage(final long correlationId, final long subscriptionRegistrationId, final int streamId)
    {
        final Subscription subscription = (Subscription)resourceByRegIdMap.get(subscriptionRegistrationId);
        if (null != subscription)
        {
            final Image image = subscription.removeImage(correlationId);
            if (null != image)
            {
                final UnavailableImageHandler handler = subscription.unavailableImageHandler();
                if (null != handler)
                {
                    isInCallback = true;
                    try
                    {
                        handler.onUnavailableImage(image);
                    }
                    catch (final Throwable ex)
                    {
                        handleError(ex);
                    }
                    finally
                    {
                        isInCallback = false;
                    }
                }
            }
        }
    }

    public void onNewCounter(final long correlationId, final int counterId)
    {
        resourceByRegIdMap.put(correlationId, new Counter(correlationId, this, counterValuesBuffer, counterId));
        onAvailableCounter(correlationId, counterId);
    }

    public void onAvailableCounter(final long registrationId, final int counterId)
    {
        if (null != availableCounterHandler)
        {
            isInCallback = true;
            try
            {
                availableCounterHandler.onAvailableCounter(countersReader, registrationId, counterId);
            }
            catch (final Exception ex)
            {
                handleError(ex);
            }
            finally
            {
                isInCallback = false;
            }
        }
    }

    public void onUnavailableCounter(final long registrationId, final int counterId)
    {
        if (null != unavailableCounterHandler)
        {
            isInCallback = true;
            try
            {
                unavailableCounterHandler.onUnavailableCounter(countersReader, registrationId, counterId);
            }
            catch (final Exception ex)
            {
                handleError(ex);
            }
            finally
            {
                isInCallback = false;
            }
        }
    }

    CountersReader countersReader()
    {
        return countersReader;
    }

    void handleError(final Throwable ex)
    {
        ctx.errorHandler().onError(ex);
    }

    ConcurrentPublication addPublication(final String channel, final int streamId)
    {
        clientLock.lock();
        try
        {
            ensureOpen();
            ensureNotReentrant();

            stashedChannel = channel;
            final long registrationId = driverProxy.addPublication(channel, streamId);
            awaitResponse(registrationId);

            return (ConcurrentPublication)resourceByRegIdMap.get(registrationId);
        }
        finally
        {
            clientLock.unlock();
        }
    }

    ExclusivePublication addExclusivePublication(final String channel, final int streamId)
    {
        clientLock.lock();
        try
        {
            ensureOpen();
            ensureNotReentrant();

            stashedChannel = channel;
            final long registrationId = driverProxy.addExclusivePublication(channel, streamId);
            awaitResponse(registrationId);

            return (ExclusivePublication)resourceByRegIdMap.get(registrationId);
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void releasePublication(final Publication publication)
    {
        clientLock.lock();
        try
        {
            if (!publication.isClosed())
            {
                publication.internalClose();

                ensureOpen();
                ensureNotReentrant();

                if (publication == resourceByRegIdMap.remove(publication.registrationId()))
                {
                    releaseLogBuffers(publication.logBuffers(), publication.originalRegistrationId());
                    awaitResponse(driverProxy.removePublication(publication.registrationId()));
                }
            }
        }
        finally
        {
            clientLock.unlock();
        }
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
        clientLock.lock();
        try
        {
            ensureOpen();
            ensureNotReentrant();

            final long correlationId = driverProxy.addSubscription(channel, streamId);
            final Subscription subscription = new Subscription(
                this,
                channel,
                streamId,
                correlationId,
                availableImageHandler,
                unavailableImageHandler);

            resourceByRegIdMap.put(correlationId, subscription);

            awaitResponse(correlationId);

            return subscription;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void releaseSubscription(final Subscription subscription)
    {
        clientLock.lock();
        try
        {
            if (!subscription.isClosed())
            {
                subscription.internalClose();

                ensureOpen();
                ensureNotReentrant();

                final long registrationId = subscription.registrationId();
                awaitResponse(driverProxy.removeSubscription(registrationId));
                resourceByRegIdMap.remove(registrationId);
            }
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void addDestination(final long registrationId, final String endpointChannel)
    {
        clientLock.lock();
        try
        {
            ensureOpen();
            ensureNotReentrant();

            awaitResponse(driverProxy.addDestination(registrationId, endpointChannel));
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void removeDestination(final long registrationId, final String endpointChannel)
    {
        clientLock.lock();
        try
        {
            ensureOpen();
            ensureNotReentrant();

            awaitResponse(driverProxy.removeDestination(registrationId, endpointChannel));
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void addRcvDestination(final long registrationId, final String endpointChannel)
    {
        clientLock.lock();
        try
        {
            ensureOpen();
            ensureNotReentrant();

            awaitResponse(driverProxy.addRcvDestination(registrationId, endpointChannel));
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void removeRcvDestination(final long registrationId, final String endpointChannel)
    {
        clientLock.lock();
        try
        {
            ensureOpen();
            ensureNotReentrant();

            awaitResponse(driverProxy.removeRcvDestination(registrationId, endpointChannel));
        }
        finally
        {
            clientLock.unlock();
        }
    }

    Counter addCounter(
        final int typeId,
        final DirectBuffer keyBuffer,
        final int keyOffset,
        final int keyLength,
        final DirectBuffer labelBuffer,
        final int labelOffset,
        final int labelLength)
    {
        clientLock.lock();
        try
        {
            ensureOpen();
            ensureNotReentrant();

            if (keyLength < 0 || keyLength > CountersManager.MAX_KEY_LENGTH)
            {
                throw new IllegalArgumentException("key length out of bounds: " + keyLength);
            }

            if (labelLength < 0 || labelLength > CountersManager.MAX_LABEL_LENGTH)
            {
                throw new IllegalArgumentException("label length out of bounds: " + labelLength);
            }

            final long registrationId = driverProxy.addCounter(
                typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength);

            awaitResponse(registrationId);

            return (Counter)resourceByRegIdMap.get(registrationId);
        }
        finally
        {
            clientLock.unlock();
        }
    }

    Counter addCounter(final int typeId, final String label)
    {
        clientLock.lock();
        try
        {
            ensureOpen();
            ensureNotReentrant();

            if (label.length() > CountersManager.MAX_LABEL_LENGTH)
            {
                throw new IllegalArgumentException("label length exceeds MAX_LABEL_LENGTH: " + label.length());
            }

            final long registrationId = driverProxy.addCounter(typeId, label);

            awaitResponse(registrationId);

            return (Counter)resourceByRegIdMap.get(registrationId);
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void releaseCounter(final Counter counter)
    {
        clientLock.lock();
        try
        {
            if (!counter.isClosed())
            {
                counter.internalClose();

                ensureOpen();
                ensureNotReentrant();

                final long registrationId = counter.registrationId();
                awaitResponse(driverProxy.removeCounter(registrationId));
                resourceByRegIdMap.remove(registrationId);
            }
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void releaseImage(final Image image)
    {
        releaseLogBuffers(image.logBuffers(), image.correlationId());
    }

    void releaseLogBuffers(final LogBuffers logBuffers, final long registrationId)
    {
        if (logBuffers.decRef() == 0)
        {
            logBuffers.timeOfLastStateChange(nanoClock.nanoTime());
            logBuffersByIdMap.remove(registrationId);
            lingeringResources.add(logBuffers);
        }
    }

    DriverEventsAdapter driverListenerAdapter()
    {
        return driverEventsAdapter;
    }

    long channelStatus(final int channelStatusId)
    {
        switch (channelStatusId)
        {
            case 0:
                return ChannelEndpointStatus.INITIALIZING;

            case ChannelEndpointStatus.NO_ID_ALLOCATED:
                return ChannelEndpointStatus.ACTIVE;

            default:
                return countersReader.getCounterValue(channelStatusId);
        }
    }

    private void ensureOpen()
    {
        if (isClosed)
        {
            throw new AeronException("Aeron client conductor is closed");
        }
    }

    private void ensureNotReentrant()
    {
        if (isInCallback)
        {
            throw new AeronException("Reentrant calls not permitted during callbacks");
        }
    }

    private LogBuffers logBuffers(final long registrationId, final String logFileName)
    {
        LogBuffers logBuffers = logBuffersByIdMap.get(registrationId);
        if (null == logBuffers)
        {
            logBuffers = logBuffersFactory.map(logFileName);
            logBuffersByIdMap.put(registrationId, logBuffers);
        }

        logBuffers.incRef();

        return logBuffers;
    }

    private int service(final long correlationId)
    {
        int workCount = 0;

        try
        {
            workCount += onCheckTimeouts();
            workCount += driverEventsAdapter.receive(correlationId);
        }
        catch (final Throwable throwable)
        {
            handleError(throwable);

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

    private void awaitResponse(final long correlationId)
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

            service(correlationId);

            if (driverEventsAdapter.receivedCorrelationId() == correlationId)
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

            forceCloseResources();

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
        if (nowNs > (timeOfLastResourcesCheckNs + RESOURCE_CHECK_INTERVAL_NS))
        {
            final ArrayList<ManagedResource> lingeringResources = this.lingeringResources;
            for (int lastIndex = lingeringResources.size() - 1, i = lastIndex; i >= 0; i--)
            {
                final ManagedResource resource = lingeringResources.get(i);
                if (nowNs > (resource.timeOfLastStateChange() + RESOURCE_LINGER_NS))
                {
                    ArrayListUtil.fastUnorderedRemove(lingeringResources, i, lastIndex--);
                    resource.delete();
                }
            }

            timeOfLastResourcesCheckNs = nowNs;

            return 1;
        }

        return 0;
    }

    private void forceCloseResources()
    {
        for (final Object resource : resourceByRegIdMap.values())
        {
            if (resource instanceof Subscription)
            {
                final Subscription subscription = (Subscription)resource;
                subscription.internalClose();
            }
            else if (resource instanceof Publication)
            {
                final Publication publication = (Publication)resource;
                publication.internalClose();
                releaseLogBuffers(publication.logBuffers(), publication.originalRegistrationId());
            }
            else if (resource instanceof Counter)
            {
                final Counter counter = (Counter)resource;
                counter.internalClose();
            }
        }

        resourceByRegIdMap.clear();
    }
}
