/*
 * Copyright 2014-2019 Real Logic Ltd.
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
import org.agrona.LangUtil;
import org.agrona.ManagedResource;
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.agrona.concurrent.status.UnsafeBufferPosition;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;

import static io.aeron.Aeron.Configuration.IDLE_SLEEP_NS;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Client conductor receives responses and notifications from Media Driver and acts on them in addition to forwarding
 * commands from the Client API to the Media Driver conductor.
 */
class ClientConductor implements Agent, DriverEventsListener
{
    private static final long NO_CORRELATION_ID = Aeron.NULL_VALUE;

    private final long keepAliveIntervalNs;
    private final long driverTimeoutMs;
    private final long driverTimeoutNs;
    private final long interServiceTimeoutNs;
    private long timeOfLastKeepAliveNs;
    private long timeOfLastServiceNs;
    private boolean isClosed;
    private boolean isInCallback;
    private boolean isTerminating;
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
        driverEventsAdapter = new DriverEventsAdapter(ctx.toClientBuffer(), ctx.clientId(), this);
        driverAgentInvoker = ctx.driverAgentInvoker();
        counterValuesBuffer = ctx.countersValuesBuffer();
        countersReader = new CountersReader(ctx.countersMetaDataBuffer(), ctx.countersValuesBuffer(), US_ASCII);

        final long nowNs = nanoClock.nanoTime();
        timeOfLastKeepAliveNs = nowNs;
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
                forceCloseResources();
                Thread.yield();

                for (int i = 0, size = lingeringResources.size(); i < size; i++)
                {
                    lingeringResources.get(i).delete();
                }

                driverProxy.clientClose();
                ctx.close();
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
                if (isTerminating)
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

    boolean isTerminating()
    {
        return isTerminating;
    }

    public void onError(final long correlationId, final int codeValue, final ErrorCode errorCode, final String message)
    {
        driverException = new RegistrationException(codeValue, errorCode, message);
    }

    public void onChannelEndpointError(final int statusIndicatorId, final String message)
    {
        final Long2ObjectHashMap<Object>.ValueIterator iterator = resourceByRegIdMap.values().iterator();
        while (iterator.hasNext())
        {
            final Object resource = iterator.next();
            if (resource instanceof Subscription)
            {
                final Subscription subscription = (Subscription)resource;

                if (subscription.channelStatusId() == statusIndicatorId)
                {
                    handleError(new ChannelEndpointException(statusIndicatorId, message));
                    subscription.internalClose();
                    iterator.remove();
                }
            }
            else if (resource instanceof Publication)
            {
                final Publication publication = (Publication)resource;

                if (publication.channelStatusId() == statusIndicatorId)
                {
                    handleError(new ChannelEndpointException(statusIndicatorId, message));
                    publication.internalClose();
                    releaseLogBuffers(publication.logBuffers(), publication.originalRegistrationId());
                    iterator.remove();
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
        final int sessionId,
        final long subscriptionRegistrationId,
        final int subscriberPositionId,
        final String logFileName,
        final String sourceIdentity)
    {
        final Subscription subscription = (Subscription)resourceByRegIdMap.get(subscriptionRegistrationId);
        if (null != subscription)
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

    public void onUnavailableImage(final long correlationId, final long subscriptionRegistrationId)
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

    public void onClientTimeout()
    {
        if (!isClosed)
        {
            isTerminating = true;
            forceCloseResources();
            handleError(new ClientTimeoutException("client timeout from driver"));
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
            ensureActive();
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
            ensureActive();
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
                ensureActive();
                ensureNotReentrant();

                publication.internalClose();
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
            ensureActive();
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
                ensureActive();
                ensureNotReentrant();

                subscription.internalClose();
                final long registrationId = subscription.registrationId();
                resourceByRegIdMap.remove(registrationId);
                awaitResponse(driverProxy.removeSubscription(registrationId));
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
            ensureActive();
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
            ensureActive();
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
            ensureActive();
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
            ensureActive();
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
            ensureActive();
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
            ensureActive();
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
                ensureActive();
                ensureNotReentrant();

                counter.internalClose();
                final long registrationId = counter.registrationId();
                if (null != resourceByRegIdMap.remove(registrationId))
                {
                    awaitResponse(driverProxy.removeCounter(registrationId));
                }
            }
        }
        finally
        {
            clientLock.unlock();
        }
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

    private void ensureActive()
    {
        if (isClosed || isTerminating)
        {
            throw new AeronException("Aeron client is closed or terminating");
        }
    }

    private void ensureNotReentrant()
    {
        if (isInCallback)
        {
            throw new AeronException("reentrant calls not permitted during callbacks");
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

            if (driverEventsAdapter.isInvalid())
            {
                onClose();
            }

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
                try
                {
                    Thread.sleep(1);
                }
                catch (final InterruptedException ex)
                {
                    isTerminating = true;
                    LangUtil.rethrowUnchecked(ex);
                }
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

            if (Thread.interrupted())
            {
                isTerminating = true;
                LangUtil.rethrowUnchecked(new InterruptedException());
            }
        }
        while (deadlineNs - nanoClock.nanoTime() > 0);

        throw new DriverTimeoutException("no response from MediaDriver within (ns): " + driverTimeoutNs);
    }

    private int onCheckTimeouts()
    {
        int workCount = 0;
        final long nowNs = nanoClock.nanoTime();

        if ((timeOfLastServiceNs + IDLE_SLEEP_NS) - nowNs < 0)
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
        if ((timeOfLastServiceNs + interServiceTimeoutNs) - nowNs < 0)
        {
            isTerminating = true;

            forceCloseResources();
            Thread.yield();

            throw new ConductorServiceTimeoutException("service interval exceeded (ns): " + interServiceTimeoutNs);
        }
    }

    private int checkLiveness(final long nowNs)
    {
        if ((timeOfLastKeepAliveNs + keepAliveIntervalNs) - nowNs < 0)
        {
            if (epochClock.time() > (driverProxy.timeOfLastDriverKeepaliveMs() + driverTimeoutMs))
            {
                isTerminating = true;

                forceCloseResources();
                Thread.yield();

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
        int workCount = 0;

        final ArrayList<ManagedResource> lingeringResources = this.lingeringResources;
        for (int lastIndex = lingeringResources.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ManagedResource resource = lingeringResources.get(i);
            if ((resource.timeOfLastStateChange() + ctx.resourceLingerDurationNs()) - nowNs < 0)
            {
                ArrayListUtil.fastUnorderedRemove(lingeringResources, i, lastIndex--);
                resource.delete();
                workCount += 1;
            }
        }

        return workCount;
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
