/*
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
import io.aeron.status.HeartbeatTimestamp;
import org.agrona.*;
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.agrona.concurrent.status.UnsafeBufferPosition;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;

import static io.aeron.Aeron.Configuration.IDLE_SLEEP_MS;
import static io.aeron.Aeron.Configuration.IDLE_SLEEP_NS;
import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.status.HeartbeatTimestamp.HEARTBEAT_TYPE_ID;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Client conductor receives responses and notifications from Media Driver and acts on them in addition to forwarding
 * commands from the Client API to the Media Driver conductor.
 */
final class ClientConductor implements Agent
{
    private static final long NO_CORRELATION_ID = NULL_VALUE;

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
    private final Aeron aeron;
    private final Lock clientLock;
    private final EpochClock epochClock;
    private final NanoClock nanoClock;
    private final IdleStrategy awaitingIdleStrategy;
    private final DriverEventsAdapter driverEventsAdapter;
    private final LogBuffersFactory logBuffersFactory;
    private final Long2ObjectHashMap<LogBuffers> logBuffersByIdMap = new Long2ObjectHashMap<>();
    private final ArrayList<LogBuffers> lingeringLogBuffers = new ArrayList<>();
    private final Long2ObjectHashMap<Object> resourceByRegIdMap = new Long2ObjectHashMap<>();
    private final LongHashSet asyncCommandIdSet = new LongHashSet();
    private final AvailableImageHandler defaultAvailableImageHandler;
    private final UnavailableImageHandler defaultUnavailableImageHandler;
    private final Long2ObjectHashMap<AvailableCounterHandler> availableCounterHandlerById = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<UnavailableCounterHandler> unavailableCounterHandlerById =
        new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<Runnable> closeHandlerByIdMap = new Long2ObjectHashMap<>();
    private final DriverProxy driverProxy;
    private final AgentInvoker driverAgentInvoker;
    private final UnsafeBuffer counterValuesBuffer;
    private final CountersReader countersReader;
    private AtomicCounter heartbeatTimestamp;

    ClientConductor(final Aeron.Context ctx, final Aeron aeron)
    {
        this.ctx = ctx;
        this.aeron = aeron;

        clientLock = ctx.clientLock();
        epochClock = ctx.epochClock();
        nanoClock = ctx.nanoClock();
        awaitingIdleStrategy = ctx.awaitingIdleStrategy();
        driverProxy = ctx.driverProxy();
        logBuffersFactory = ctx.logBuffersFactory();
        keepAliveIntervalNs = ctx.keepAliveIntervalNs();
        driverTimeoutMs = ctx.driverTimeoutMs();
        driverTimeoutNs = MILLISECONDS.toNanos(driverTimeoutMs);
        interServiceTimeoutNs = ctx.interServiceTimeoutNs();
        defaultAvailableImageHandler = ctx.availableImageHandler();
        defaultUnavailableImageHandler = ctx.unavailableImageHandler();
        driverEventsAdapter = new DriverEventsAdapter(ctx.clientId(), ctx.toClientBuffer(), this, asyncCommandIdSet);
        driverAgentInvoker = ctx.driverAgentInvoker();
        counterValuesBuffer = ctx.countersValuesBuffer();
        countersReader = new CountersReader(ctx.countersMetaDataBuffer(), ctx.countersValuesBuffer(), US_ASCII);

        if (null != ctx.availableCounterHandler())
        {
            availableCounterHandlerById.put(aeron.nextCorrelationId(), ctx.availableCounterHandler());
        }

        if (null != ctx.unavailableCounterHandler())
        {
            unavailableCounterHandlerById.put(aeron.nextCorrelationId(), ctx.unavailableCounterHandler());
        }

        if (null != ctx.closeHandler())
        {
            closeHandlerByIdMap.put(aeron.nextCorrelationId(), ctx.closeHandler());
        }

        final long nowNs = nanoClock.nanoTime();
        timeOfLastKeepAliveNs = nowNs;
        timeOfLastServiceNs = nowNs;
    }

    public void onClose()
    {
        boolean isInterrupted = false;

        clientLock.lock();
        try
        {
            if (!isClosed)
            {
                if (!aeron.isClosed())
                {
                    aeron.internalClose();
                }

                final boolean isTerminating = this.isTerminating;
                this.isTerminating = true;
                forceCloseResources();
                notifyCloseHandlers();

                try
                {
                    if (isTerminating)
                    {
                        Thread.sleep(IDLE_SLEEP_MS);
                    }

                    Thread.sleep(NANOSECONDS.toMillis(ctx.closeLingerDurationNs()));
                }
                catch (final InterruptedException ignore)
                {
                    isInterrupted = true;
                }

                for (int i = 0, size = lingeringLogBuffers.size(); i < size; i++)
                {
                    CloseHelper.close(ctx.errorHandler(), lingeringLogBuffers.get(i));
                }

                driverProxy.clientClose();
                ctx.close();
            }
        }
        finally
        {
            isClosed = true;
            if (isInterrupted)
            {
                Thread.currentThread().interrupt();
            }

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

    void onError(final long correlationId, final int codeValue, final ErrorCode errorCode, final String message)
    {
        driverException = new RegistrationException(correlationId, codeValue, errorCode, message);

        final Object resource = resourceByRegIdMap.get(correlationId);
        if (resource instanceof Subscription)
        {
            final Subscription subscription = (Subscription)resource;
            subscription.internalClose(NULL_VALUE);
            resourceByRegIdMap.remove(correlationId);
        }
    }

    void onAsyncError(final long correlationId, final int codeValue, final ErrorCode errorCode, final String message)
    {
        handleError(new RegistrationException(correlationId, codeValue, errorCode, message));
    }

    void onChannelEndpointError(final int statusIndicatorId, final String message)
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

    void onNewPublication(
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
            logBuffers(registrationId, logFileName, stashedChannel),
            registrationId,
            correlationId);

        resourceByRegIdMap.put(correlationId, publication);
    }

    void onNewExclusivePublication(
        final long correlationId,
        final long registrationId,
        final int streamId,
        final int sessionId,
        final int publicationLimitId,
        final int statusIndicatorId,
        final String logFileName)
    {
        if (correlationId != registrationId)
        {
            handleError(new IllegalStateException(
                "correlationId=" + correlationId + " registrationId=" + registrationId));
        }

        final ExclusivePublication publication = new ExclusivePublication(
            this,
            stashedChannel,
            streamId,
            sessionId,
            new UnsafeBufferPosition(counterValuesBuffer, publicationLimitId),
            statusIndicatorId,
            logBuffers(registrationId, logFileName, stashedChannel),
            registrationId,
            correlationId);

        resourceByRegIdMap.put(correlationId, publication);
    }

    void onNewSubscription(final long correlationId, final int statusIndicatorId)
    {
        final Subscription subscription = (Subscription)resourceByRegIdMap.get(correlationId);
        subscription.channelStatusId(statusIndicatorId);
    }

    void onAvailableImage(
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
                logBuffers(correlationId, logFileName, subscription.channel()),
                ctx.subscriberErrorHandler(),
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

    void onUnavailableImage(final long correlationId, final long subscriptionRegistrationId)
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
                    notifyImageUnavailable(handler, image);
                }
            }
        }
    }

    void onNewCounter(final long correlationId, final int counterId)
    {
        resourceByRegIdMap.put(correlationId, new Counter(correlationId, this, counterValuesBuffer, counterId));
        onAvailableCounter(correlationId, counterId);
    }

    void onAvailableCounter(final long registrationId, final int counterId)
    {
        for (final AvailableCounterHandler handler : availableCounterHandlerById.values())
        {
            notifyCounterAvailable(registrationId, counterId, handler);
        }
    }

    void onUnavailableCounter(final long registrationId, final int counterId)
    {
        notifyUnavailableCounterHandlers(registrationId, counterId);
    }

    void onClientTimeout()
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
        if (!isClosed)
        {
            ctx.errorHandler().onError(ex);
        }
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
            if (isTerminating || isClosed)
            {
                return;
            }

            if (!publication.isClosed())
            {
                ensureNotReentrant();

                publication.internalClose();
                if (publication == resourceByRegIdMap.remove(publication.registrationId()))
                {
                    releaseLogBuffers(publication.logBuffers(), publication.originalRegistrationId(), 0);
                    asyncCommandIdSet.add(driverProxy.removePublication(publication.registrationId()));
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
            if (isTerminating || isClosed)
            {
                return;
            }

            if (!subscription.isClosed())
            {
                ensureNotReentrant();

                subscription.internalClose(0);
                final long registrationId = subscription.registrationId();
                if (subscription == resourceByRegIdMap.remove(registrationId))
                {
                    asyncCommandIdSet.add(driverProxy.removeSubscription(registrationId));
                }
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

    long asyncAddDestination(final long registrationId, final String endpointChannel)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long correlationId = driverProxy.addDestination(registrationId, endpointChannel);
            asyncCommandIdSet.add(correlationId);
            return correlationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long asyncRemoveDestination(final long registrationId, final String endpointChannel)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long correlationId = driverProxy.removeDestination(registrationId, endpointChannel);
            asyncCommandIdSet.add(correlationId);
            return correlationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long asyncAddRcvDestination(final long registrationId, final String endpointChannel)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long correlationId = driverProxy.addRcvDestination(registrationId, endpointChannel);
            asyncCommandIdSet.add(correlationId);
            return correlationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long asyncRemoveRcvDestination(final long registrationId, final String endpointChannel)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long correlationId = driverProxy.removeRcvDestination(registrationId, endpointChannel);
            asyncCommandIdSet.add(correlationId);
            return correlationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    boolean isCommandActive(final long correlationId)
    {
        clientLock.lock();
        try
        {
            if (isClosed)
            {
                return false;
            }

            ensureActive();

            return asyncCommandIdSet.contains(correlationId);
        }
        finally
        {
            clientLock.unlock();
        }
    }

    boolean hasActiveCommands()
    {
        clientLock.lock();
        try
        {
            if (isClosed)
            {
                return false;
            }

            ensureActive();

            return !asyncCommandIdSet.isEmpty();
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

    long addAvailableCounterHandler(final AvailableCounterHandler handler)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long registrationId = aeron.nextCorrelationId();
            availableCounterHandlerById.put(registrationId, handler);
            return registrationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    boolean removeAvailableCounterHandler(final long registrationId)
    {
        clientLock.lock();
        try
        {
            return availableCounterHandlerById.remove(registrationId) != null;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    boolean removeAvailableCounterHandler(final AvailableCounterHandler handler)
    {
        clientLock.lock();
        try
        {
            if (isTerminating || isClosed)
            {
                return false;
            }

            ensureNotReentrant();

            final Long2ObjectHashMap<AvailableCounterHandler>.ValueIterator iterator =
                availableCounterHandlerById.values().iterator();
            while (iterator.hasNext())
            {
                if (handler == iterator.next())
                {
                    iterator.remove();
                    return true;
                }
            }

            return false;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long addUnavailableCounterHandler(final UnavailableCounterHandler handler)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long registrationId = aeron.nextCorrelationId();
            unavailableCounterHandlerById.put(registrationId, handler);
            return registrationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    boolean removeUnavailableCounterHandler(final long registrationId)
    {
        clientLock.lock();
        try
        {
            return unavailableCounterHandlerById.remove(registrationId) != null;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    boolean removeUnavailableCounterHandler(final UnavailableCounterHandler handler)
    {
        clientLock.lock();
        try
        {
            if (isTerminating || isClosed)
            {
                return false;
            }

            ensureNotReentrant();

            final Long2ObjectHashMap<UnavailableCounterHandler>.ValueIterator iterator =
                unavailableCounterHandlerById.values().iterator();
            while (iterator.hasNext())
            {
                if (handler == iterator.next())
                {
                    iterator.remove();
                    return true;
                }
            }

            return false;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    long addCloseHandler(final Runnable handler)
    {
        clientLock.lock();
        try
        {
            ensureActive();
            ensureNotReentrant();

            final long registrationId = aeron.nextCorrelationId();
            closeHandlerByIdMap.put(registrationId, handler);
            return registrationId;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    boolean removeCloseHandler(final long registrationId)
    {
        clientLock.lock();
        try
        {
            return closeHandlerByIdMap.remove(registrationId) != null;
        }
        finally
        {
            clientLock.unlock();
        }
    }

    boolean removeCloseHandler(final Runnable handler)
    {
        clientLock.lock();
        try
        {
            if (isTerminating || isClosed)
            {
                return false;
            }

            ensureNotReentrant();

            final Long2ObjectHashMap<Runnable>.ValueIterator iterator = closeHandlerByIdMap.values().iterator();
            while (iterator.hasNext())
            {
                if (handler == iterator.next())
                {
                    iterator.remove();
                    return true;
                }
            }

            return false;
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
            if (isTerminating || isClosed)
            {
                return;
            }

            ensureNotReentrant();

            final long registrationId = counter.registrationId();
            if (counter == resourceByRegIdMap.remove(registrationId))
            {
                asyncCommandIdSet.add(driverProxy.removeCounter(registrationId));
            }
        }
        finally
        {
            clientLock.unlock();
        }
    }

    void releaseLogBuffers(
        final LogBuffers logBuffers, final long registrationId, final long lingerDurationNs)
    {
        if (logBuffers.decRef() == 0)
        {
            lingeringLogBuffers.add(logBuffers);
            logBuffersByIdMap.remove(registrationId);

            final long lingerNs = NULL_VALUE == lingerDurationNs ? ctx.resourceLingerDurationNs() : lingerDurationNs;
            logBuffers.lingerDeadlineNs(nanoClock.nanoTime() + lingerNs);
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

    void closeImages(
        final Image[] images, final UnavailableImageHandler unavailableImageHandler, final long lingerNs)
    {
        for (final Image image : images)
        {
            image.close();
            releaseLogBuffers(image.logBuffers(), image.correlationId(), lingerNs);
        }

        if (null != unavailableImageHandler)
        {
            for (final Image image : images)
            {
                notifyImageUnavailable(unavailableImageHandler, image);
            }
        }
    }

    private void ensureActive()
    {
        if (isClosed)
        {
            throw new AeronException("Aeron client is closed");
        }

        if (isTerminating)
        {
            throw new AeronException("Aeron client is terminating");
        }
    }

    private void ensureNotReentrant()
    {
        if (isInCallback)
        {
            throw new AeronException("reentrant calls not permitted during callbacks");
        }
    }

    private LogBuffers logBuffers(final long registrationId, final String logFileName, final String channel)
    {
        LogBuffers logBuffers = logBuffersByIdMap.get(registrationId);
        if (null == logBuffers)
        {
            logBuffers = logBuffersFactory.map(logFileName);

            if (ctx.preTouchMappedMemory() && !channel.contains("sparse=true"))
            {
                logBuffers.preTouch();
            }

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
            workCount += checkTimeouts(nanoClock.nanoTime());
            workCount += driverEventsAdapter.receive(correlationId);
        }
        catch (final AgentTerminationException ex)
        {
            if (isClientApiCall(correlationId))
            {
                isTerminating = true;
                forceCloseResources();
            }
            throw ex;
        }
        catch (final Throwable ex)
        {
            if (driverEventsAdapter.isInvalid())
            {
                isTerminating = true;
                forceCloseResources();

                if (!isClientApiCall(correlationId))
                {
                    throw new AeronException("Driver events adapter is invalid", ex);
                }
            }

            if (isClientApiCall(correlationId))
            {
                throw ex;
            }

            handleError(ex);
        }

        return workCount;
    }

    private static boolean isClientApiCall(final long correlationId)
    {
        return correlationId != NO_CORRELATION_ID;
    }

    private void awaitResponse(final long correlationId)
    {
        final long nowNs = nanoClock.nanoTime();
        final long deadlineNs = nowNs + driverTimeoutNs;
        checkTimeouts(nowNs);

        awaitingIdleStrategy.reset();
        do
        {
            if (null == driverAgentInvoker)
            {
                awaitingIdleStrategy.idle();
            }
            else
            {
                driverAgentInvoker.invoke();
            }

            service(correlationId);

            if (driverEventsAdapter.receivedCorrelationId() == correlationId)
            {
                stashedChannel = null;
                final RegistrationException ex = driverException;
                if (null != ex)
                {
                    driverException = null;
                    throw ex;
                }

                return;
            }

            if (Thread.currentThread().isInterrupted())
            {
                isTerminating = true;
                throw new AeronException("unexpected interrupt");
            }
        }
        while (deadlineNs - nanoClock.nanoTime() > 0);

        throw new DriverTimeoutException("no response from MediaDriver within (ns): " + driverTimeoutNs);
    }

    private int checkTimeouts(final long nowNs)
    {
        int workCount = 0;

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

            throw new ConductorServiceTimeoutException(
                "service interval exceeded (ns): timeout=" + interServiceTimeoutNs +
                ", interval=" + (nowNs - timeOfLastServiceNs));
        }
    }

    private int checkLiveness(final long nowNs)
    {
        if ((timeOfLastKeepAliveNs + keepAliveIntervalNs) - nowNs < 0)
        {
            final long nowMs = epochClock.time();
            final long lastKeepAliveMs = driverProxy.timeOfLastDriverKeepaliveMs();

            if (nowMs > (lastKeepAliveMs + driverTimeoutMs))
            {
                isTerminating = true;
                forceCloseResources();

                throw new DriverTimeoutException(
                    "MediaDriver keepalive age exceeded (ms): timeout= " +
                     driverTimeoutMs + ", age=" + (nowMs - lastKeepAliveMs));
            }

            if (null == heartbeatTimestamp)
            {
                final int counterId = HeartbeatTimestamp.findCounterIdByRegistrationId(
                    countersReader, HEARTBEAT_TYPE_ID, ctx.clientId());

                if (counterId != CountersReader.NULL_COUNTER_ID)
                {
                    heartbeatTimestamp = new AtomicCounter(counterValuesBuffer, counterId);
                    heartbeatTimestamp.setOrdered(nowMs);
                    timeOfLastKeepAliveNs = nowNs;
                }
            }
            else
            {
                final int counterId = heartbeatTimestamp.id();
                if (!HeartbeatTimestamp.isActive(countersReader, counterId, HEARTBEAT_TYPE_ID, ctx.clientId()))
                {
                    isTerminating = true;
                    forceCloseResources();

                    throw new AeronException("unexpected close of heartbeat timestamp counter: " + counterId);
                }

                heartbeatTimestamp.setOrdered(nowMs);
                timeOfLastKeepAliveNs = nowNs;
            }

            return 1;
        }

        return 0;
    }

    private int checkLingeringResources(final long nowNs)
    {
        int workCount = 0;

        for (int lastIndex = lingeringLogBuffers.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final LogBuffers logBuffers = lingeringLogBuffers.get(i);
            if (logBuffers.lingerDeadlineNs() - nowNs < 0)
            {
                ArrayListUtil.fastUnorderedRemove(lingeringLogBuffers, i, lastIndex--);
                CloseHelper.close(ctx.errorHandler(), logBuffers);

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
                subscription.internalClose(NULL_VALUE);
            }
            else if (resource instanceof Publication)
            {
                final Publication publication = (Publication)resource;
                publication.internalClose();
                releaseLogBuffers(publication.logBuffers(), publication.originalRegistrationId(), NULL_VALUE);
            }
            else if (resource instanceof Counter)
            {
                final Counter counter = (Counter)resource;
                counter.internalClose();
                notifyUnavailableCounterHandlers(counter.registrationId(), counter.id());
            }
        }

        resourceByRegIdMap.clear();
    }

    private void notifyUnavailableCounterHandlers(final long registrationId, final int counterId)
    {
        for (final UnavailableCounterHandler handler : unavailableCounterHandlerById.values())
        {
            isInCallback = true;
            try
            {
                handler.onUnavailableCounter(countersReader, registrationId, counterId);
            }
            catch (final AgentTerminationException ex)
            {
                if (!isTerminating)
                {
                    throw ex;
                }
                handleError(ex);
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

    private void notifyImageUnavailable(final UnavailableImageHandler handler, final Image image)
    {
        isInCallback = true;
        try
        {
            handler.onUnavailableImage(image);
        }
        catch (final AgentTerminationException ex)
        {
            if (!isTerminating)
            {
                throw ex;
            }
            handleError(ex);
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

    private void notifyCounterAvailable(
        final long registrationId, final int counterId, final AvailableCounterHandler handler)
    {
        isInCallback = true;
        try
        {
            handler.onAvailableCounter(countersReader, registrationId, counterId);
        }
        catch (final AgentTerminationException ex)
        {
            throw ex;
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

    private void notifyCloseHandlers()
    {
        for (final Runnable closeHandler : closeHandlerByIdMap.values())
        {
            isInCallback = true;
            try
            {
                closeHandler.run();
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
