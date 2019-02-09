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

#ifndef AERON_CLIENT_CONDUCTOR_H
#define AERON_CLIENT_CONDUCTOR_H

#include <vector>
#include <mutex>
#include <concurrent/logbuffer/TermReader.h>
#include <concurrent/status/UnsafeBufferPosition.h>
#include <util/LangUtil.h>
#include "Publication.h"
#include "ExclusivePublication.h"
#include "Subscription.h"
#include "Counter.h"
#include "DriverProxy.h"
#include "Context.h"
#include "DriverListenerAdapter.h"
#include "LogBuffers.h"

namespace aeron {

using namespace aeron::concurrent::logbuffer;
using namespace aeron::concurrent::status;
using namespace aeron::concurrent;

typedef std::function<long long()> epoch_clock_t;
typedef std::function<long long()> nano_clock_t;

static const long KEEPALIVE_TIMEOUT_MS = 500;
static const long RESOURCE_TIMEOUT_MS = 1000;

class ClientConductor
{
public:

    ClientConductor(
        epoch_clock_t epochClock,
        DriverProxy& driverProxy,
        CopyBroadcastReceiver& broadcastReceiver,
        AtomicBuffer& counterMetadataBuffer,
        AtomicBuffer& counterValuesBuffer,
        const on_new_publication_t& newPublicationHandler,
        const on_new_publication_t& newExclusivePublicationHandler,
        const on_new_subscription_t& newSubscriptionHandler,
        const exception_handler_t& errorHandler,
        const on_available_counter_t& availableCounterHandler,
        const on_unavailable_counter_t& unavailableCounterHandler,
        long driverTimeoutMs,
        long resourceLingerTimeoutMs,
        long long interServiceTimeoutNs) :
        m_driverProxy(driverProxy),
        m_driverListenerAdapter(broadcastReceiver, *this),
        m_countersReader(counterMetadataBuffer, counterValuesBuffer),
        m_counterValuesBuffer(counterValuesBuffer),
        m_onNewPublicationHandler(newPublicationHandler),
        m_onNewExclusivePublicationHandler(newExclusivePublicationHandler),
        m_onNewSubscriptionHandler(newSubscriptionHandler),
        m_errorHandler(errorHandler),
        m_onAvailableCounterHandler(availableCounterHandler),
        m_onUnavailableCounterHandler(unavailableCounterHandler),
        m_epochClock(std::move(epochClock)),
        m_timeOfLastKeepalive(m_epochClock()),
        m_timeOfLastCheckManagedResources(m_epochClock()),
        m_timeOfLastDoWork(m_epochClock()),
        m_driverTimeoutMs(driverTimeoutMs),
        m_resourceLingerTimeoutMs(resourceLingerTimeoutMs),
        m_interServiceTimeoutMs(static_cast<long>(interServiceTimeoutNs / 1000000)),
        m_driverActive(true),
        m_isClosed(false)
    {
    }

    virtual ~ClientConductor();

    void onStart()
    {
    }

    int doWork()
    {
        int workCount = 0;

        workCount += m_driverListenerAdapter.receiveMessages();
        workCount += onHeartbeatCheckTimeouts();

        return workCount;
    }

    void onClose()
    {
    }

    std::int64_t addPublication(const std::string& channel, std::int32_t streamId);
    std::shared_ptr<Publication> findPublication(std::int64_t registrationId);
    void releasePublication(std::int64_t registrationId);

    std::int64_t addExclusivePublication(const std::string& channel, std::int32_t streamId);
    std::shared_ptr<ExclusivePublication> findExclusivePublication(std::int64_t registrationId);
    void releaseExclusivePublication(std::int64_t registrationId);

    std::int64_t addSubscription(
        const std::string& channel,
        std::int32_t streamId,
        const on_available_image_t &onAvailableImageHandler,
        const on_unavailable_image_t &onUnavailableImageHandler);
    std::shared_ptr<Subscription> findSubscription(std::int64_t registrationId);
    void releaseSubscription(std::int64_t registrationId, struct ImageList *imageList);

    std::int64_t addCounter(
        std::int32_t typeId,
        const std::uint8_t *keyBuffer,
        std::size_t keyLength,
        const std::string& label);
    std::shared_ptr<Counter> findCounter(std::int64_t registrationId);
    void releaseCounter(std::int64_t registrationId);

    void onNewPublication(std::int64_t registrationId, std::int64_t originalRegistrationId, std::int32_t streamId, std::int32_t sessionId,
        std::int32_t publicationLimitCounterId, std::int32_t channelStatusIndicatorId, const std::string &logFileName);

    void onNewExclusivePublication(std::int64_t registrationId, std::int64_t originalRegistrationId, std::int32_t streamId,
        std::int32_t sessionId, std::int32_t publicationLimitCounterId, std::int32_t channelStatusIndicatorId, const std::string &logFileName);

    void onSubscriptionReady(
        std::int64_t registrationId,
        std::int32_t channelStatusId);

    void onOperationSuccess(std::int64_t correlationId);

    void onErrorResponse(
        std::int64_t offendingCommandCorrelationId,
        std::int32_t errorCode,
        const std::string& errorMessage);

    void onAvailableImage(std::int64_t correlationId, std::int32_t sessionId, std::int32_t subscriberPositionId,
        std::int64_t subscriptionRegistrationId, const std::string &logFilename, const std::string &sourceIdentity);

    void onUnavailableImage(
        std::int64_t correlationId,
        std::int64_t subscriptionRegistrationId);

    void onAvailableCounter(
        std::int64_t registrationId,
        std::int32_t counterId);

    void onUnavailableCounter(
        std::int64_t registrationId,
        std::int32_t counterId);

    void onClientTimeout(std::int64_t clientId);

    void closeAllResources(long long now);

    void addDestination(std::int64_t publicationRegistrationId, const std::string& endpointChannel);
    void removeDestination(std::int64_t publicationRegistrationId, const std::string& endpointChannel);

    void addRcvDestination(std::int64_t subscriptionRegistrationId, const std::string& endpointChannel);
    void removeRcvDestination(std::int64_t subscriptionRegistrationId, const std::string& endpointChannel);

    inline CountersReader& countersReader()
    {
        return m_countersReader;
    }

    inline std::int64_t channelStatus(std::int32_t counterId) const
    {
        switch (counterId)
        {
            case 0:
                return ChannelEndpointStatus::CHANNEL_ENDPOINT_INITIALIZING;

            case ChannelEndpointStatus::NO_ID_ALLOCATED:
                return ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE;

            default:
                return m_countersReader.getCounterValue(counterId);
        }
    }

    inline bool isClosed() const
    {
        return std::atomic_load_explicit(&m_isClosed, std::memory_order_acquire);
    }

    inline void forceClose()
    {
        std::atomic_store_explicit(&m_isClosed, true, std::memory_order_release);
    }

protected:
    void onCheckManagedResources(long long now);

    void lingerResource(long long now, struct ImageList *imageList);
    void lingerResource(long long now, std::shared_ptr<LogBuffers> logBuffers);
    void lingerAllResources(long long now, struct ImageList *imageList);

private:
    enum class RegistrationStatus
    {
        AWAITING_MEDIA_DRIVER, REGISTERED_MEDIA_DRIVER, ERRORED_MEDIA_DRIVER
    };

    struct PublicationStateDefn
    {
        std::string m_channel;
        std::int64_t m_registrationId;
        std::int64_t m_originalRegistrationId;
        std::int32_t m_streamId;
        std::int32_t m_sessionId = -1;
        std::int32_t m_publicationLimitCounterId = -1;
        std::int32_t m_channelStatusId = -1;
        long long m_timeOfRegistration;
        RegistrationStatus m_status = RegistrationStatus::AWAITING_MEDIA_DRIVER;
        std::int32_t m_errorCode;
        std::string m_errorMessage;
        std::shared_ptr<LogBuffers> m_buffers;
        std::weak_ptr<Publication> m_publication;

        PublicationStateDefn(
            const std::string& channel, std::int64_t registrationId, std::int32_t streamId, long long now) :
            m_channel(channel),
            m_registrationId(registrationId),
            m_streamId(streamId),
            m_timeOfRegistration(now)
        {
        }
    };

    struct ExclusivePublicationStateDefn
    {
        std::string m_channel;
        std::int64_t m_registrationId;
        std::int64_t m_originalRegistrationId;
        std::int32_t m_streamId;
        std::int32_t m_sessionId = -1;
        std::int32_t m_publicationLimitCounterId = -1;
        std::int32_t m_channelStatusId = -1;
        long long m_timeOfRegistration;
        RegistrationStatus m_status = RegistrationStatus::AWAITING_MEDIA_DRIVER;
        std::int32_t m_errorCode;
        std::string m_errorMessage;
        std::shared_ptr<LogBuffers> m_buffers;
        std::weak_ptr<ExclusivePublication> m_publication;

        ExclusivePublicationStateDefn(
            const std::string& channel, std::int64_t registrationId, std::int32_t streamId, long long now) :
            m_channel(channel),
            m_registrationId(registrationId),
            m_streamId(streamId),
            m_timeOfRegistration(now)
        {
        }
    };

    struct SubscriptionStateDefn
    {
        std::string m_channel;
        std::int64_t m_registrationId;
        std::int32_t m_streamId;
        long long m_timeOfRegistration;
        RegistrationStatus m_status = RegistrationStatus::AWAITING_MEDIA_DRIVER;
        std::int32_t m_errorCode;
        std::string m_errorMessage;
        std::shared_ptr<Subscription> m_subscriptionCache;
        std::weak_ptr<Subscription> m_subscription;
        on_available_image_t m_onAvailableImageHandler;
        on_unavailable_image_t m_onUnavailableImageHandler;

        SubscriptionStateDefn(
            const std::string& channel,
            std::int64_t registrationId,
            std::int32_t streamId,
            long long now,
            const on_available_image_t &onAvailableImageHandler,
            const on_unavailable_image_t &onUnavailableImageHandler) :
            m_channel(channel),
            m_registrationId(registrationId),
            m_streamId(streamId),
            m_timeOfRegistration(now),
            m_onAvailableImageHandler(onAvailableImageHandler),
            m_onUnavailableImageHandler(onUnavailableImageHandler)
        {
        }
    };

    struct CounterStateDefn
    {
        std::int64_t m_registrationId;
        std::int32_t m_counterId = -1;
        long long m_timeOfRegistration;
        RegistrationStatus m_status = RegistrationStatus::AWAITING_MEDIA_DRIVER;
        std::int32_t m_errorCode;
        std::string m_errorMessage;
        std::shared_ptr<Counter> m_counterCache;
        std::weak_ptr<Counter> m_counter;

        CounterStateDefn(std::int64_t registrationId, long long now) :
            m_registrationId(registrationId),
            m_timeOfRegistration(now)
        {
        }
    };

    struct ImageListLingerDefn
    {
        long long m_timeOfLastStatusChange;
        struct ImageList *m_imageList;

        ImageListLingerDefn(long long now, struct ImageList *imageList) :
            m_timeOfLastStatusChange(now), m_imageList(imageList)
        {
        }
    };

    struct LogBuffersLingerDefn
    {
        long long m_timeOfLastStatusChange;
        std::shared_ptr<LogBuffers> m_logBuffers;

        LogBuffersLingerDefn(long long now, std::shared_ptr<LogBuffers> buffers) :
            m_timeOfLastStatusChange(now),
            m_logBuffers(std::move(buffers))
        {
        }
    };

    std::recursive_mutex m_adminLock;

    std::vector<PublicationStateDefn> m_publications;
    std::vector<ExclusivePublicationStateDefn> m_exclusivePublications;
    std::vector<SubscriptionStateDefn> m_subscriptions;
    std::vector<CounterStateDefn> m_counters;

    std::vector<LogBuffersLingerDefn> m_lingeringLogBuffers;
    std::vector<ImageListLingerDefn> m_lingeringImageLists;

    DriverProxy& m_driverProxy;
    DriverListenerAdapter<ClientConductor> m_driverListenerAdapter;

    CountersReader m_countersReader;
    AtomicBuffer& m_counterValuesBuffer;

    on_new_publication_t m_onNewPublicationHandler;
    on_new_publication_t m_onNewExclusivePublicationHandler;
    on_new_subscription_t m_onNewSubscriptionHandler;
    exception_handler_t m_errorHandler;
    on_available_counter_t m_onAvailableCounterHandler;
    on_unavailable_counter_t m_onUnavailableCounterHandler;

    epoch_clock_t m_epochClock;
    long long m_timeOfLastKeepalive;
    long long m_timeOfLastCheckManagedResources;
    long long m_timeOfLastDoWork;
    long m_driverTimeoutMs;
    long m_resourceLingerTimeoutMs;
    long m_interServiceTimeoutMs;

    std::atomic<bool> m_driverActive;
    std::atomic<bool> m_isClosed;

    inline int onHeartbeatCheckTimeouts()
    {
        // TODO: use system nano clock since it is quicker to poll, then use epochClock only for driver activity

        const long long now = m_epochClock();
        int result = 0;

        if (now > (m_timeOfLastDoWork + m_interServiceTimeoutMs))
        {
            closeAllResources(now);

            ConductorServiceTimeoutException exception(
                "timeout between service calls over " + std::to_string(m_interServiceTimeoutMs) + " ms", SOURCEINFO);
            m_errorHandler(exception);
        }

        m_timeOfLastDoWork = now;

        if (now > (m_timeOfLastKeepalive + KEEPALIVE_TIMEOUT_MS))
        {
            m_driverProxy.sendClientKeepalive();

            if (now > (m_driverProxy.timeOfLastDriverKeepalive() + m_driverTimeoutMs))
            {
                m_driverActive = false;

                DriverTimeoutException exception(
                    "driver has been inactive for over " + std::to_string(m_driverTimeoutMs) + " ms", SOURCEINFO);
                m_errorHandler(exception);
            }

            m_timeOfLastKeepalive = now;
            result = 1;
        }

        if (now > (m_timeOfLastCheckManagedResources + RESOURCE_TIMEOUT_MS))
        {
            onCheckManagedResources(now);
            m_timeOfLastCheckManagedResources = now;
            result = 1;
        }

        return result;
    }

    inline void verifyDriverIsActive()
    {
        if (!m_driverActive)
        {
            throw DriverTimeoutException("driver is inactive", SOURCEINFO);
        }
    }

    inline void verifyDriverIsActiveViaErrorHandler()
    {
        if (!m_driverActive)
        {
            DriverTimeoutException exception("driver is inactive", SOURCEINFO);
            m_errorHandler(exception);
        }
    }

    inline void ensureOpen()
    {
        if (isClosed())
        {
            throw AeronException("Aeron client conductor is closed", SOURCEINFO);
        }
    }
};

}

#endif
