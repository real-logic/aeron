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

#ifndef INCLUDED_AERON_CLIENT_CONDUCTOR__
#define INCLUDED_AERON_CLIENT_CONDUCTOR__

#include <vector>
#include <mutex>
#include <concurrent/logbuffer/TermReader.h>
#include <concurrent/status/UnsafeBufferPosition.h>
#include <util/LangUtil.h>
#include "Publication.h"
#include "Subscription.h"
#include "DriverProxy.h"
#include "Context.h"
#include "DriverListenerAdapter.h"
#include "LogBuffers.h"

namespace aeron {

using namespace aeron::concurrent::logbuffer;
using namespace aeron::concurrent::status;
using namespace aeron::concurrent;

typedef std::function<long()> epoch_clock_t;
typedef std::function<long()> nano_clock_t;

static const long KEEPALIVE_TIMEOUT_MS = 500;
static const long RESOURCE_TIMEOUT_MS = 1000;

class ClientConductor
{
public:

    ClientConductor(
        epoch_clock_t epochClock,
        DriverProxy& driverProxy,
        CopyBroadcastReceiver& broadcastReceiver,
        AtomicBuffer& counterValuesBuffer,
        const on_new_publication_t& newPublicationHandler,
        const on_new_subscription_t& newSubscriptionHandler,
        const on_available_image_t & newImageHandler,
        const on_unavailable_image_t & inactiveImageHandler,
        const exception_handler_t& errorHandler,
        long driverTimeoutMs,
        long resourceLingerTimeoutMs,
        long interServiceTimeoutNs,
        long publicationConnectionTimeoutMs) :
        m_driverProxy(driverProxy),
        m_driverListenerAdapter(broadcastReceiver, *this),
        m_counterValuesBuffer(counterValuesBuffer),
        m_onNewPublicationHandler(newPublicationHandler),
        m_onNewSubscriptionHandler(newSubscriptionHandler),
        m_onAvailableImageHandler(newImageHandler),
        m_onUnavailableImageHandler(inactiveImageHandler),
        m_errorHandler(errorHandler),
        m_epochClock(epochClock),
        m_timeOfLastKeepalive(epochClock()),
        m_timeOfLastCheckManagedResources(epochClock()),
        m_timeOfLastDoWork(epochClock()),
        m_driverTimeoutMs(driverTimeoutMs),
        m_resourceLingerTimeoutMs(resourceLingerTimeoutMs),
        m_interServiceTimeoutMs(interServiceTimeoutNs / 1000000),
        m_publicationConnectionTimeoutMs(publicationConnectionTimeoutMs),
        m_driverActive(true)
    {
    }

    virtual ~ClientConductor();

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

    std::int64_t addSubscription(const std::string& channel, std::int32_t streamId);
    std::shared_ptr<Subscription> findSubscription(std::int64_t registrationId);
    void releaseSubscription(std::int64_t registrationId, Image *images, int imagesLength);

    void onNewPublication(
        std::int32_t streamId,
        std::int32_t sessionId,
        std::int32_t positionLimitCounterId,
        const std::string& logFileName,
        std::int64_t registrationId);

    void onOperationSuccess(std::int64_t correlationId);

    void onErrorResponse(
        std::int64_t offendingCommandCorrelationId,
        std::int32_t errorCode,
        const std::string& errorMessage);

    void onAvailableImage(
        std::int32_t streamId,
        std::int32_t sessionId,
        const std::string &logFilename,
        const std::string &sourceIdentity,
        std::int32_t subscriberPositionCount,
        const ImageBuffersReadyDefn::SubscriberPosition *subscriberPositions,
        std::int64_t correlationId);

    void onUnavailableImage(
        std::int32_t streamId,
        std::int64_t correlationId);

    void onInterServiceTimeout(long now);

    inline bool isPublicationConnected(std::int64_t timeOfLastSm)
    {
        return (m_epochClock() <= (timeOfLastSm + m_publicationConnectionTimeoutMs));
    }

protected:
    void onCheckManagedResources(long now);

    void lingerResource(long now, Image * array);
    void lingerResource(long now, std::shared_ptr<LogBuffers> logBuffers);
    void lingerResources(long now, Image *images, int connectionsLength);

private:
    enum class RegistrationStatus
    {
        AWAITING_MEDIA_DRIVER, REGISTERED_MEDIA_DRIVER, ERRORED_MEDIA_DRIVER
    };

    struct PublicationStateDefn
    {
        std::string m_channel;
        std::int64_t m_registrationId;
        std::int32_t m_streamId;
        std::int32_t m_sessionId = -1;
        std::int32_t m_positionLimitCounterId = -1;
        long m_timeOfRegistration;
        RegistrationStatus m_status = RegistrationStatus::AWAITING_MEDIA_DRIVER;
        std::int32_t m_errorCode;
        std::string m_errorMessage;
        std::shared_ptr<LogBuffers> m_buffers;
        std::weak_ptr<Publication> m_publication;

        PublicationStateDefn(
            const std::string& channel, std::int64_t registrationId, std::int32_t streamId, long now) :
            m_channel(channel), m_registrationId(registrationId), m_streamId(streamId), m_timeOfRegistration(now)
        {
        }
    };

    struct SubscriptionStateDefn
    {
        std::string m_channel;
        std::int64_t m_registrationId;
        std::int32_t m_streamId;
        long m_timeOfRegistration;
        RegistrationStatus m_status = RegistrationStatus::AWAITING_MEDIA_DRIVER;
        std::int32_t m_errorCode;
        std::string m_errorMessage;
        std::shared_ptr<Subscription> m_subscriptionCache;
        std::weak_ptr<Subscription> m_subscription;

        SubscriptionStateDefn(
            const std::string& channel, std::int64_t registrationId, std::int32_t streamId, long now) :
            m_channel(channel), m_registrationId(registrationId), m_streamId(streamId), m_timeOfRegistration(now)
        {
        }
    };

    struct ImageArrayLingerDefn
    {
        long m_timeOfLastStatusChange;
        Image * m_array;

        ImageArrayLingerDefn(long now, Image *array) :
            m_timeOfLastStatusChange(now), m_array(array)
        {
        }
    };

    struct LogBuffersLingerDefn
    {
        long m_timeOfLastStatusChange;
        std::shared_ptr<LogBuffers> m_logBuffers;

        LogBuffersLingerDefn(long now, std::shared_ptr<LogBuffers> buffers) :
            m_timeOfLastStatusChange(now), m_logBuffers(buffers)
        {
        }
    };

    std::recursive_mutex m_adminLock;

    std::vector<PublicationStateDefn> m_publications;
    std::vector<SubscriptionStateDefn> m_subscriptions;

    std::vector<LogBuffersLingerDefn> m_lingeringLogBuffers;
    std::vector<ImageArrayLingerDefn> m_lingeringImageArrays;

    DriverProxy& m_driverProxy;
    DriverListenerAdapter<ClientConductor> m_driverListenerAdapter;

    AtomicBuffer& m_counterValuesBuffer;

    on_new_publication_t m_onNewPublicationHandler;
    on_new_subscription_t m_onNewSubscriptionHandler;
    on_available_image_t m_onAvailableImageHandler;
    on_unavailable_image_t m_onUnavailableImageHandler;
    exception_handler_t m_errorHandler;

    epoch_clock_t m_epochClock;
    long m_timeOfLastKeepalive;
    long m_timeOfLastCheckManagedResources;
    long m_timeOfLastDoWork;
    long m_driverTimeoutMs;
    long m_resourceLingerTimeoutMs;
    long m_interServiceTimeoutMs;
    long m_publicationConnectionTimeoutMs;

    std::atomic<bool> m_driverActive;

    inline int onHeartbeatCheckTimeouts()
    {
        // TODO: use system nano clock since it is quicker to poll, then use epochClock only for driver activity

        const long now = m_epochClock();
        int result = 0;

        if (now > (m_timeOfLastDoWork + m_interServiceTimeoutMs))
        {
            onInterServiceTimeout(now);

            ConductorServiceTimeoutException exception(
                strPrintf("Timeout between service calls over %d ms", m_interServiceTimeoutMs), SOURCEINFO);
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
                    strPrintf("Driver has been inactive for over %d ms", m_driverTimeoutMs), SOURCEINFO);
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
            throw DriverTimeoutException("Driver is inactive", SOURCEINFO);
        }
    }
};

}

#endif