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
        const on_new_connection_t& newConnectionHandler,
        const on_inactive_connection_t& inactiveConnectionHandler,
        long driverTimeoutMs,
        long resourceLingerTimeoutMs) :
        m_driverProxy(driverProxy),
        m_driverListenerAdapter(broadcastReceiver, *this),
        m_counterValuesBuffer(counterValuesBuffer),
        m_onNewPublicationHandler(newPublicationHandler),
        m_onNewSubscpriptionHandler(newSubscriptionHandler),
        m_onNewConnectionHandler(newConnectionHandler),
        m_onInactiveConnectionHandler(inactiveConnectionHandler),
        m_epochClock(epochClock),
        m_timeOfLastKeepalive(epochClock()),
        m_timeOfLastCheckManagedResources(epochClock()),
        m_driverTimeoutMs(driverTimeoutMs),
        m_resourceLingerTimeoutMs(resourceLingerTimeoutMs)
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

    /*
     * non-blocking API semantics
     * - addPublication, addSubscription do NOT return objects, but instead return a registrationId
     * - addPublication/addSubscription should NOT take futures for completion
     * - onNewPublication -> deliver notification via Aeron to inform app (but not hand back Publication, just id)
     * - onNewSubscription -> deliver notification via Aeron to inform app (but not hand back Subscription, just id)
     * - onNewConnection -> deliver notification (as is done currently in Java)
     * - on error [timeout or error return] -> deliver notification via errorHandler of Aeron
     * - app can poll for usage of Publication/Subscription (concurrent array/map)
     *      - use correlationId as key
     */

    /*
     * addPublication - create unique_ptr<state entry> (if doesn't exist), send command, etc.
     * onNewPublication - create buffers, save them to state entry
     * findPublication - create Publication (if it doesn't exist), returning shared_ptr, keep weak_ptr.
     * releasePublication - delete unique_ptr<state entry> (should be called by Publication dtor) (should only delete once operation success from driver?)
     */

    std::int64_t addPublication(const std::string& channel, std::int32_t streamId, std::int32_t sessionId);
    std::shared_ptr<Publication> findPublication(std::int64_t registrationId);
    void releasePublication(std::int64_t registrationId);

    std::int64_t addSubscription(const std::string& channel, std::int32_t streamId);
    std::shared_ptr<Subscription> findSubscription(std::int64_t registrationId);
    void releaseSubscription(std::int64_t registrationId);

    void onNewPublication(
        std::int32_t streamId,
        std::int32_t sessionId,
        std::int32_t positionLimitCounterId,
        const std::string& logFileName,
        std::int64_t registrationId);

    void onOperationSuccess(std::int64_t correlationId);

    void onNewConnection(
        std::int32_t streamId,
        std::int32_t sessionId,
        std::int64_t joiningPosition,
        const std::string& logFilename,
        const std::string& sourceIdentity,
        std::int32_t subscriberPositionCount,
        const ConnectionBuffersReadyDefn::SubscriberPosition* subscriberPositions,
        std::int64_t correlationId);

    void onInactiveConnection(
        std::int32_t streamId,
        std::int32_t sessionId,
        std::int64_t position,
        std::int64_t correlationId);

    void onCheckManagedResources(long now);

private:
    struct PublicationStateDefn
    {
        std::string m_channel;
        std::int64_t m_registrationId;
        std::int32_t m_streamId;
        std::int32_t m_sessionId;
        std::int32_t m_positionLimitCounterId = -1;
        std::shared_ptr<LogBuffers> m_buffers;
        std::weak_ptr<Publication> m_publication;

        PublicationStateDefn(
            const std::string& channel, std::int64_t registrationId, std::int32_t streamId, std::int32_t sessionId) :
            m_channel(channel), m_registrationId(registrationId), m_streamId(streamId), m_sessionId(sessionId)
        {
        }
    };

    struct SubscriptionStateDefn
    {
        std::string m_channel;
        std::int64_t m_registrationId;
        std::int64_t m_removeCorrelationId = -1;
        std::int32_t m_streamId;
        bool m_registered = false;
        std::shared_ptr<Subscription> m_subscription;

        SubscriptionStateDefn(
            const std::string& channel, std::int64_t registrationId, std::int32_t streamId) :
            m_channel(channel), m_registrationId(registrationId), m_streamId(streamId)
        {
        }
    };

    struct LogBuffersStateDefn
    {
        std::int64_t m_correlationId;
        std::shared_ptr<LogBuffers> m_logBuffers;

        LogBuffersStateDefn(std::int64_t correlationId, std::shared_ptr<LogBuffers> buffers) :
            m_correlationId(correlationId), m_logBuffers(buffers)
        {
        }
    };

    struct ConnectionArrayLingerDefn
    {
        long m_timeOfLastStatusChange;
        Connection* m_array;

        ConnectionArrayLingerDefn(long now, Connection *array) :
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

    std::mutex m_adminLock;

    std::vector<PublicationStateDefn> m_publications;
    std::vector<SubscriptionStateDefn> m_subscriptions;

    std::vector<LogBuffersStateDefn> m_logBuffers;

    std::vector<LogBuffersLingerDefn> m_lingeringLogBuffers;
    std::vector<ConnectionArrayLingerDefn> m_lingeringConnectionArrays;

    DriverProxy& m_driverProxy;
    DriverListenerAdapter<ClientConductor> m_driverListenerAdapter;

    AtomicBuffer& m_counterValuesBuffer;

    on_new_publication_t m_onNewPublicationHandler;
    on_new_subscription_t m_onNewSubscpriptionHandler;
    on_new_connection_t m_onNewConnectionHandler;
    on_inactive_connection_t m_onInactiveConnectionHandler;

    epoch_clock_t m_epochClock;
    long m_timeOfLastKeepalive;
    long m_timeOfLastCheckManagedResources;
    long m_driverTimeoutMs;
    long m_resourceLingerTimeoutMs;

    inline int onHeartbeatCheckTimeouts()
    {
        // TODO: use system nano clock since it is quicker to poll, then use epochClock only for driver activity

        const long now = m_epochClock();
        int result = 0;

        if (now > (m_timeOfLastKeepalive + KEEPALIVE_TIMEOUT_MS))
        {
            m_driverProxy.sendClientKeepalive();

            if (now > (m_driverProxy.timeOfLastDriverKeepalive() + m_driverTimeoutMs))
            {
                // TODO: set driverActive to false and call error handler
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
};

}

#endif