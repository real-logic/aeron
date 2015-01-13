/*
 * Copyright 2014 Real Logic Ltd.
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

#include <concurrent/logbuffer/LogReader.h>
#include <vector>
#include <mutex>
#include "Publication.h"
#include "Subscription.h"
#include "DriverProxy.h"
#include "Context.h"
#include "DriverListenerAdapter.h"

namespace aeron {

using namespace aeron::common::concurrent;

class ClientConductor
{
public:
    ClientConductor(DriverProxy& driverProxy, CopyBroadcastReceiver& broadcastReceiver) :
        m_driverProxy(driverProxy),
        m_broadcastReceiver(broadcastReceiver),
        m_driverListenerAdapter(broadcastReceiver, *this)
    {

    }

    int doWork()
    {
        int workCount = 0;

        workCount += m_driverListenerAdapter.receiveMessages();

        return workCount;
    }

    void onClose()
    {
    }

    /*
     * non-blocking API semantics
     * - addPublication, addSubscription do NOT return objects, but instead return correlationIds
     * - addPublication/addSubscription should take futures for completion?
     * - onReadyFlyweight -> deliver notification via handler(correlationId, Publication) (2 handlers: 1 Pub + 1 Sub)
     *      - Publication/Subscription created on reception of Flyweight
     * - onError [timeout or error return] -> deliver notification via errorHandler (should call handler with NULL Pub/Sub?)
     * - app can poll for usage of Publication/Subscription (concurrent array/map)
     *      - use correlationId as key
     */

    Publication* addPublication(const std::string& channel, std::int32_t streamId, std::int32_t sessionId);
    void releasePublication(Publication* publication);

    Subscription* addSubscription(const std::string& channel, std::int32_t streamId, logbuffer::handler_t& handler);
    void releaseSubscription(Subscription* subscription);

    void onNewPublication(
        std::int64_t correlationId,
        const std::string& channel,
        std::int32_t streamId,
        std::int32_t sessionId,
        std::int32_t termId,
        std::int32_t positionCounterId,
        std::int32_t mtuLengt,
        const PublicationReadyFlyweight& publicationReady);

private:
    std::mutex m_publicationsLock;
    std::mutex m_subscriptionsLock;
    std::vector<Publication*> m_publications;
    std::vector<Subscription*> m_subscriptions;
    DriverProxy& m_driverProxy;
    CopyBroadcastReceiver& m_broadcastReceiver;
    DriverListenerAdapter<ClientConductor> m_driverListenerAdapter;
};

}

#endif