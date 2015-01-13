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

#include "ClientConductor.h"

using namespace aeron;

Publication* ClientConductor::addPublication(const std::string& channel, std::int32_t streamId, std::int32_t sessionId)
{
    std::lock_guard<std::mutex> lock(m_publicationsLock);

    std::vector<Publication*>::const_iterator it = std::find_if(m_publications.begin(), m_publications.end(),
        [&](Publication *pub)
        {
            return (streamId == pub->streamId() && sessionId == pub->sessionId() && channel == pub->channel());
        });

    Publication* publication = nullptr;

    if (it == m_publications.end())
    {
        std::int64_t correlationId = m_driverProxy.addPublication(channel, streamId, sessionId);
        Publication::Identification ident(channel, correlationId, streamId, sessionId);

        publication = new Publication(*this, ident);

        m_publications.push_back(publication);
    }
    else
    {
        publication = *it;
    }

    return publication;
}

void ClientConductor::releasePublication(Publication* publication)
{
    std::lock_guard<std::mutex> lock(m_publicationsLock);

    // TODO: send command to driver?

    std::vector<Publication*>::const_iterator it = std::find_if(m_publications.begin(), m_publications.end(),
        [&](Publication *pub)
        {
            return (publication == pub);
        });

    if (it != m_publications.end())
    {
        m_publications.erase(it);
    }
}

Subscription* ClientConductor::addSubscription(const std::string& channel, std::int32_t streamId, logbuffer::handler_t& handler)
{
    std::lock_guard<std::mutex> lock(m_subscriptionsLock);

    std::vector<Subscription*>::const_iterator it = std::find_if(m_subscriptions.begin(), m_subscriptions.end(),
        [&](Subscription* sub)
        {
            return (streamId == sub->streamId() && channel == sub->channel());
        });

    Subscription* subscription = nullptr;

    if (it == m_subscriptions.end())
    {
        subscription = new Subscription(*this, channel, streamId);

        m_subscriptions.push_back(subscription);
        // TODO: send command to driver
    }
    else
    {
        subscription = *it;
    }

    return subscription;
}

void ClientConductor::releaseSubscription(Subscription* subscription)
{
    std::lock_guard<std::mutex> lock(m_subscriptionsLock);

    // TODO: send command to driver?

    std::vector<Subscription*>::const_iterator it = std::find_if(m_subscriptions.begin(), m_subscriptions.end(),
        [&](Subscription* sub)
        {
            return (subscription == sub);
        });

    if (it != m_subscriptions.end())
    {
        m_subscriptions.erase(it);
    }
}

void ClientConductor::onNewPublication(
    std::int64_t correlationId,
    const std::string& channel,
    std::int32_t streamId,
    std::int32_t sessionId,
    std::int32_t termId,
    std::int32_t positionCounterId,
    std::int32_t mtuLengt,
    const PublicationReadyFlyweight& publicationReady)
{

}
