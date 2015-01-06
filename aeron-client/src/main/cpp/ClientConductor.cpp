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

template <typename A>
inline static std::size_t findInVector(std::vector<A>& container, std::function<int(A)> compare)
{
    std::size_t result = SIZE_MAX;

    for (std::size_t i = 0; i < container.size(); i++)
    {
        if (compare(container[i]) == 0)
        {
            result = i;
            break;
        }
    }

    return result;
}

Publication* ClientConductor::addPublication(const std::string& channel, std::int32_t streamId, std::int32_t sessionId)
{
    std::lock_guard<std::mutex> lock(m_publicationsLock);

    std::size_t element = findInVector<Publication*>(m_publications, [&](Publication* pub)
    {
        return (streamId == pub->streamId() && sessionId == pub->sessionId() && channel == pub->channel());
    });

    Publication* publication = nullptr;

    if (SIZE_MAX == element)
    {
        std::int64_t correlationId = 0;

        publication = new Publication(*this, channel, streamId, sessionId, correlationId);

        m_publications.push_back(publication);
    }
    else
    {
        publication = m_publications[element];
    }

    return publication;
}

void ClientConductor::releasePublication(Publication* publication)
{
    std::lock_guard<std::mutex> lock(m_publicationsLock);

    // TODO: send command to driver?

    std::size_t element = findInVector<Publication*>(m_publications, [&](Publication* pub)
    {
        return pub == publication;
    });

    if (SIZE_MAX != element)
    {
        m_publications.erase(m_publications.begin() + element);
    }
}

Subscription* ClientConductor::addSubscription(const std::string& channel, std::int32_t streamId, logbuffer::handler_t& handler)
{
    std::lock_guard<std::mutex> lock(m_subscriptionsLock);

    std::size_t element = findInVector<Subscription*>(m_subscriptions, [&](Subscription* sub)
    {
        return (streamId == sub->streamId() && channel == sub->channel());
    });

    Subscription* subscription = nullptr;

    if (SIZE_MAX == element)
    {
        subscription = new Subscription(*this, channel, streamId);

        m_subscriptions.push_back(subscription);
        // TODO: send command to driver
    }
    else
    {
        subscription = m_subscriptions[element];
    }

    return subscription;
}

void ClientConductor::releaseSubscription(Subscription* subscription)
{
    std::lock_guard<std::mutex> lock(m_subscriptionsLock);

    // TODO: send command to driver?

    std::size_t element = findInVector<Subscription*>(m_subscriptions, [&](Subscription* sub)
    {
        return sub == subscription;
    });

    if (SIZE_MAX != element)
    {
        m_subscriptions.erase(m_subscriptions.begin() + element);
    }
}
