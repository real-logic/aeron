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

#include "ClientConductor.h"

using namespace aeron;

std::int64_t ClientConductor::addPublication(const std::string& channel, std::int32_t streamId, std::int32_t sessionId)
{
    std::lock_guard<std::mutex> lock(m_publicationsLock);
    std::int64_t id;

    std::vector<PublicationStateDefn>::const_iterator it = std::find_if(m_publications.begin(), m_publications.end(),
        [&](PublicationStateDefn & entry)
        {
            return (streamId == entry.m_streamId && sessionId == entry.m_sessionId && channel == entry.m_channel);
        });

    if (it == m_publications.end())
    {
        std::int64_t correlationId = m_driverProxy.addPublication(channel, streamId, sessionId);

        m_publications.push_back(PublicationStateDefn(channel, correlationId, streamId, sessionId));
        id = correlationId;
    }
    else
    {
        id = (*it).m_correlationId;
    }

    return id;
}

std::shared_ptr<Publication> ClientConductor::findPublication(std::int64_t correlationId)
{
    std::lock_guard<std::mutex> lock(m_publicationsLock);

    std::vector<PublicationStateDefn>::iterator it = std::find_if(m_publications.begin(), m_publications.end(),
        [&](PublicationStateDefn & entry)
        {
            return (correlationId == entry.m_correlationId);
        });

    if (it == m_publications.end())
    {
        return std::shared_ptr<Publication>();
    }

    std::shared_ptr<Publication> pub((*it).m_publication.lock());

    // construct Publication if we've heard from the driver and have the log buffers around
    if (!pub && ((*it).m_buffers))
    {
        pub = std::make_shared<Publication>(*this, (*it).m_channel, (*it).m_correlationId, (*it).m_streamId, (*it).m_sessionId, *((*it).m_buffers));

        (*it).m_publication = std::weak_ptr<Publication>(pub);
        return pub;
    }

    return pub;
}

void ClientConductor::releasePublication(std::int64_t correlationId)
{
    std::lock_guard<std::mutex> lock(m_publicationsLock);

    std::vector<PublicationStateDefn>::iterator it = std::find_if(m_publications.begin(), m_publications.end(),
        [&](PublicationStateDefn & entry)
        {
            return (correlationId == entry.m_correlationId);
        });

    if (it != m_publications.end())
    {
        m_driverProxy.removePublication(correlationId);
        m_publications.erase(it);
    }
}

std::int64_t ClientConductor::addSubscription(const std::string& channel, std::int32_t streamId, logbuffer::handler_t& handler)
{
    std::lock_guard<std::mutex> lock(m_subscriptionsLock);
    std::int64_t id;

    std::vector<SubscriptionStateDefn>::const_iterator it = std::find_if(m_subscriptions.begin(), m_subscriptions.end(),
        [&](SubscriptionStateDefn& entry)
        {
            return (streamId == entry.m_streamId && channel == entry.m_channel);
        });


    if (it == m_subscriptions.end())
    {
        std::int64_t correlationId = m_driverProxy.addSubscription(channel, streamId);

        m_subscriptions.push_back(SubscriptionStateDefn(channel, correlationId, streamId, handler));
        id = correlationId;
    }
    else
    {
        id = (*it).m_correlationId;
    }

    return id;
}

std::shared_ptr<Subscription> ClientConductor::findSubscription(std::int64_t correlationId)
{
    std::lock_guard<std::mutex> lock(m_subscriptionsLock);

    std::vector<SubscriptionStateDefn>::iterator it = std::find_if(m_subscriptions.begin(), m_subscriptions.end(),
        [&](SubscriptionStateDefn& entry)
        {
            return (correlationId == entry.m_correlationId);
        });

    if (it == m_subscriptions.end())
    {
        return std::shared_ptr<Subscription>();
    }

    // TODO: construct initial Subscription if it has been acked by driver

    return (*it).m_subscription.lock();
}

void ClientConductor::releaseSubscription(std::int64_t correlationId)
{
    std::lock_guard<std::mutex> lock(m_subscriptionsLock);

    std::vector<SubscriptionStateDefn>::iterator it = std::find_if(m_subscriptions.begin(), m_subscriptions.end(),
        [&](SubscriptionStateDefn& entry)
        {
            return (correlationId == entry.m_correlationId);
        });

    if (it != m_subscriptions.end())
    {
        // TODO: send command to driver?
        m_subscriptions.erase(it);
    }
}

void ClientConductor::onNewPublication(
    const std::string& channel,
    std::int32_t streamId,
    std::int32_t sessionId,
    std::int32_t limitPositionIndicatorOffset,
    std::int32_t mtuLength,
    const std::string& logFileName,
    std::int64_t correlationId)
{
    std::lock_guard<std::mutex> lock(m_publicationsLock);

    std::vector<PublicationStateDefn>::iterator it = std::find_if(m_publications.begin(), m_publications.end(),
        [&](PublicationStateDefn& entry)
        {
            return (correlationId == entry.m_correlationId);
        });

    if (it != m_publications.end())
    {
        // TODO: create log buffers, etc. and set (*it).m_buffers to hold them
        (*it).m_buffers = std::make_shared<LogBuffers>(logFileName.c_str());
    }

    m_onNewPublicationHandler(channel, streamId, sessionId, correlationId);
}
