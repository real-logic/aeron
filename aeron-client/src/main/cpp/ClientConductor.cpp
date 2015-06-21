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

namespace aeron {

std::int64_t ClientConductor::addPublication(const std::string &channel, std::int32_t streamId, std::int32_t sessionId)
{
    std::lock_guard<std::mutex> lock(m_adminLock);
    std::int64_t id;

    std::vector<PublicationStateDefn>::const_iterator it = std::find_if(m_publications.begin(), m_publications.end(),
        [&](PublicationStateDefn &entry)
        {
            return (streamId == entry.m_streamId && sessionId == entry.m_sessionId && channel == entry.m_channel);
        });

    if (it == m_publications.end())
    {
        std::int64_t registrationId = m_driverProxy.addPublication(channel, streamId, sessionId);

        m_publications.push_back(PublicationStateDefn(channel, registrationId, streamId, sessionId));
        id = registrationId;
    }
    else
    {
        id = (*it).m_registrationId;
    }

    return id;
}

std::shared_ptr<Publication> ClientConductor::findPublication(std::int64_t registrationId)
{
    std::lock_guard<std::mutex> lock(m_adminLock);

    std::vector<PublicationStateDefn>::iterator it = std::find_if(m_publications.begin(), m_publications.end(),
        [&](PublicationStateDefn &entry)
        {
            return (registrationId == entry.m_registrationId);
        });

    if (it == m_publications.end())
    {
        return std::shared_ptr<Publication>();
    }

    std::shared_ptr<Publication> pub((*it).m_publication.lock());

    // construct Publication if we've heard from the driver and have the log buffers around
    if (!pub && ((*it).m_buffers))
    {
        UnsafeBufferPosition publicationLimit(m_counterValuesBuffer, (*it).m_positionLimitCounterId);

        pub = std::make_shared<Publication>(*this, (*it).m_channel, (*it).m_registrationId, (*it).m_streamId,
            (*it).m_sessionId, publicationLimit, *((*it).m_buffers));

        (*it).m_publication = std::weak_ptr<Publication>(pub);
        return pub;
    }

    return pub;
}

void ClientConductor::releasePublication(std::int64_t registrationId)
{
    std::lock_guard<std::mutex> lock(m_adminLock);

    std::vector<PublicationStateDefn>::iterator it = std::find_if(m_publications.begin(), m_publications.end(),
        [&](PublicationStateDefn &entry)
        {
            return (registrationId == entry.m_registrationId);
        });

    if (it != m_publications.end())
    {
        m_driverProxy.removePublication(registrationId);
        m_publications.erase(it);
    }
}

std::int64_t ClientConductor::addSubscription(const std::string &channel, std::int32_t streamId)
{
    std::lock_guard<std::mutex> lock(m_adminLock);

    std::int64_t registrationId = m_driverProxy.addSubscription(channel, streamId);

    m_subscriptions.push_back(SubscriptionStateDefn(channel, registrationId, streamId));

    return registrationId;
}

std::shared_ptr<Subscription> ClientConductor::findSubscription(std::int64_t registrationId)
{
    std::lock_guard<std::mutex> lock(m_adminLock);

    std::vector<SubscriptionStateDefn>::iterator it = std::find_if(m_subscriptions.begin(), m_subscriptions.end(),
        [&](SubscriptionStateDefn &entry)
        {
            return (registrationId == entry.m_registrationId);
        });

    if (it != m_subscriptions.end() && (*it).m_registered)
    {
        std::shared_ptr<Subscription> sub = (*it).m_subscription;

        if (sub == nullptr)
        {
            sub = std::make_shared<Subscription>(*this, (*it).m_registrationId, (*it).m_channel, (*it).m_streamId);

            (*it).m_subscription = sub;
        }
        return sub;
    }

    return std::shared_ptr<Subscription>();
}

void ClientConductor::releaseSubscription(std::int64_t registrationId)
{
    std::lock_guard<std::mutex> lock(m_adminLock);

    std::vector<SubscriptionStateDefn>::iterator it = std::find_if(m_subscriptions.begin(), m_subscriptions.end(),
        [&](SubscriptionStateDefn &entry)
        {
            return (registrationId == entry.m_registrationId);
        });

    if (it != m_subscriptions.end())
    {
        (*it).m_removeCorrelationId = m_driverProxy.removePublication((*it).m_registrationId);
    }
}

void ClientConductor::onNewPublication(
    std::int32_t streamId,
    std::int32_t sessionId,
    std::int32_t positionLimitCounterId,
    const std::string &logFileName,
    std::int64_t registrationId)
{
    std::lock_guard<std::mutex> lock(m_adminLock);

    std::vector<PublicationStateDefn>::iterator it = std::find_if(m_publications.begin(), m_publications.end(),
        [&](PublicationStateDefn &entry)
        {
            return (registrationId == entry.m_registrationId);
        });

    if (it != m_publications.end())
    {
        (*it).m_positionLimitCounterId = positionLimitCounterId;
        (*it).m_buffers = std::make_shared<LogBuffers>(logFileName.c_str());

        m_onNewPublicationHandler((*it).m_channel, streamId, sessionId, registrationId);
    }
}

void ClientConductor::onOperationSuccess(std::int64_t correlationId)
{
    std::lock_guard<std::mutex> lock(m_adminLock);

    std::vector<SubscriptionStateDefn>::iterator it = std::find_if(m_subscriptions.begin(), m_subscriptions.end(),
        [&](SubscriptionStateDefn &entry)
        {
            return (correlationId == entry.m_registrationId || correlationId == entry.m_removeCorrelationId);
        });

    if (it != m_subscriptions.end())
    {
        if (correlationId == (*it).m_registrationId)
        {
            (*it).m_registered = true;
            m_onNewSubscpriptionHandler((*it).m_channel, (*it).m_streamId, correlationId);
        }
        else if (correlationId == (*it).m_removeCorrelationId)
        {
            // TODO: inform API of close
            m_subscriptions.erase(it);
        }
        return;
    }

    // TODO: do same for publications for close
}

void ClientConductor::onNewConnection(
    std::int32_t streamId,
    std::int32_t sessionId,
    std::int64_t joiningPosition,
    const std::string& logFilename,
    const std::string& sourceIdentity,
    std::int32_t subscriberPositionCount,
    const ConnectionBuffersReadyDefn::SubscriberPosition* subscriberPositions,
    std::int64_t correlationId)
{
    std::lock_guard<std::mutex> lock(m_adminLock);

    std::for_each(m_subscriptions.begin(), m_subscriptions.end(),
        [&](SubscriptionStateDefn &entry)
        {
            if (streamId == entry.m_streamId)
            {
                std::shared_ptr<Subscription> subscription = entry.m_subscription;

                if (subscription != nullptr &&
                    !(subscription->isConnected(sessionId)))
                {
                    for (int i = 0; i < subscriberPositionCount; i++)
                    {
                        if (subscription->registrationId() == subscriberPositions[i].registrationId)
                        {
                            std::shared_ptr<LogBuffers> logBuffers = std::make_shared<LogBuffers>(logFilename.c_str());

                            UnsafeBufferPosition subscriberPosition(m_counterValuesBuffer, subscriberPositions[i].indicatorId);

                            Connection connection(
                                sessionId, joiningPosition, correlationId, subscriberPosition, *logBuffers);

                            Connection* oldArray = subscription->addConnection(connection);

                            if (nullptr != oldArray)
                            {
                                m_lingeringConnectionArrays.push_back(ConnectionArrayLingerDefn(m_epochClock(), oldArray));
                            }

                            m_logBuffers.push_back(LogBuffersStateDefn(correlationId, logBuffers));

                            m_onNewConnectionHandler(
                                subscription->channel(), streamId, sessionId, joiningPosition, sourceIdentity);
                            break;
                        }
                    }
                }
            }
        });
}

void ClientConductor::onInactiveConnection(
    std::int32_t streamId,
    std::int32_t sessionId,
    std::int64_t position,
    std::int64_t correlationId)
{
    const long now = m_epochClock();
    std::lock_guard<std::mutex> lock(m_adminLock);

    std::for_each(m_subscriptions.begin(), m_subscriptions.end(),
        [&](SubscriptionStateDefn &entry)
        {
            if (streamId == entry.m_streamId)
            {
                std::shared_ptr<Subscription> subscription = entry.m_subscription;

                Connection *oldArray = subscription->removeConnection(correlationId);

                if (nullptr != oldArray)
                {
                    m_lingeringConnectionArrays.push_back(ConnectionArrayLingerDefn(now, oldArray));
                    m_onInactiveConnectionHandler(subscription->channel(), streamId, sessionId, position);
                }
            }
        });

    // erase-remove idiom
    std::vector<LogBuffersStateDefn>::iterator it = std::remove_if(m_logBuffers.begin(), m_logBuffers.end(),
        [&](LogBuffersStateDefn& entry)
        {
            if (correlationId == entry.m_correlationId)
            {
                m_lingeringLogBuffers.push_back(LogBuffersLingerDefn(now, entry.m_logBuffers));
                return true;
            }

            return false;
        });

    m_logBuffers.erase(it, m_logBuffers.end());
}

void ClientConductor::onCheckManagedResources(long now)
{
    // erase-remove idiom

    // check LogBuffers
    std::vector<LogBuffersLingerDefn>::iterator logIt =
        std::remove_if(m_lingeringLogBuffers.begin(), m_lingeringLogBuffers.end(),
            [&](LogBuffersLingerDefn& entry)
            {
                return (now > (entry.m_timeOfLastStatusChange + m_resourceLingerTimeoutMs));
            });

    m_lingeringLogBuffers.erase(logIt, m_lingeringLogBuffers.end());

    // check old arrays
    std::vector<ConnectionArrayLingerDefn>::iterator arrayIt =
        std::remove_if(m_lingeringConnectionArrays.begin(), m_lingeringConnectionArrays.end(),
            [&](ConnectionArrayLingerDefn& entry)
            {
                if (now > (entry.m_timeOfLastStatusChange + m_resourceLingerTimeoutMs))
                {
                    delete[] entry.m_array;
                    entry.m_array = nullptr;
                    return true;
                }

                return false;
            });

    m_lingeringConnectionArrays.erase(arrayIt, m_lingeringConnectionArrays.end());
}

}