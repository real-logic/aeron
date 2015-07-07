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

ClientConductor::~ClientConductor()
{
    std::vector<std::shared_ptr<Subscription>> subscriptions;

    std::for_each(m_subscriptions.begin(), m_subscriptions.end(),
        [&](SubscriptionStateDefn& entry)
        {
            subscriptions.push_back(entry.m_subscriptionCache);
            entry.m_subscriptionCache.reset();
        });

    std::for_each(m_lingeringImageArrays.begin(), m_lingeringImageArrays.end(),
        [](ImageArrayLingerDefn & entry)
        {
            delete[] entry.m_array;
            entry.m_array = nullptr;
        });
}

std::int64_t ClientConductor::addPublication(const std::string &channel, std::int32_t streamId, std::int32_t sessionId)
{
    verifyDriverIsActive();

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

        m_publications.push_back(PublicationStateDefn(channel, registrationId, streamId, sessionId, m_epochClock()));
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

    PublicationStateDefn& state = (*it);
    std::shared_ptr<Publication> pub(state.m_publication.lock());

    if (!pub)
    {
        switch (state.m_status)
        {
            case RegistrationStatus::AWAITING_MEDIA_DRIVER:
                if (m_epochClock() > (state.m_timeOfRegistration + m_driverTimeoutMs))
                {
                    throw DriverTimeoutException(
                        strPrintf("No response from driver in %d ms", m_driverTimeoutMs), SOURCEINFO);
                }
                break;

            case RegistrationStatus::REGISTERED_MEDIA_DRIVER:
                {
                    UnsafeBufferPosition publicationLimit(m_counterValuesBuffer, state.m_positionLimitCounterId);

                    pub = std::make_shared<Publication>(*this, state.m_channel, state.m_registrationId, state.m_streamId,
                        state.m_sessionId, publicationLimit, *(state.m_buffers));

                    state.m_publication = std::weak_ptr<Publication>(pub);
                }
                break;

            case RegistrationStatus::ERRORED_MEDIA_DRIVER:
                throw RegistrationException(state.m_errorCode, state.m_errorMessage, SOURCEINFO);
        }
    }

    return pub;
}

void ClientConductor::releasePublication(std::int64_t registrationId)
{
    verifyDriverIsActive();

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
    verifyDriverIsActive();

    std::lock_guard<std::mutex> lock(m_adminLock);

    std::int64_t registrationId = m_driverProxy.addSubscription(channel, streamId);

    m_subscriptions.push_back(SubscriptionStateDefn(channel, registrationId, streamId, m_epochClock()));

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

    if (it == m_subscriptions.end())
    {
        return std::shared_ptr<Subscription>();
    }

    SubscriptionStateDefn& state = *it;
    std::shared_ptr<Subscription> sub = state.m_subscription.lock();

    // now remove the cached value
    if (state.m_subscriptionCache)
    {
        state.m_subscriptionCache.reset();
    }

    if (!sub && RegistrationStatus::AWAITING_MEDIA_DRIVER == state.m_status)
    {
        if (m_epochClock() > (state.m_timeOfRegistration + m_driverTimeoutMs))
        {
            throw DriverTimeoutException(
                strPrintf("No response from driver in %d ms", m_driverTimeoutMs), SOURCEINFO);
        }
    }
    else if (!sub && RegistrationStatus::ERRORED_MEDIA_DRIVER == state.m_status)
    {
        throw RegistrationException(state.m_errorCode, state.m_errorMessage, SOURCEINFO);
    }

    return sub;
}

void ClientConductor::releaseSubscription(std::int64_t registrationId, Image * connections, int connectionsLength)
{
    verifyDriverIsActive();

    std::lock_guard<std::mutex> lock(m_adminLock);

    std::vector<SubscriptionStateDefn>::iterator it = std::find_if(m_subscriptions.begin(), m_subscriptions.end(),
        [&](SubscriptionStateDefn &entry)
        {
            return (registrationId == entry.m_registrationId);
        });

    if (it != m_subscriptions.end())
    {
        m_driverProxy.removeSubscription((*it).m_registrationId);
        m_subscriptions.erase(it);

        lingerResources(m_epochClock(), connections, connectionsLength);
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
        (*it).m_status = RegistrationStatus::REGISTERED_MEDIA_DRIVER;
        (*it).m_positionLimitCounterId = positionLimitCounterId;
        (*it).m_buffers = std::make_shared<LogBuffers>(logFileName.c_str());

        m_onNewPublicationHandler((*it).m_channel, streamId, sessionId, registrationId);
    }
}

void ClientConductor::onOperationSuccess(std::int64_t correlationId)
{
    std::lock_guard<std::mutex> lock(m_adminLock);

    std::vector<SubscriptionStateDefn>::iterator subIt = std::find_if(m_subscriptions.begin(), m_subscriptions.end(),
        [&](SubscriptionStateDefn &entry)
        {
            return (correlationId == entry.m_registrationId);
        });

    if (subIt != m_subscriptions.end() && (*subIt).m_status == RegistrationStatus::AWAITING_MEDIA_DRIVER)
    {
        (*subIt).m_status = RegistrationStatus::REGISTERED_MEDIA_DRIVER;
        (*subIt).m_subscriptionCache =
            std::make_shared<Subscription>(*this, (*subIt).m_registrationId, (*subIt).m_channel, (*subIt).m_streamId);
        (*subIt).m_subscription = std::weak_ptr<Subscription>((*subIt).m_subscriptionCache);
        m_onNewSubscpriptionHandler((*subIt).m_channel, (*subIt).m_streamId, correlationId);
        return;
    }
}

void ClientConductor::onErrorResponse(
    std::int64_t offendingCommandCorrelationId,
    std::int32_t errorCode,
    const std::string& errorMessage)
{
    std::lock_guard<std::mutex> lock(m_adminLock);

    std::vector<SubscriptionStateDefn>::iterator subIt = std::find_if(m_subscriptions.begin(), m_subscriptions.end(),
        [&](SubscriptionStateDefn &entry)
        {
            return (offendingCommandCorrelationId == entry.m_registrationId);
        });

    if (subIt != m_subscriptions.end())
    {
        (*subIt).m_status = RegistrationStatus::ERRORED_MEDIA_DRIVER;
        (*subIt).m_errorCode = errorCode;
        (*subIt).m_errorMessage = errorMessage;
        return;
    }

    std::vector<PublicationStateDefn>::iterator pubIt = std::find_if(m_publications.begin(), m_publications.end(),
        [&](PublicationStateDefn &entry)
        {
            return (offendingCommandCorrelationId == entry.m_registrationId);
        });

    if (pubIt != m_publications.end())
    {
        (*pubIt).m_status = RegistrationStatus::ERRORED_MEDIA_DRIVER;
        (*pubIt).m_errorCode = errorCode;
        (*pubIt).m_errorMessage = errorMessage;
        return;
    }
}


void ClientConductor::onNewImage(
    std::int32_t streamId,
    std::int32_t sessionId,
    std::int64_t joiningPosition,
    const std::string &logFilename,
    const std::string &sourceIdentity,
    std::int32_t subscriberPositionCount,
    const ImageBuffersReadyDefn::SubscriberPosition *subscriberPositions,
    std::int64_t correlationId)
{
    std::lock_guard<std::mutex> lock(m_adminLock);

    std::for_each(m_subscriptions.begin(), m_subscriptions.end(),
        [&](SubscriptionStateDefn &entry)
        {
            if (streamId == entry.m_streamId)
            {
                std::shared_ptr<Subscription> subscription = entry.m_subscription.lock();

                if (subscription != nullptr &&
                    !(subscription->hasImage(sessionId)))
                {
                    for (int i = 0; i < subscriberPositionCount; i++)
                    {
                        if (subscription->registrationId() == subscriberPositions[i].registrationId)
                        {
                            std::shared_ptr<LogBuffers> logBuffers = std::make_shared<LogBuffers>(logFilename.c_str());

                            UnsafeBufferPosition subscriberPosition(m_counterValuesBuffer, subscriberPositions[i].indicatorId);

                            Image image(
                                sessionId, joiningPosition, correlationId, subscriberPosition, logBuffers);

                            Image* oldArray = subscription->addImage(image);

                            if (nullptr != oldArray)
                            {
                                lingerResource(m_epochClock(), oldArray);
                            }

                            m_onNewImageHandler(
                                image, subscription->channel(), streamId, sessionId, joiningPosition, sourceIdentity);
                            break;
                        }
                    }
                }
            }
        });
}

void ClientConductor::onInactiveImage(
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
                std::shared_ptr<Subscription> subscription = entry.m_subscription.lock();

                if (nullptr != subscription)
                {
                    std::pair<Image*, int> result = subscription->removeImage(correlationId);
                    Image* oldArray = result.first;
                    const int index = result.second;

                    if (nullptr != oldArray)
                    {
                        lingerResource(now, oldArray[index].logBuffers());
                        lingerResource(now, oldArray);
                        m_onInactiveImageHandler(oldArray[index], subscription->channel(), streamId, sessionId, position);
                    }
                }
            }
        });
}

void ClientConductor::onCheckManagedResources(long now)
{
    std::lock_guard<std::mutex> lock(m_adminLock);

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
    std::vector<ImageArrayLingerDefn>::iterator arrayIt =
        std::remove_if(m_lingeringImageArrays.begin(), m_lingeringImageArrays.end(),
            [&](ImageArrayLingerDefn & entry)
            {
                if (now > (entry.m_timeOfLastStatusChange + m_resourceLingerTimeoutMs))
                {
                    delete[] entry.m_array;
                    entry.m_array = nullptr;
                    return true;
                }

                return false;
            });

    m_lingeringImageArrays.erase(arrayIt, m_lingeringImageArrays.end());
}

void ClientConductor::lingerResource(long now, Image* array)
{
    m_lingeringImageArrays.push_back(ImageArrayLingerDefn(now, array));
}

void ClientConductor::lingerResource(long now, std::shared_ptr<LogBuffers> logBuffers)
{
    m_lingeringLogBuffers.push_back(LogBuffersLingerDefn(now, logBuffers));
}

void ClientConductor::lingerResources(long now, Image* image, int connectionsLength)
{
    for (int i = 0; i < connectionsLength; i++)
    {
        lingerResource(now, image[i].logBuffers());
    }

    if (nullptr != image)
    {
        lingerResource(now, image);
    }
}

}