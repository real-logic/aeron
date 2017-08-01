/*
 * Copyright 2014-2017 Real Logic Ltd.
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
        [&subscriptions](SubscriptionStateDefn& entry)
        {
            subscriptions.push_back(entry.m_subscriptionCache);
            entry.m_subscriptionCache.reset();
        });

    std::for_each(m_lingeringImageLists.begin(), m_lingeringImageLists.end(),
        [](ImageListLingerDefn & entry)
        {
            delete[] entry.m_imageList->m_images;
            delete entry.m_imageList;
            entry.m_imageList = nullptr;
        });
}

std::int64_t ClientConductor::addPublication(const std::string &channel, std::int32_t streamId)
{
    verifyDriverIsActive();

    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    std::int64_t id;

    auto it = std::find_if(m_publications.begin(), m_publications.end(),
        [channel, streamId](const PublicationStateDefn &entry)
        {
            return (streamId == entry.m_streamId && channel == entry.m_channel);
        });

    if (it == m_publications.end())
    {
        std::int64_t registrationId = m_driverProxy.addPublication(channel, streamId);

        m_publications.emplace_back(channel, registrationId, streamId, m_epochClock());
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
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);

    auto it = std::find_if(m_publications.begin(), m_publications.end(),
        [registrationId](const PublicationStateDefn &entry)
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

                    pub = std::make_shared<Publication>(
                        *this, state.m_channel, state.m_registrationId, state.m_originalRegistrationId, state.m_streamId,
                        state.m_sessionId, publicationLimit, state.m_buffers);

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
    verifyDriverIsActiveViaErrorHandler();

    std::lock_guard<std::recursive_mutex> lock(m_adminLock);

    auto it = std::find_if(m_publications.begin(), m_publications.end(),
        [registrationId](const PublicationStateDefn &entry)
        {
            return (registrationId == entry.m_registrationId);
        });

    if (it != m_publications.end())
    {
        m_driverProxy.removePublication(registrationId);
        m_publications.erase(it);
    }
}

std::int64_t ClientConductor::addExclusivePublication(const std::string& channel, std::int32_t streamId)
{
    verifyDriverIsActive();

    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    std::int64_t registrationId = m_driverProxy.addExclusivePublication(channel, streamId);

    m_exclusivePublications.emplace_back(channel, registrationId, streamId, m_epochClock());

    return registrationId;
}

std::shared_ptr<ExclusivePublication> ClientConductor::findExclusivePublication(std::int64_t registrationId)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);

    auto it = std::find_if(m_exclusivePublications.begin(), m_exclusivePublications.end(),
        [registrationId](const ExclusivePublicationStateDefn &entry)
        {
            return (registrationId == entry.m_registrationId);
        });

    if (it == m_exclusivePublications.end())
    {
        return std::shared_ptr<ExclusivePublication>();
    }

    ExclusivePublicationStateDefn& state = (*it);
    std::shared_ptr<ExclusivePublication> pub(state.m_publication.lock());

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

                pub = std::make_shared<ExclusivePublication>(
                    *this, state.m_channel, state.m_registrationId, state.m_originalRegistrationId, state.m_streamId,
                    state.m_sessionId, publicationLimit, state.m_buffers);

                state.m_publication = std::weak_ptr<ExclusivePublication>(pub);
                break;
            }

            case RegistrationStatus::ERRORED_MEDIA_DRIVER:
                throw RegistrationException(state.m_errorCode, state.m_errorMessage, SOURCEINFO);
        }
    }

    return pub;
}

void ClientConductor::releaseExclusivePublication(std::int64_t registrationId)
{
    verifyDriverIsActiveViaErrorHandler();

    std::lock_guard<std::recursive_mutex> lock(m_adminLock);

    auto it = std::find_if(m_exclusivePublications.begin(), m_exclusivePublications.end(),
        [registrationId](const ExclusivePublicationStateDefn &entry)
        {
            return (registrationId == entry.m_registrationId);
        });

    if (it != m_exclusivePublications.end())
    {
        m_driverProxy.removePublication(registrationId);
        m_exclusivePublications.erase(it);
    }
}

std::int64_t ClientConductor::addSubscription(
    const std::string &channel,
    std::int32_t streamId,
    const on_available_image_t &onAvailableImageHandler,
    const on_unavailable_image_t &onUnavailableImageHandler)
{
    verifyDriverIsActive();

    std::lock_guard<std::recursive_mutex> lock(m_adminLock);

    std::int64_t registrationId = m_driverProxy.addSubscription(channel, streamId);

    m_subscriptions.emplace_back(
        channel, registrationId, streamId, m_epochClock(), onAvailableImageHandler, onUnavailableImageHandler);

    return registrationId;
}

std::shared_ptr<Subscription> ClientConductor::findSubscription(std::int64_t registrationId)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);

    auto it = std::find_if(m_subscriptions.begin(), m_subscriptions.end(),
        [registrationId](const SubscriptionStateDefn &entry)
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

void ClientConductor::releaseSubscription(std::int64_t registrationId, struct ImageList *imageList)
{
    verifyDriverIsActiveViaErrorHandler();

    std::lock_guard<std::recursive_mutex> lock(m_adminLock);

    auto it = std::find_if(m_subscriptions.begin(), m_subscriptions.end(),
        [registrationId](const SubscriptionStateDefn &entry)
        {
            return (registrationId == entry.m_registrationId);
        });

    if (it != m_subscriptions.end())
    {
        m_driverProxy.removeSubscription((*it).m_registrationId);

        for (std::size_t i = 0; i < imageList->m_length; i++)
        {
            (*it).m_onUnavailableImageHandler(imageList->m_images[i]);
        }

        m_subscriptions.erase(it);

        lingerAllResources(m_epochClock(), imageList);
    }
    else if (nullptr != imageList)
    {
        delete[] imageList->m_images;
        delete imageList;
    }
}

void ClientConductor::addDestination(std::int64_t publicationRegistrationId, const std::string& endpointChannel)
{
    verifyDriverIsActive();

    m_driverProxy.addDestination(publicationRegistrationId, endpointChannel);
}

void ClientConductor::removeDestination(std::int64_t publicationRegistrationId, const std::string& endpointChannel)
{
    verifyDriverIsActive();

    m_driverProxy.removeDestination(publicationRegistrationId, endpointChannel);
}

void ClientConductor::onNewPublication(
    std::int32_t streamId,
    std::int32_t sessionId,
    std::int32_t positionLimitCounterId,
    const std::string &logFileName,
    std::int64_t registrationId,
    std::int64_t originalRegistrationId)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);

    auto it = std::find_if(m_publications.begin(), m_publications.end(),
        [registrationId](const PublicationStateDefn &entry)
        {
            return (registrationId == entry.m_registrationId);
        });

    if (it != m_publications.end())
    {
        PublicationStateDefn& state = (*it);

        state.m_status = RegistrationStatus::REGISTERED_MEDIA_DRIVER;
        state.m_sessionId = sessionId;
        state.m_positionLimitCounterId = positionLimitCounterId;
        state.m_buffers = std::make_shared<LogBuffers>(logFileName.c_str());
        state.m_originalRegistrationId = originalRegistrationId;

        m_onNewPublicationHandler(state.m_channel, streamId, sessionId, registrationId);
    }
}

void ClientConductor::onNewExclusivePublication(
    std::int32_t streamId,
    std::int32_t sessionId,
    std::int32_t positionLimitCounterId,
    const std::string &logFileName,
    std::int64_t registrationId,
    std::int64_t originalRegistrationId)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);

    auto it = std::find_if(m_exclusivePublications.begin(), m_exclusivePublications.end(),
        [registrationId](const ExclusivePublicationStateDefn &entry)
        {
            return (registrationId == entry.m_registrationId);
        });

    if (it != m_exclusivePublications.end())
    {
        ExclusivePublicationStateDefn& state = (*it);

        state.m_status = RegistrationStatus::REGISTERED_MEDIA_DRIVER;
        state.m_sessionId = sessionId;
        state.m_positionLimitCounterId = positionLimitCounterId;
        state.m_buffers = std::make_shared<LogBuffers>(logFileName.c_str());
        state.m_originalRegistrationId = originalRegistrationId;

        m_onNewPublicationHandler(state.m_channel, streamId, sessionId, registrationId);
    }
}

void ClientConductor::onOperationSuccess(std::int64_t correlationId)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);

    auto subIt = std::find_if(m_subscriptions.begin(), m_subscriptions.end(),
        [correlationId](const SubscriptionStateDefn &entry)
        {
            return (correlationId == entry.m_registrationId);
        });

    if (subIt != m_subscriptions.end() && (*subIt).m_status == RegistrationStatus::AWAITING_MEDIA_DRIVER)
    {
        SubscriptionStateDefn& state = (*subIt);

        state.m_status = RegistrationStatus::REGISTERED_MEDIA_DRIVER;
        state.m_subscriptionCache =
            std::make_shared<Subscription>(*this, state.m_registrationId, state.m_channel, state.m_streamId);
        state.m_subscription = std::weak_ptr<Subscription>(state.m_subscriptionCache);
        m_onNewSubscriptionHandler(state.m_channel, state.m_streamId, correlationId);
        return;
    }
}

void ClientConductor::onErrorResponse(
    std::int64_t offendingCommandCorrelationId,
    std::int32_t errorCode,
    const std::string& errorMessage)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);

    auto subIt = std::find_if(m_subscriptions.begin(), m_subscriptions.end(),
        [offendingCommandCorrelationId](const SubscriptionStateDefn &entry)
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

    auto pubIt = std::find_if(m_publications.begin(), m_publications.end(),
        [offendingCommandCorrelationId](const PublicationStateDefn &entry)
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

    auto exPubIt = std::find_if(m_exclusivePublications.begin(), m_exclusivePublications.end(),
        [offendingCommandCorrelationId](const ExclusivePublicationStateDefn &entry)
        {
            return (offendingCommandCorrelationId == entry.m_registrationId);
        });

    if (exPubIt != m_exclusivePublications.end())
    {
        (*exPubIt).m_status = RegistrationStatus::ERRORED_MEDIA_DRIVER;
        (*exPubIt).m_errorCode = errorCode;
        (*exPubIt).m_errorMessage = errorMessage;
        return;
    }
}


void ClientConductor::onAvailableImage(
    std::int32_t streamId,
    std::int32_t sessionId,
    const std::string &logFilename,
    const std::string &sourceIdentity,
    std::int32_t subscriberPositionIndicatorId,
    std::int64_t subscriberPositionRegistrationId,
    std::int64_t correlationId)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);

    std::for_each(m_subscriptions.begin(), m_subscriptions.end(),
        [&](const SubscriptionStateDefn &entry)
        {
            if (streamId == entry.m_streamId)
            {
                std::shared_ptr<Subscription> subscription = entry.m_subscription.lock();

                if (subscription != nullptr &&
                    !(subscription->hasImage(correlationId)) &&
                    subscriberPositionRegistrationId == subscription->registrationId())
                {
                    std::shared_ptr<LogBuffers> logBuffers = std::make_shared<LogBuffers>(logFilename.c_str());

                    UnsafeBufferPosition subscriberPosition(m_counterValuesBuffer, subscriberPositionIndicatorId);

                    Image image(
                        sessionId,
                        correlationId,
                        subscription->registrationId(),
                        sourceIdentity,
                        subscriberPosition,
                        logBuffers,
                        m_errorHandler);

                    entry.m_onAvailableImageHandler(image);

                    struct ImageList *oldImageList = subscription->addImage(image);

                    if (nullptr != oldImageList)
                    {
                        lingerResource(m_epochClock(), oldImageList);
                    }
                }
            }
        });
}

void ClientConductor::onUnavailableImage(
    std::int32_t streamId,
    std::int64_t correlationId)
{
    const long long now = m_epochClock();
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);

    std::for_each(m_subscriptions.begin(), m_subscriptions.end(),
        [&](const SubscriptionStateDefn &entry)
        {
            if (streamId == entry.m_streamId)
            {
                std::shared_ptr<Subscription> subscription = entry.m_subscription.lock();

                if (nullptr != subscription)
                {
                    std::pair<struct ImageList *,int> result = subscription->removeImage(correlationId);
                    struct ImageList *oldImageList = result.first;
                    const int index = result.second;

                    if (nullptr != oldImageList)
                    {
                        Image* oldArray = oldImageList->m_images;

                        lingerResource(now, oldArray[index].logBuffers());
                        lingerResource(now, oldImageList);
                        entry.m_onUnavailableImageHandler(oldArray[index]);
                    }
                }
            }
        });
}

void ClientConductor::onInterServiceTimeout(long long now)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);

    std::for_each(m_publications.begin(), m_publications.end(),
        [&](PublicationStateDefn& entry)
        {
            std::shared_ptr<Publication> pub = entry.m_publication.lock();

            if (nullptr != pub)
            {
                pub->close();
            }
        });

    m_publications.clear();

    std::for_each(m_exclusivePublications.begin(), m_exclusivePublications.end(),
        [&](ExclusivePublicationStateDefn& entry)
        {
            std::shared_ptr<ExclusivePublication> pub = entry.m_publication.lock();

            if (nullptr != pub)
            {
                pub->close();
            }
        });

    m_exclusivePublications.clear();

    std::for_each(m_subscriptions.begin(), m_subscriptions.end(),
        [&](SubscriptionStateDefn& entry)
        {
            std::shared_ptr<Subscription> sub = entry.m_subscription.lock();

            if (nullptr != sub)
            {
                lingerAllResources(now, sub->removeAndCloseAllImages());
            }
        });

    m_subscriptions.clear();
}

void ClientConductor::onCheckManagedResources(long long now)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);

    // erase-remove idiom

    // check LogBuffers
    auto logIt = std::remove_if(m_lingeringLogBuffers.begin(), m_lingeringLogBuffers.end(),
        [now, this](const LogBuffersLingerDefn& entry)
        {
            return (now > (entry.m_timeOfLastStatusChange + m_resourceLingerTimeoutMs));
        });

    m_lingeringLogBuffers.erase(logIt, m_lingeringLogBuffers.end());

    // check old arrays
    auto arrayIt = std::remove_if(m_lingeringImageLists.begin(), m_lingeringImageLists.end(),
        [now, this](ImageListLingerDefn & entry)
        {
            if (now > (entry.m_timeOfLastStatusChange + m_resourceLingerTimeoutMs))
            {
                delete[] entry.m_imageList->m_images;
                delete entry.m_imageList;
                entry.m_imageList = nullptr;
                return true;
            }

            return false;
        });

    m_lingeringImageLists.erase(arrayIt, m_lingeringImageLists.end());
}

void ClientConductor::lingerResource(long long now, struct ImageList *imageList)
{
    m_lingeringImageLists.emplace_back(now, imageList);
}

void ClientConductor::lingerResource(long long now, std::shared_ptr<LogBuffers> logBuffers)
{
    m_lingeringLogBuffers.emplace_back(now, logBuffers);
}

void ClientConductor::lingerAllResources(long long now, struct ImageList *imageList)
{
    if (nullptr != imageList)
    {
        for (std::size_t i = 0; i < imageList->m_length; i++)
        {
            lingerResource(now, imageList->m_images[i].logBuffers());
        }

        lingerResource(now, imageList);
    }
}

}
