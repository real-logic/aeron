/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ClientConductor.h"

#include <cassert>

namespace aeron
{

template<typename T, typename... U>
static std::size_t getAddress(const std::function<T(U...)> &f)
{
    typedef T(fnType)(U...);
    auto fnPointer = f.template target<fnType *>();

    return nullptr != fnPointer ? reinterpret_cast<std::size_t>(*fnPointer) : 0;
}

static ExceptionCategory getCategory(std::int32_t errorCode)
{
    return errorCode == ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE ?
        ExceptionCategory::EXCEPTION_CATEGORY_WARN : ExceptionCategory::EXCEPTION_CATEGORY_ERROR;
}

ClientConductor::~ClientConductor()
{
    std::for_each(m_lingeringImageLists.begin(), m_lingeringImageLists.end(),
        [](ImageListLingerDefn &entry)
        {
            delete[] entry.m_imageArray;
            entry.m_imageArray = nullptr;
        });

    m_driverProxy.clientClose();
}

void ClientConductor::onStart()
{
}

int ClientConductor::doWork()
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    int workCount = 0;

    workCount += m_driverListenerAdapter.receiveMessages();
    workCount += onHeartbeatCheckTimeouts();

    return workCount;
}

void ClientConductor::onClose()
{
    if (!m_isClosed)
    {
        std::lock_guard<std::recursive_mutex> lock(m_adminLock);
        closeAllResources(m_epochClock());
    }
}

std::int64_t ClientConductor::addPublication(const std::string &channel, std::int32_t streamId)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    verifyDriverIsActive();
    ensureNotReentrant();
    ensureOpen();

    std::int64_t registrationId = m_driverProxy.addPublication(channel, streamId);

    m_publicationByRegistrationId.insert(std::pair<std::int64_t, PublicationStateDefn>(
        registrationId,
        PublicationStateDefn(channel, registrationId, streamId, m_epochClock())));

    return registrationId;
}

std::shared_ptr<Publication> ClientConductor::findPublication(std::int64_t registrationId)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    ensureNotReentrant();
    ensureOpen();

    auto it = m_publicationByRegistrationId.find(registrationId);
    if (it == m_publicationByRegistrationId.end())
    {
        return {};
    }

    PublicationStateDefn &state = it->second;
    std::shared_ptr<Publication> pub(state.m_publication.lock());

    if (!pub)
    {
        switch (state.m_status)
        {
            case RegistrationStatus::AWAITING_MEDIA_DRIVER:
                if (m_epochClock() > (state.m_timeOfRegistrationMs + m_driverTimeoutMs))
                {
                    throw DriverTimeoutException(
                        "no response from driver in " + std::to_string(m_driverTimeoutMs) + " ms", SOURCEINFO);
                }
                break;

            case RegistrationStatus::REGISTERED_MEDIA_DRIVER:
            {
                UnsafeBufferPosition publicationLimit(m_counterValuesBuffer, state.m_publicationLimitCounterId);

                pub = std::make_shared<Publication>(
                    *this,
                    state.m_channel,
                    state.m_registrationId,
                    state.m_originalRegistrationId,
                    state.m_streamId,
                    state.m_sessionId,
                    publicationLimit,
                    state.m_channelStatusId,
                    state.m_buffers);

                state.m_publication = std::weak_ptr<Publication>(pub);
                break;
            }

            case RegistrationStatus::ERRORED_MEDIA_DRIVER:
            {
                const std::int32_t errorCode = state.m_errorCode;
                const std::string errorMessage = state.m_errorMessage;

                m_publicationByRegistrationId.erase(it);

                throw RegistrationException(errorCode, getCategory(errorCode), errorMessage, SOURCEINFO);
            }
        }
    }

    return pub;
}

void ClientConductor::releasePublication(std::int64_t registrationId)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    verifyDriverIsActiveViaErrorHandler();

    auto it = m_publicationByRegistrationId.find(registrationId);
    if (it != m_publicationByRegistrationId.end())
    {
        m_driverProxy.removePublication(registrationId);
        m_publicationByRegistrationId.erase(it);
    }
}

std::int64_t ClientConductor::addExclusivePublication(const std::string &channel, std::int32_t streamId)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    verifyDriverIsActive();
    ensureNotReentrant();
    ensureOpen();

    std::int64_t registrationId = m_driverProxy.addExclusivePublication(channel, streamId);

    m_exclusivePublicationByRegistrationId.insert(std::pair<std::int64_t, ExclusivePublicationStateDefn>(
        registrationId,
        ExclusivePublicationStateDefn(channel, registrationId, streamId, m_epochClock())));

    return registrationId;
}

std::shared_ptr<ExclusivePublication> ClientConductor::findExclusivePublication(std::int64_t registrationId)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    ensureNotReentrant();
    ensureOpen();

    auto it = m_exclusivePublicationByRegistrationId.find(registrationId);
    if (it == m_exclusivePublicationByRegistrationId.end())
    {
        return {};
    }

    ExclusivePublicationStateDefn &state = it->second;
    std::shared_ptr<ExclusivePublication> pub(state.m_publication.lock());

    if (!pub)
    {
        switch (state.m_status)
        {
            case RegistrationStatus::AWAITING_MEDIA_DRIVER:
                if (m_epochClock() > (state.m_timeOfRegistrationMs + m_driverTimeoutMs))
                {
                    throw DriverTimeoutException(
                        "no response from driver in " + std::to_string(m_driverTimeoutMs) + " ms", SOURCEINFO);
                }
                break;

            case RegistrationStatus::REGISTERED_MEDIA_DRIVER:
            {
                UnsafeBufferPosition publicationLimit(m_counterValuesBuffer, state.m_publicationLimitCounterId);

                pub = std::make_shared<ExclusivePublication>(
                    *this,
                    state.m_channel,
                    state.m_registrationId,
                    state.m_streamId,
                    state.m_sessionId,
                    publicationLimit,
                    state.m_channelStatusId,
                    state.m_buffers);

                state.m_publication = std::weak_ptr<ExclusivePublication>(pub);
                break;
            }

            case RegistrationStatus::ERRORED_MEDIA_DRIVER:
            {
                const std::int32_t errorCode = state.m_errorCode;
                const std::string errorMessage = state.m_errorMessage;

                m_exclusivePublicationByRegistrationId.erase(it);

                throw RegistrationException(errorCode, getCategory(errorCode), errorMessage, SOURCEINFO);
            }
        }
    }

    return pub;
}

void ClientConductor::releaseExclusivePublication(std::int64_t registrationId)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    verifyDriverIsActiveViaErrorHandler();

    auto it = m_exclusivePublicationByRegistrationId.find(registrationId);
    if (it != m_exclusivePublicationByRegistrationId.end())
    {
        m_driverProxy.removePublication(registrationId);
        m_exclusivePublicationByRegistrationId.erase(it);
    }
}

std::int64_t ClientConductor::addSubscription(
    const std::string &channel,
    std::int32_t streamId,
    const on_available_image_t &onAvailableImageHandler,
    const on_unavailable_image_t &onUnavailableImageHandler)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    verifyDriverIsActive();
    ensureNotReentrant();
    ensureOpen();

    std::int64_t registrationId = m_driverProxy.addSubscription(channel, streamId);

    m_subscriptionByRegistrationId.insert(std::pair<std::int64_t, SubscriptionStateDefn>(
        registrationId,
        SubscriptionStateDefn(
            channel, registrationId, streamId, m_epochClock(), onAvailableImageHandler, onUnavailableImageHandler)));

    return registrationId;
}

std::shared_ptr<Subscription> ClientConductor::findSubscription(std::int64_t registrationId)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    ensureNotReentrant();
    ensureOpen();

    auto it = m_subscriptionByRegistrationId.find(registrationId);
    if (it == m_subscriptionByRegistrationId.end())
    {
        return {};
    }

    SubscriptionStateDefn &state = it->second;
    std::shared_ptr<Subscription> sub = state.m_subscription.lock();

    if (state.m_subscriptionCache)
    {
        state.m_subscriptionCache.reset();
    }

    if (!sub && RegistrationStatus::AWAITING_MEDIA_DRIVER == state.m_status)
    {
        if (m_epochClock() > (state.m_timeOfRegistrationMs + m_driverTimeoutMs))
        {
            throw DriverTimeoutException(
                "no response from driver in " + std::to_string(m_driverTimeoutMs) + " ms", SOURCEINFO);
        }
    }
    else if (!sub && RegistrationStatus::ERRORED_MEDIA_DRIVER == state.m_status)
    {
        const std::int32_t errorCode = state.m_errorCode;
        const std::string errorMessage = state.m_errorMessage;

        m_subscriptionByRegistrationId.erase(it);

        throw RegistrationException(errorCode, getCategory(errorCode), errorMessage, SOURCEINFO);
    }

    return sub;
}

void ClientConductor::releaseSubscription(std::int64_t registrationId, Image::array_t imageArray, std::size_t length)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    verifyDriverIsActiveViaErrorHandler();

    auto it = m_subscriptionByRegistrationId.find(registrationId);
    if (it != m_subscriptionByRegistrationId.end())
    {
        m_driverProxy.removeSubscription(registrationId);
        lingerAllResources(m_epochClock(), imageArray);

        for (std::size_t i = 0; i < length; i++)
        {
            auto image = *imageArray[i];
            image.close();

            CallbackGuard callbackGuard(m_isInCallback);
            it->second.m_onUnavailableImageHandler(image);
        }

        m_subscriptionByRegistrationId.erase(it);
    }
    else
    {
        delete[] imageArray;
    }
}

std::int64_t ClientConductor::addCounter(
    std::int32_t typeId, const std::uint8_t *keyBuffer, std::size_t keyLength, const std::string &label)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    verifyDriverIsActive();
    ensureNotReentrant();
    ensureOpen();

    if (keyLength > CountersManager::MAX_KEY_LENGTH)
    {
        throw IllegalArgumentException("key length out of bounds: " + std::to_string(keyLength), SOURCEINFO);
    }

    if (label.length() > CountersManager::MAX_LABEL_LENGTH)
    {
        throw IllegalArgumentException("label length out of bounds: " + std::to_string(label.length()), SOURCEINFO);
    }

    std::int64_t registrationId = m_driverProxy.addCounter(typeId, keyBuffer, keyLength, label);

    m_counterByRegistrationId.insert(std::pair<std::int64_t, CounterStateDefn>(
        registrationId, CounterStateDefn(registrationId, m_epochClock())));

    return registrationId;
}

std::shared_ptr<Counter> ClientConductor::findCounter(std::int64_t registrationId)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    ensureNotReentrant();
    ensureOpen();

    auto it = m_counterByRegistrationId.find(registrationId);
    if (it == m_counterByRegistrationId.end())
    {
        return {};
    }

    CounterStateDefn &state = it->second;
    std::shared_ptr<Counter> counter = state.m_counter.lock();

    if (state.m_counterCache)
    {
        state.m_counterCache.reset();
    }

    if (!counter && RegistrationStatus::AWAITING_MEDIA_DRIVER == state.m_status)
    {
        if (m_epochClock() > (state.m_timeOfRegistrationMs + m_driverTimeoutMs))
        {
            throw DriverTimeoutException(
                "no response from driver in " + std::to_string(m_driverTimeoutMs) + " ms", SOURCEINFO);
        }
    }
    else if (!counter && RegistrationStatus::ERRORED_MEDIA_DRIVER == state.m_status)
    {
        const std::int32_t errorCode = state.m_errorCode;
        const std::string errorMessage = state.m_errorMessage;

        m_counterByRegistrationId.erase(it);

        throw RegistrationException(errorCode, getCategory(errorCode), errorMessage, SOURCEINFO);
    }

    return counter;
}

void ClientConductor::releaseCounter(std::int64_t registrationId)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    verifyDriverIsActiveViaErrorHandler();

    auto it = m_counterByRegistrationId.find(registrationId);
    if (it != m_counterByRegistrationId.end())
    {
        m_driverProxy.removeCounter(registrationId);

        m_counterByRegistrationId.erase(it);
    }
}

std::int64_t ClientConductor::addDestination(
    std::int64_t publicationRegistrationId, const std::string &endpointChannel)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    verifyDriverIsActive();
    ensureNotReentrant();
    ensureOpen();

    std::int64_t correlationId = m_driverProxy.addDestination(publicationRegistrationId, endpointChannel);

    m_destinationStateByCorrelationId.insert(std::pair<std::int64_t, DestinationStateDefn>(
        correlationId,
        DestinationStateDefn(correlationId, publicationRegistrationId, m_epochClock())));

    return correlationId;
}

std::int64_t ClientConductor::removeDestination(
    std::int64_t publicationRegistrationId, const std::string &endpointChannel)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    verifyDriverIsActive();
    ensureNotReentrant();
    ensureOpen();

    std::int64_t correlationId = m_driverProxy.removeDestination(publicationRegistrationId, endpointChannel);

    m_destinationStateByCorrelationId.insert(std::pair<std::int64_t, DestinationStateDefn>(
        correlationId,
        DestinationStateDefn(correlationId, publicationRegistrationId, m_epochClock())));

    return correlationId;
}

std::int64_t ClientConductor::addRcvDestination(
    std::int64_t subscriptionRegistrationId, const std::string &endpointChannel)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    verifyDriverIsActive();
    ensureNotReentrant();
    ensureOpen();

    std::int64_t correlationId = m_driverProxy.addRcvDestination(subscriptionRegistrationId, endpointChannel);

    m_destinationStateByCorrelationId.insert(std::pair<std::int64_t, DestinationStateDefn>(
        correlationId,
        DestinationStateDefn(correlationId, subscriptionRegistrationId, m_epochClock())));

    return correlationId;
}

std::int64_t ClientConductor::removeRcvDestination(
    std::int64_t subscriptionRegistrationId, const std::string &endpointChannel)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    verifyDriverIsActive();
    ensureNotReentrant();
    ensureOpen();

    std::int64_t correlationId = m_driverProxy.removeRcvDestination(subscriptionRegistrationId, endpointChannel);

    m_destinationStateByCorrelationId.insert(std::pair<std::int64_t, DestinationStateDefn>(
        correlationId,
        DestinationStateDefn(correlationId, subscriptionRegistrationId, m_epochClock())));

    return correlationId;
}

bool ClientConductor::findDestinationResponse(std::int64_t correlationId)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    ensureNotReentrant();
    ensureOpen();

    auto it = m_destinationStateByCorrelationId.find(correlationId);
    if (it == m_destinationStateByCorrelationId.end())
    {
        throw IllegalArgumentException("correlationId unknown", SOURCEINFO);
    }

    DestinationStateDefn &state = it->second;
    bool result = false;

    switch (state.m_status)
    {
        case RegistrationStatus::AWAITING_MEDIA_DRIVER:
        {
            if (m_epochClock() > (state.m_timeOfRegistrationMs + m_driverTimeoutMs))
            {
                m_destinationStateByCorrelationId.erase(it);
                throw DriverTimeoutException(
                    "no response from driver in " + std::to_string(m_driverTimeoutMs) + " ms", SOURCEINFO);
            }
            break;
        }

        case RegistrationStatus::REGISTERED_MEDIA_DRIVER:
        {
            m_destinationStateByCorrelationId.erase(it);
            result = true;
            break;
        }

        case RegistrationStatus::ERRORED_MEDIA_DRIVER:
        {
            const std::int32_t errorCode = state.m_errorCode;
            const std::string errorMessage = state.m_errorMessage;

            m_destinationStateByCorrelationId.erase(it);

            throw RegistrationException(errorCode, getCategory(errorCode), errorMessage, SOURCEINFO);
        }
    }

    return result;
}

std::int64_t ClientConductor::addAvailableCounterHandler(const on_available_counter_t &handler)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    ensureNotReentrant();
    ensureOpen();

    const std::int64_t registrationId = m_driverProxy.nextCorrelationId();
    m_onAvailableCounterHandlers.emplace_back(registrationId, handler);

    return registrationId;
}

void ClientConductor::removeAvailableCounterHandler(const on_available_counter_t &handler)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    ensureNotReentrant();
    ensureOpen();

    auto &v = m_onAvailableCounterHandlers;
    auto predicate =
        [handler](const std::pair<std::int64_t, on_available_counter_t> &item)
        {
            std::size_t itemAddress = getAddress(item.second);
            std::size_t handlerAddress = getAddress(handler);
            return itemAddress != 0 && itemAddress == handlerAddress;
        };

    v.erase(std::remove_if(v.begin(), v.end(), predicate), v.end());
}

void ClientConductor::removeAvailableCounterHandler(std::int64_t registrationId)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    ensureNotReentrant();
    ensureOpen();

    auto &v = m_onAvailableCounterHandlers;
    auto predicate =
        [registrationId](const std::pair<std::int64_t, on_available_counter_t> &item)
        {
            return item.first == registrationId;
        };

    v.erase(std::remove_if(v.begin(), v.end(), predicate), v.end());
}

std::int64_t ClientConductor::addUnavailableCounterHandler(const on_unavailable_counter_t &handler)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    ensureNotReentrant();
    ensureOpen();

    std::int64_t registrationId = m_driverProxy.nextCorrelationId();
    m_onUnavailableCounterHandlers.emplace_back(registrationId, handler);

    return registrationId;
}

void ClientConductor::removeUnavailableCounterHandler(const on_unavailable_counter_t &handler)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    ensureNotReentrant();
    ensureOpen();

    auto &v = m_onUnavailableCounterHandlers;
    auto predicate =
        [handler](const std::pair<std::int64_t, on_unavailable_counter_t> &item)
        {
            std::size_t itemAddress = getAddress(item.second);
            std::size_t handlerAddress = getAddress(handler);
            return itemAddress != 0 && itemAddress == handlerAddress;
        };

    v.erase(std::remove_if(v.begin(), v.end(), predicate), v.end());
}

void ClientConductor::removeUnavailableCounterHandler(std::int64_t registrationId)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    ensureNotReentrant();
    ensureOpen();

    auto &v = m_onUnavailableCounterHandlers;
    auto predicate =
        [registrationId](const std::pair<std::int64_t, on_unavailable_counter_t> &item)
        {
            return item.first == registrationId;
        };

    v.erase(std::remove_if(v.begin(), v.end(), predicate), v.end());
}

std::int64_t ClientConductor::addCloseClientHandler(const on_close_client_t &handler)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    ensureNotReentrant();
    ensureOpen();

    std::int64_t registrationId = m_driverProxy.nextCorrelationId();
    m_onCloseClientHandlers.emplace_back(registrationId, handler);

    return registrationId;
}

void ClientConductor::removeCloseClientHandler(const on_close_client_t &handler)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    ensureNotReentrant();
    ensureOpen();

    auto &v = m_onCloseClientHandlers;
    auto predicate =
        [handler](const std::pair<std::int64_t, on_close_client_t> &item)
        {
            std::size_t itemAddress = getAddress(item.second);
            std::size_t handlerAddress = getAddress(handler);
            return itemAddress != 0 && itemAddress == handlerAddress;
        };

    v.erase(std::remove_if(v.begin(), v.end(), predicate), v.end());
}

void ClientConductor::removeCloseClientHandler(std::int64_t registrationId)
{
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    ensureNotReentrant();
    ensureOpen();

    auto &v = m_onCloseClientHandlers;
    auto predicate =
        [registrationId](const std::pair<std::int64_t, on_close_client_t> &item)
        {
            return item.first == registrationId;
        };

    v.erase(std::remove_if(v.begin(), v.end(), predicate), v.end());
}

void ClientConductor::onNewPublication(
    std::int64_t registrationId,
    std::int64_t originalRegistrationId,
    std::int32_t streamId,
    std::int32_t sessionId,
    std::int32_t publicationLimitCounterId,
    std::int32_t channelStatusIndicatorId,
    const std::string &logFileName)
{
    auto it = m_publicationByRegistrationId.find(registrationId);
    if (it != m_publicationByRegistrationId.end())
    {
        PublicationStateDefn &state = it->second;

        state.m_status = RegistrationStatus::REGISTERED_MEDIA_DRIVER;
        state.m_sessionId = sessionId;
        state.m_publicationLimitCounterId = publicationLimitCounterId;
        state.m_channelStatusId = channelStatusIndicatorId;
        state.m_buffers = getLogBuffers(originalRegistrationId, logFileName, state.m_channel);
        state.m_originalRegistrationId = originalRegistrationId;

        CallbackGuard callbackGuard(m_isInCallback);
        m_onNewPublicationHandler(state.m_channel, streamId, sessionId, registrationId);
    }
}

void ClientConductor::onNewExclusivePublication(
    std::int64_t registrationId,
    std::int64_t originalRegistrationId,
    std::int32_t streamId,
    std::int32_t sessionId,
    std::int32_t publicationLimitCounterId,
    std::int32_t channelStatusIndicatorId,
    const std::string &logFileName)
{
    assert(registrationId == originalRegistrationId);

    auto it = m_exclusivePublicationByRegistrationId.find(registrationId);
    if (it != m_exclusivePublicationByRegistrationId.end())
    {
        ExclusivePublicationStateDefn &state = it->second;

        state.m_status = RegistrationStatus::REGISTERED_MEDIA_DRIVER;
        state.m_sessionId = sessionId;
        state.m_publicationLimitCounterId = publicationLimitCounterId;
        state.m_channelStatusId = channelStatusIndicatorId;
        state.m_buffers = getLogBuffers(originalRegistrationId, logFileName, state.m_channel);

        CallbackGuard callbackGuard(m_isInCallback);
        m_onNewExclusivePublicationHandler(state.m_channel, streamId, sessionId, registrationId);
    }
}

void ClientConductor::onSubscriptionReady(std::int64_t registrationId, std::int32_t channelStatusId)
{
    auto it = m_subscriptionByRegistrationId.find(registrationId);
    if (it != m_subscriptionByRegistrationId.end() && it->second.m_status == RegistrationStatus::AWAITING_MEDIA_DRIVER)
    {
        SubscriptionStateDefn &state = it->second;

        state.m_status = RegistrationStatus::REGISTERED_MEDIA_DRIVER;
        state.m_subscriptionCache = std::make_shared<Subscription>(
            *this, state.m_registrationId, state.m_channel, state.m_streamId, channelStatusId);
        state.m_subscription = std::weak_ptr<Subscription>(state.m_subscriptionCache);

        CallbackGuard callbackGuard(m_isInCallback);
        m_onNewSubscriptionHandler(state.m_channel, state.m_streamId, registrationId);
    }
}

void ClientConductor::onAvailableCounter(std::int64_t registrationId, std::int32_t counterId)
{
    auto it = m_counterByRegistrationId.find(registrationId);
    if (it != m_counterByRegistrationId.end() && it->second.m_status == RegistrationStatus::AWAITING_MEDIA_DRIVER)
    {
        CounterStateDefn &state = it->second;

        state.m_status = RegistrationStatus::REGISTERED_MEDIA_DRIVER;
        state.m_counterId = counterId;
        state.m_counterCache = std::make_shared<Counter>(
            this, m_counterValuesBuffer, state.m_registrationId, counterId);
        state.m_counter = std::weak_ptr<Counter>(state.m_counterCache);
    }

    for (auto const &handler: m_onAvailableCounterHandlers)
    {
        CallbackGuard callbackGuard(m_isInCallback);
        handler.second(m_countersReader, registrationId, counterId);
    }
}

void ClientConductor::onUnavailableCounter(std::int64_t registrationId, std::int32_t counterId)
{
    for (auto const &handler: m_onUnavailableCounterHandlers)
    {
        CallbackGuard callbackGuard(m_isInCallback);
        handler.second(m_countersReader, registrationId, counterId);
    }
}

void ClientConductor::onOperationSuccess(std::int64_t correlationId)
{
    auto it = m_destinationStateByCorrelationId.find(correlationId);
    if (it != m_destinationStateByCorrelationId.end() &&
        it->second.m_status == RegistrationStatus::AWAITING_MEDIA_DRIVER)
    {
        DestinationStateDefn &state = it->second;

        state.m_status = RegistrationStatus::REGISTERED_MEDIA_DRIVER;
    }
}

void ClientConductor::onChannelEndpointErrorResponse(std::int32_t channelStatusId, const std::string &errorMessage)
{
    for (auto it = m_subscriptionByRegistrationId.begin(); it != m_subscriptionByRegistrationId.end();)
    {
        std::shared_ptr<Subscription> subscription = it->second.m_subscription.lock();

        if (subscription && subscription->channelStatusId() == channelStatusId)
        {
            ChannelEndpointException exception(channelStatusId, errorMessage, SOURCEINFO);
            m_errorHandler(exception);

            std::pair<Image::array_t, std::size_t> imageArrayPair = subscription->closeAndRemoveImages();

            auto imageArray = imageArrayPair.first;
            lingerAllResources(m_epochClock(), imageArray);

            const std::size_t length = imageArrayPair.second;
            for (std::size_t i = 0; i < length; i++)
            {
                auto image = *(imageArray[i]);
                image.close();

                CallbackGuard callbackGuard(m_isInCallback);
                it->second.m_onUnavailableImageHandler(image);
            }

            it = m_subscriptionByRegistrationId.erase(it);
        }
        else
        {
            ++it;
        }
    }

    for (auto it = m_publicationByRegistrationId.begin(); it != m_publicationByRegistrationId.end();)
    {
        std::shared_ptr<Publication> publication = it->second.m_publication.lock();

        if (publication && publication->channelStatusId() == channelStatusId)
        {
            ChannelEndpointException exception(channelStatusId, errorMessage, SOURCEINFO);
            m_errorHandler(exception);

            publication->close();

            it = m_publicationByRegistrationId.erase(it);
        }
        else
        {
            ++it;
        }
    }

    for (auto it = m_exclusivePublicationByRegistrationId.begin(); it != m_exclusivePublicationByRegistrationId.end();)
    {
        std::shared_ptr<ExclusivePublication> publication = it->second.m_publication.lock();

        if (publication && publication->channelStatusId() == channelStatusId)
        {
            ChannelEndpointException exception(channelStatusId, errorMessage, SOURCEINFO);
            m_errorHandler(exception);

            publication->close();

            it = m_exclusivePublicationByRegistrationId.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

void ClientConductor::onErrorResponse(
    std::int64_t offendingCommandCorrelationId, std::int32_t errorCode, const std::string &errorMessage)
{
    auto subIt = m_subscriptionByRegistrationId.find(offendingCommandCorrelationId);
    if (subIt != m_subscriptionByRegistrationId.end())
    {
        subIt->second.m_status = RegistrationStatus::ERRORED_MEDIA_DRIVER;
        subIt->second.m_errorCode = errorCode;
        subIt->second.m_errorMessage = errorMessage;
        return;
    }

    auto pubIt = m_publicationByRegistrationId.find(offendingCommandCorrelationId);
    if (pubIt != m_publicationByRegistrationId.end())
    {
        pubIt->second.m_status = RegistrationStatus::ERRORED_MEDIA_DRIVER;
        pubIt->second.m_errorCode = errorCode;
        pubIt->second.m_errorMessage = errorMessage;
        return;
    }

    auto exPubIt = m_exclusivePublicationByRegistrationId.find(offendingCommandCorrelationId);
    if (exPubIt != m_exclusivePublicationByRegistrationId.end())
    {
        exPubIt->second.m_status = RegistrationStatus::ERRORED_MEDIA_DRIVER;
        exPubIt->second.m_errorCode = errorCode;
        exPubIt->second.m_errorMessage = errorMessage;
        return;
    }

    auto counterIt = m_counterByRegistrationId.find(offendingCommandCorrelationId);
    if (counterIt != m_counterByRegistrationId.end())
    {
        counterIt->second.m_status = RegistrationStatus::ERRORED_MEDIA_DRIVER;
        counterIt->second.m_errorCode = errorCode;
        counterIt->second.m_errorMessage = errorMessage;
        return;
    }

    auto destinationIt = m_destinationStateByCorrelationId.find(offendingCommandCorrelationId);
    if (destinationIt != m_destinationStateByCorrelationId.end())
    {
        destinationIt->second.m_status = RegistrationStatus::ERRORED_MEDIA_DRIVER;
        destinationIt->second.m_errorCode = errorCode;
        destinationIt->second.m_errorMessage = errorMessage;
        return;
    }
}

void ClientConductor::onAvailableImage(
    std::int64_t correlationId,
    std::int32_t sessionId,
    std::int32_t subscriberPositionId,
    std::int64_t subscriptionRegistrationId,
    const std::string &logFilename,
    const std::string &sourceIdentity)
{
    auto it = m_subscriptionByRegistrationId.find(subscriptionRegistrationId);
    if (it != m_subscriptionByRegistrationId.end())
    {
        SubscriptionStateDefn &entry = it->second;
        std::shared_ptr<Subscription> subscription = entry.m_subscription.lock();

        if (nullptr != subscription)
        {
            UnsafeBufferPosition subscriberPosition(m_counterValuesBuffer, subscriberPositionId);

            std::shared_ptr<Image> image = std::make_shared<Image>(
                sessionId,
                correlationId,
                subscriptionRegistrationId,
                sourceIdentity,
                subscriberPosition,
                getLogBuffers(correlationId, logFilename, entry.m_channel),
                m_errorHandler);

            Image::array_t oldImageArray = subscription->addImage(image);
            if (nullptr != oldImageArray)
            {
                lingerResource(m_epochClock(), oldImageArray);
            }

            CallbackGuard callbackGuard(m_isInCallback);
            entry.m_onAvailableImageHandler(*image);
        }
    }
}

void ClientConductor::onUnavailableImage(std::int64_t correlationId, std::int64_t subscriptionRegistrationId)
{
    auto it = m_subscriptionByRegistrationId.find(subscriptionRegistrationId);
    if (it != m_subscriptionByRegistrationId.end())
    {
        SubscriptionStateDefn &entry = it->second;
        std::shared_ptr<Subscription> subscription = entry.m_subscription.lock();

        if (nullptr != subscription)
        {
            std::pair<Image::array_t, std::size_t> result = subscription->removeImage(correlationId);
            Image::array_t oldImageArray = result.first;

            if (nullptr != oldImageArray)
            {
                lingerResource(m_epochClock(), oldImageArray);

                CallbackGuard callbackGuard(m_isInCallback);
                entry.m_onUnavailableImageHandler(*(oldImageArray[result.second]));
            }
        }
    }
}

void ClientConductor::onClientTimeout(std::int64_t clientId)
{
    if (m_driverProxy.clientId() == clientId && !isClosed())
    {
        closeAllResources(m_epochClock());

        ClientTimeoutException exception("client timeout from driver", SOURCEINFO);
        m_errorHandler(exception);
    }
}

void ClientConductor::closeAllResources(long long nowMs)
{
    m_isClosed.store(true, std::memory_order_release);

    for (auto &kv : m_publicationByRegistrationId)
    {
        std::shared_ptr<Publication> pub = kv.second.m_publication.lock();

        if (nullptr != pub)
        {
            pub->close();
        }
    }
    m_publicationByRegistrationId.clear();

    for (auto &kv : m_exclusivePublicationByRegistrationId)
    {
        std::shared_ptr<ExclusivePublication> pub = kv.second.m_publication.lock();

        if (nullptr != pub)
        {
            pub->close();
        }
    }
    m_exclusivePublicationByRegistrationId.clear();

    std::vector<std::shared_ptr<Subscription>> subscriptionsToHoldUntilCleared;

    for (auto &kv : m_subscriptionByRegistrationId)
    {
        std::shared_ptr<Subscription> sub = kv.second.m_subscription.lock();

        if (nullptr != sub)
        {
            std::pair<Image::array_t, std::size_t> imageArrayPair = sub->closeAndRemoveImages();

            auto imageArray = imageArrayPair.first;
            lingerAllResources(nowMs, imageArray);

            const std::size_t length = imageArrayPair.second;
            for (std::size_t i = 0; i < length; i++)
            {
                auto image = *(imageArray[i]);
                image.close();

                CallbackGuard callbackGuard(m_isInCallback);
                kv.second.m_onUnavailableImageHandler(image);
            }

            if (kv.second.m_subscriptionCache)
            {
                subscriptionsToHoldUntilCleared.push_back(kv.second.m_subscriptionCache);
                kv.second.m_subscriptionCache.reset();
            }
        }
    }
    m_subscriptionByRegistrationId.clear();

    std::vector<std::shared_ptr<Counter>> countersToHoldUntilCleared;

    for (auto &kv : m_counterByRegistrationId)
    {
        std::shared_ptr<Counter> counter = kv.second.m_counter.lock();

        if (nullptr != counter)
        {
            counter->close();
            std::int64_t registrationId = counter->registrationId();
            std::int32_t counterId = counter->id();

            for (auto const &handler: m_onUnavailableCounterHandlers)
            {
                CallbackGuard callbackGuard(m_isInCallback);
                handler.second(m_countersReader, registrationId, counterId);
            }

            if (kv.second.m_counterCache)
            {
                countersToHoldUntilCleared.push_back(kv.second.m_counterCache);
                kv.second.m_counterCache.reset();
            }
        }
    }
    m_counterByRegistrationId.clear();

    for (auto const &handler: m_onCloseClientHandlers)
    {
        CallbackGuard callbackGuard(m_isInCallback);
        handler.second();
    }
}

void ClientConductor::onCheckManagedResources(long long nowMs)
{
    for (auto it = m_logBuffersByRegistrationId.begin(); it != m_logBuffersByRegistrationId.end();)
    {
        LogBuffersDefn &entry = it->second;

        if (entry.m_logBuffers.use_count() == 1)
        {
            if (LLONG_MAX == entry.m_timeOfLastStateChangeMs)
            {
                entry.m_timeOfLastStateChangeMs = nowMs;
            }
            else if ((nowMs - m_resourceLingerTimeoutMs) > entry.m_timeOfLastStateChangeMs)
            {
                it = m_logBuffersByRegistrationId.erase(it);
                continue;
            }
        }

        ++it;
    }

    auto arrayIt = std::remove_if(m_lingeringImageLists.begin(), m_lingeringImageLists.end(),
        [nowMs, this](ImageListLingerDefn &entry)
        {
            if ((nowMs - m_resourceLingerTimeoutMs) > entry.m_timeOfLastStateChangeMs)
            {
                delete[] entry.m_imageArray;
                entry.m_imageArray = nullptr;

                return true;
            }

            return false;
        });

    m_lingeringImageLists.erase(arrayIt, m_lingeringImageLists.end());
}

void ClientConductor::lingerResource(long long nowMs, Image::array_t imageArray)
{
    m_lingeringImageLists.emplace_back(nowMs, imageArray);
}

void ClientConductor::lingerAllResources(long long nowMs, Image::array_t imageArray)
{
    if (nullptr != imageArray)
    {
        lingerResource(nowMs, imageArray);
    }
}

}
