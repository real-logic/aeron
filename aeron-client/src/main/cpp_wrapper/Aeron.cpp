/*
 * Copyright 2014-2020 Real Logic Limited.
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

#include "Aeron.h"

extern "C"
{
#include "aeron_client_conductor.h"
}

namespace aeron
{

Aeron::Aeron(Context &context) :
    m_context(context.conclude()),
    m_aeron(init_aeron(m_context)),
    m_countersReader(aeron_counters_reader(m_aeron)),
    m_clientConductor(m_aeron),
    m_conductorInvoker(m_clientConductor, m_context.m_exceptionHandler)
{
    aeron_start(m_aeron);
}

Aeron::~Aeron()
{
    aeron_close(m_aeron);
    aeron_context_close(m_context.m_context);
}

aeron_t *Aeron::init_aeron(Context &context)
{
    aeron_t *aeron;
    context.attachCallbacksToContext();
    if (aeron_init(&aeron, context.m_context) < 0)
    {
        AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
    }
    return aeron;
}

std::int64_t Aeron::addPublication(const std::string &channel, std::int32_t streamId)
{
    AsyncAddPublication *addPublication = addPublicationAsync(channel, streamId);
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    m_pendingPublications[addPublication->registration_id] = addPublication;
    return addPublication->registration_id;
}

std::int64_t Aeron::addExclusivePublication(const std::string &channel, std::int32_t streamId)
{
    AsyncAddExclusivePublication *addExclusivePublication = addExclusivePublicationAsync(channel, streamId);
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    m_pendingExclusivePublications[addExclusivePublication->registration_id] = addExclusivePublication;
    return addExclusivePublication->registration_id;
}

std::int64_t Aeron::addSubscription(
    const std::string &channel,
    std::int32_t streamId,
    const on_available_image_t &onAvailableImageHandler,
    const on_unavailable_image_t &onUnavailableImageHandler)
{
    AsyncAddSubscription *addSubscription = addSubscriptionAsync(
        channel, streamId, onAvailableImageHandler, onUnavailableImageHandler);
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    m_pendingSubscriptions[addSubscription->m_async->registration_id] = addSubscription;
    return addSubscription->m_async->registration_id;
}

std::int64_t Aeron::addCounter(
    std::int32_t typeId,
    const std::uint8_t *keyBuffer,
    std::size_t keyLength,
    const std::string &label)
{
    AsyncAddCounter *addCounter = addCounterAsync(typeId, keyBuffer, keyLength, label);
    std::lock_guard<std::recursive_mutex> lock(m_adminLock);
    m_pendingCounters[addCounter->registration_id] = addCounter;
    return addCounter->registration_id;
}

std::string Aeron::version()
{
    return std::string(aeron_version_full());
}

}
