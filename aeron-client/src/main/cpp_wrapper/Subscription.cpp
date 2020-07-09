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

#include "Subscription.h"
#include "concurrent/status/StatusIndicatorReader.h"
#include "concurrent/status/LocalSocketAddressStatus.h"

namespace aeron
{

std::int64_t Subscription::addDestination(const std::string &endpointChannel)
{
    std::int64_t correlationId;
    if (aeron_subscription_add_destination(m_subscription, endpointChannel.c_str(), &correlationId) < 0)
    {
        AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
    }

    return correlationId;
}

std::int64_t Subscription::removeDestination(const std::string &endpointChannel)
{
    std::int64_t correlationId;
    if (aeron_subscription_remove_destination(m_subscription, endpointChannel.c_str(), &correlationId) < 0)
    {
        AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
    }

    return correlationId;
}

bool Subscription::findDestinationResponse(std::int64_t correlationId)
{
    return false;
}

std::vector<std::string> Subscription::localSocketAddresses() const
{
    return aeron::concurrent::status::LocalSocketAddressStatus::findAddresses(
        m_countersReader, channelStatus(), channelStatusId());
}

}
