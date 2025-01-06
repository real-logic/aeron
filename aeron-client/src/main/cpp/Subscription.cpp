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

#include "Subscription.h"
#include "ClientConductor.h"
#include "ChannelUri.h"
#include "concurrent/status/LocalSocketAddressStatus.h"

using namespace aeron;
using namespace aeron::concurrent::status;

Subscription::Subscription(
    ClientConductor &conductor,
    std::int64_t registrationId,
    const std::string &channel,
    std::int32_t streamId,
    std::int32_t channelStatusId) :
    m_conductor(conductor),
    m_channel(channel),
    m_channelStatusId(channelStatusId),
    m_streamId(streamId),
    m_registrationId(registrationId)
{
    static_cast<void>(m_paddingBefore);
    static_cast<void>(m_paddingAfter);
}

Subscription::~Subscription()
{
    auto imageArrayPair = m_imageArray.load();

    m_conductor.releaseSubscription(m_registrationId, imageArrayPair.first, imageArrayPair.second);
}

std::int64_t Subscription::addDestination(const std::string &endpointChannel)
{
    if (isClosed())
    {
        throw util::IllegalStateException(std::string("Subscription is closed"), SOURCEINFO);
    }

    return m_conductor.addRcvDestination(m_registrationId, endpointChannel);
}

std::int64_t Subscription::removeDestination(const std::string &endpointChannel)
{
    if (isClosed())
    {
        throw util::IllegalStateException(std::string("Subscription is closed"), SOURCEINFO);
    }

    return m_conductor.removeRcvDestination(m_registrationId, endpointChannel);
}

bool Subscription::findDestinationResponse(std::int64_t correlationId)
{
    return m_conductor.findDestinationResponse(correlationId);
}

std::int64_t Subscription::channelStatus() const
{
    if (isClosed())
    {
        return ChannelEndpointStatus::NO_ID_ALLOCATED;
    }

    return m_conductor.channelStatus(m_channelStatusId);
}

std::vector<std::string> Subscription::localSocketAddresses() const
{
    return LocalSocketAddressStatus::findAddresses(
        m_conductor.countersReader(), channelStatus(), channelStatusId());
}

std::string Subscription::tryResolveChannelEndpointPort() const
{
    const std::int64_t currentChannelStatus = channelStatus();

    if (ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE == currentChannelStatus)
    {
        std::vector<std::string> localSocketAddresses = LocalSocketAddressStatus::findAddresses(
            m_conductor.countersReader(), currentChannelStatus, m_channelStatusId);

        if (1 == localSocketAddresses.size())
        {
            std::shared_ptr<ChannelUri> channelUriPtr = ChannelUri::parse(m_channel);
            std::string endpoint = channelUriPtr->get(ENDPOINT_PARAM_NAME);

            if (!endpoint.empty() && endsWith(endpoint, std::string(":0")))
            {
                std::string &resolvedEndpoint = localSocketAddresses.at(0);
                std::size_t i = resolvedEndpoint.find_last_of(':');
                std::string newEndpoint = endpoint.substr(0, endpoint.length() - 2) + resolvedEndpoint.substr(i);

                channelUriPtr->put(ENDPOINT_PARAM_NAME, newEndpoint);

                return channelUriPtr->toString();
            }
        }

        return m_channel;
    }

    return {};
}

std::string Subscription::resolvedEndpoint() const
{
    return LocalSocketAddressStatus::findAddress(m_conductor.countersReader(), channelStatus(), m_channelStatusId);
}
