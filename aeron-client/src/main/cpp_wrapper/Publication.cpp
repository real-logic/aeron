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

#include <utility>

#include "Publication.h"
#include "concurrent/status/LocalSocketAddressStatus.h"
extern "C"
{
#include "aeron_client_conductor.h"
}

namespace aeron
{

Publication::~Publication()
{
    aeron_publication_close(m_publication, NULL, NULL);
    // TODO: How should cleanup occur here?
}

std::int64_t Publication::addDestination(const std::string &endpointChannel)
{
    int64_t correlationId;
    if (aeron_publication_add_destination(m_publication, endpointChannel.c_str(), &correlationId) < 0)
    {
        AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
    }
    return correlationId;
}

std::int64_t Publication::removeDestination(const std::string &endpointChannel)
{
    int64_t correlationId;
    if (aeron_publication_remove_destination(m_publication, endpointChannel.c_str(), &correlationId) < 0)
    {
        AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
    }
    return correlationId;
}

bool Publication::findDestinationResponse(std::int64_t correlationId)
{
    throw UnsupportedOperationException(
        "Should look at using the same async approach to adding destinations", SOURCEINFO);
//    aeron_client_conductor_find_destination_response(correlationId);
//    return m_conductor.findDestinationResponse(correlationId);
}

std::vector<std::string> Publication::localSocketAddresses() const
{
    throw UnsupportedOperationException("Not yet implemented, need the channel status id first", SOURCEINFO);
    //    return LocalSocketAddressStatus::findAddresses(m_conductor.countersReader(), channelStatus(), channelStatusId());
}

}
