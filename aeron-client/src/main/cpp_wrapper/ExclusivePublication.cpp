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

#include "ExclusivePublication.h"
#include "concurrent/status/LocalSocketAddressStatus.h"

namespace aeron {

ExclusivePublication::~ExclusivePublication()
{
    aeron_exclusive_publication_close(m_publication);
}

void ExclusivePublication::addDestination(const std::string& endpointChannel)
{
    if (aeron_exclusive_publication_add_destination(m_publication, endpointChannel.c_str(), NULL) < 0)
    {
        AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
    }
}

void ExclusivePublication::removeDestination(const std::string& endpointChannel)
{
    if (aeron_exclusive_publication_remove_destination(m_publication, endpointChannel.c_str(), NULL) < 0)
    {
        AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
    }
}

std::vector<std::string> ExclusivePublication::localSocketAddresses() const
{
    throw UnsupportedOperationException("Need channel status id through the C API", SOURCEINFO);
}

}
