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

#ifndef AERON_LOCALSOCKETADDRESSSTATUS_H
#define AERON_LOCALSOCKETADDRESSSTATUS_H

#include "util/Index.h"

namespace aeron { namespace concurrent { namespace status {

class LocalSocketAddressStatus
{
public:
    static std::vector<std::string> findAddresses(
        const CountersReader &countersReader,
        const std::int64_t channelStatus,
        const std::int32_t channelStatusId)
    {
        std::vector<std::string> localAddresses;

        countersReader.forEach(
            [&](std::int32_t counterId, std::int32_t typeId, const AtomicBuffer &keyBuffer, const std::string &label)
            {
                if (typeId == LOCAL_SOCKET_ADDRESS_STATUS_TYPE_ID &&
                    channelStatusId == channelStatusIdFromKeyBuffer(keyBuffer) &&
                    ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE == countersReader.getCounterValue(counterId))
                {
                    localAddresses.push_back(localSocketAddressFromKeyBuffer(keyBuffer));
                }
            });

        return localAddresses;
    }

private:
    static const util::index_t LOCAL_SOCKET_ADDRESS_STATUS_TYPE_ID = 14;
    static const util::index_t CHANNEL_STATUS_ID_OFFSET = 0;
    static const util::index_t LOCAL_SOCKET_ADDRESS_LENGTH_OFFSET =
        CHANNEL_STATUS_ID_OFFSET + static_cast<util::index_t>(sizeof(std::int32_t));
    static const std::int32_t LOCAL_SOCKET_ADDRESS_STRING_OFFSET =
        LOCAL_SOCKET_ADDRESS_LENGTH_OFFSET + static_cast<util::index_t>(sizeof(std::int32_t));

    static std::int32_t channelStatusIdFromKeyBuffer(const AtomicBuffer &keyBuffer)
    {
        return keyBuffer.getInt32(CHANNEL_STATUS_ID_OFFSET);
    }

    static std::string localSocketAddressFromKeyBuffer(const AtomicBuffer &keyBuffer)
    {
        const int32_t length = keyBuffer.getStringLength(LOCAL_SOCKET_ADDRESS_LENGTH_OFFSET);
        return keyBuffer.getStringWithoutLength(LOCAL_SOCKET_ADDRESS_STRING_OFFSET, static_cast<size_t>(length));
    }
};

}}}


#endif //AERON_LOCALSOCKETADDRESSSTATUS_H
