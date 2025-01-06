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

#ifndef AERON_LOCALSOCKETADDRESSSTATUS_H
#define AERON_LOCALSOCKETADDRESSSTATUS_H

#include "concurrent/status/StatusIndicatorReader.h"

namespace aeron { namespace concurrent { namespace status {

class LocalSocketAddressStatus
{
public:
    static std::vector<std::string> findAddresses(
        const CountersReader &countersReader, const std::int64_t channelStatus, const std::int32_t channelStatusId)
    {
        std::vector<std::string> localAddresses;

        if (ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE == channelStatus)
        {
            countersReader.forEach(
                [&](std::int32_t id, std::int32_t typeId, const AtomicBuffer &keyBuffer, const std::string &label)
                {
                    if (typeId == LOCAL_SOCKET_ADDRESS_STATUS_TYPE_ID &&
                        channelStatusId == channelStatusIdFromKeyBuffer(keyBuffer) &&
                        ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE == countersReader.getCounterValue(id))
                    {
                        localAddresses.push_back(localSocketAddressFromKeyBuffer(keyBuffer));
                    }
                });
        }

        return localAddresses;
    }

    static std::string findAddress(
        const CountersReader &countersReader, const std::int64_t channelStatus, const std::int32_t channelStatusId)
    {
        if (ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE == channelStatus)
        {
            for (std::int32_t i = 0, size = countersReader.maxCounterId(); i < size; i++)
            {
                const std::int32_t counterState = countersReader.getCounterState(i);
                if (CountersReader::RECORD_ALLOCATED == counterState)
                {
                    std::uint8_t *keyPtr = countersReader.metaDataBuffer().buffer() +
                        static_cast<std::size_t>(aeron::concurrent::CountersReader::metadataOffset(i)) +
                        static_cast<std::size_t>(CountersReader::KEY_OFFSET);
                    const AtomicBuffer keyBuffer(keyPtr, static_cast<std::size_t>(CountersReader::MAX_KEY_LENGTH));

                    if (countersReader.getCounterTypeId(i) == LOCAL_SOCKET_ADDRESS_STATUS_TYPE_ID &&
                        channelStatusId == channelStatusIdFromKeyBuffer(keyBuffer) &&
                        ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE == countersReader.getCounterValue(i))
                    {
                        std::string endpoint = localSocketAddressFromKeyBuffer(keyBuffer);
                        if (!endpoint.empty())
                        {
                            return endpoint;
                        }
                    }
                }
                else if (CountersReader::RECORD_UNUSED == counterState)
                {
                    break;
                }
            }
        }

        return {};
    }

private:
    static const index_t LOCAL_SOCKET_ADDRESS_STATUS_TYPE_ID = 14;
    static const index_t CHANNEL_STATUS_ID_OFFSET = 0;
    static const index_t LOCAL_SOCKET_ADDRESS_LENGTH_OFFSET =
        CHANNEL_STATUS_ID_OFFSET + static_cast<index_t>(sizeof(std::int32_t));
    static const index_t LOCAL_SOCKET_ADDRESS_STRING_OFFSET =
        LOCAL_SOCKET_ADDRESS_LENGTH_OFFSET + static_cast<index_t>(sizeof(std::int32_t));

    static std::int32_t channelStatusIdFromKeyBuffer(const AtomicBuffer &buffer)
    {
        return buffer.getInt32(CHANNEL_STATUS_ID_OFFSET);
    }

    static std::string localSocketAddressFromKeyBuffer(const AtomicBuffer &buffer)
    {
        const std::int32_t length = buffer.getStringLength(LOCAL_SOCKET_ADDRESS_LENGTH_OFFSET);
        return buffer.getStringWithoutLength(LOCAL_SOCKET_ADDRESS_STRING_OFFSET, static_cast<std::size_t>(length));
    }
};

}}}


#endif //AERON_LOCALSOCKETADDRESSSTATUS_H
