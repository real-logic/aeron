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

#ifndef AERON_STATUS_INDICATOR_READER_H
#define AERON_STATUS_INDICATOR_READER_H

#include "concurrent/AtomicBuffer.h"
#include "concurrent/CountersReader.h"

namespace aeron { namespace concurrent { namespace status {

namespace ChannelEndpointStatus
{
static const std::int64_t CHANNEL_ENDPOINT_INITIALIZING = 0;
static const std::int64_t CHANNEL_ENDPOINT_ERRORED = -1;
static const std::int64_t CHANNEL_ENDPOINT_ACTIVE = 1;
static const std::int64_t CHANNEL_ENDPOINT_CLOSING = 2;

static const std::int32_t NO_ID_ALLOCATED = -1;
}

class StatusIndicatorReader
{
public:
    StatusIndicatorReader(AtomicBuffer& buffer, std::int32_t id) :
        m_staticBuffer(),
        m_id(id),
        m_offset(CountersReader::counterOffset(id))
    {
        if (ChannelEndpointStatus::NO_ID_ALLOCATED == m_id)
        {
            m_buffer.wrap(m_staticBuffer);
            m_offset = 0;
            m_buffer.putInt64Ordered(0, ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE);
        }
        else
        {
            m_buffer.wrap(buffer);
        }
    }

    StatusIndicatorReader(const StatusIndicatorReader& indicatorReader) :
        m_staticBuffer(indicatorReader.m_staticBuffer),
        m_id(indicatorReader.m_id),
        m_offset(indicatorReader.m_offset)
    {
        if (ChannelEndpointStatus::NO_ID_ALLOCATED == m_id)
        {
            m_buffer.wrap(m_staticBuffer);
        }
        else
        {
            m_buffer.wrap(indicatorReader.m_buffer);
        }

        m_buffer.putInt64Ordered(m_offset, indicatorReader.m_buffer.getInt64Volatile(indicatorReader.m_offset));
    }

    StatusIndicatorReader& operator=(const StatusIndicatorReader& indicatorReader)
    {
        m_staticBuffer = indicatorReader.m_staticBuffer;
        m_id = indicatorReader.m_id;
        m_offset = indicatorReader.m_offset;

        if (ChannelEndpointStatus::NO_ID_ALLOCATED == m_id)
        {
            m_buffer.wrap(m_staticBuffer);
        }
        else
        {
            m_buffer.wrap(indicatorReader.m_buffer);
        }

        m_buffer.putInt64Ordered(m_offset, indicatorReader.m_buffer.getInt64Volatile(indicatorReader.m_offset));

        return *this;
    }

    inline std::int32_t id() const
    {
        return m_id;
    }

    inline std::int64_t getVolatile() const
    {
        return m_buffer.getInt64Volatile(m_offset);
    }

private:
    std::array<std::uint8_t, sizeof(std::int64_t)> m_staticBuffer;
    AtomicBuffer m_buffer;
    std::int32_t m_id;
    std::int32_t m_offset;
};

}}}

#endif
