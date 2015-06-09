/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
#ifndef INCLUDED_AERON_COMMAND_CONNECTIONMESSAGEFLYWEIGHT__
#define INCLUDED_AERON_COMMAND_CONNECTIONMESSAGEFLYWEIGHT__

#include <cstdint>
#include <stddef.h>
#include "Flyweight.h"

namespace aeron { namespace command {

/**
* Control message flyweight for any message that needs to represent a connection
*
* <p>
* 0                   1                   2                   3
* 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
* +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
* |                        Correlation ID                         |
* |                                                               |
* +---------------------------------------------------------------+
* |                          Session ID                           |
* +---------------------------------------------------------------+
* |                          Stream ID                            |
* +---------------------------------------------------------------+
* |                          Position                             |
* |                                                               |
* +---------------------------------------------------------------+
* |                        Channel Length                         |
* +---------------------------------------------------------------+
* |                           Channel                           ...
* ...                                                             |
* +---------------------------------------------------------------+
*/

#pragma pack(push)
#pragma pack(4)
struct ConnectionMessageDefn
{
    std::int64_t correlationId;
    std::int32_t sessionId;
    std::int32_t streamId;
    std::int64_t position;
    struct
    {
        std::int32_t channelLength;
        std::int8_t  channelData[1];
    } channel;
};
#pragma pack(pop)


class ConnectionMessageFlyweight : public Flyweight<ConnectionMessageDefn>
{
public:
    typedef ConnectionMessageFlyweight this_t;

    inline ConnectionMessageFlyweight (concurrent::AtomicBuffer& buffer, util::index_t offset) :
        Flyweight<ConnectionMessageDefn>(buffer, offset)
    {
    }

    inline std::int64_t correlationId() const
    {
        return m_struct.correlationId;
    }

    inline this_t& correlationId(std::int64_t value)
    {
        m_struct.correlationId = value;
        return *this;
    }

    inline std::int32_t sessionId() const
    {
        return m_struct.sessionId;
    }

    inline this_t& sessionId(std::int32_t value)
    {
        m_struct.sessionId = value;
        return *this;
    }

    inline std::int32_t streamId() const
    {
        return m_struct.streamId;
    }

    inline this_t& streamId(std::int32_t value)
    {
        m_struct.streamId = value;
        return *this;
    }

    inline std::int64_t position() const
    {
        return m_struct.position;
    }

    inline this_t& position(std::int64_t value)
    {
        m_struct.position = value;
        return *this;
    }

    inline std::string channel()
    {
        return stringGet(offsetof(ConnectionMessageDefn, channel));
    }

    inline this_t& channel(const std::string& value)
    {
        stringPut(offsetof(ConnectionMessageDefn, channel), value);
        return *this;
    }

    util::index_t length()
    {
        return offsetof(ConnectionMessageDefn, channel.channelData) + m_struct.channel.channelLength;
    }
};

}}
#endif