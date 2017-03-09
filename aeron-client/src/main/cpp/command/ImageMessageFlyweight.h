/*
 * Copyright 2014-2017 Real Logic Ltd.
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
* Control message flyweight for any message that needs to represent an image
*
* <p>
* 0                   1                   2                   3
* 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
* +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
* |                        Correlation ID                         |
* |                                                               |
* +---------------------------------------------------------------+
* |                          Stream ID                            |
* +---------------------------------------------------------------+
* |                        Channel Length                         |
* +---------------------------------------------------------------+
* |                           Channel                           ...
* ...                                                             |
* +---------------------------------------------------------------+
*/

#pragma pack(push)
#pragma pack(4)
struct ImageMessageDefn
{
    std::int64_t correlationId;
    std::int32_t streamId;
    std::int32_t channelLength;
    std::int8_t  channelData[1];
};
#pragma pack(pop)


class ImageMessageFlyweight : public Flyweight<ImageMessageDefn>
{
public:
    typedef ImageMessageFlyweight this_t;

    inline ImageMessageFlyweight(concurrent::AtomicBuffer& buffer, util::index_t offset) :
        Flyweight<ImageMessageDefn>(buffer, offset)
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

    inline std::int32_t streamId() const
    {
        return m_struct.streamId;
    }

    inline this_t& streamId(std::int32_t value)
    {
        m_struct.streamId = value;
        return *this;
    }

    inline std::string channel() const
    {
        return stringGet(offsetof(ImageMessageDefn, channelLength));
    }

    inline this_t& channel(const std::string& value)
    {
        stringPut(offsetof(ImageMessageDefn, channelLength), value);
        return *this;
    }

    inline std::int32_t length() const
    {
        return offsetof(ImageMessageDefn, channelData) + m_struct.channelLength;
    }
};

}}
#endif
