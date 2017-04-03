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
#ifndef INCLUDED_AERON_COMMAND_PUBLICATIONMESSAGEFLYWEIGHT__
#define INCLUDED_AERON_COMMAND_PUBLICATIONMESSAGEFLYWEIGHT__

#include <cstdint>
#include <stddef.h>
#include "Flyweight.h"

#include "CorrelatedMessageFlyweight.h"
#include "PublicationBuffersReadyFlyweight.h"

namespace aeron { namespace command {

/**
* Control message for adding a publication
*
* <p>
* 0                   1                   2                   3
* 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
* +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
* |                            Client ID                          |
* |                                                               |
* +---------------------------------------------------------------+
* |                         Correlation ID                        |
* |                                                               |
* +---------------------------------------------------------------+
* |                          Stream ID                            |
* +---------------------------------------------------------------+
* |                        Channel Length                         |
* +---------------------------------------------------------------+
* |                           Channel                            ...
* +---------------------------------------------------------------+
*...                                                              |
* +---------------------------------------------------------------+
*/

#pragma pack(push)
#pragma pack(4)
struct PublicationMessageDefn
{
    CorrelatedMessageDefn correlatedMessage;
    std::int32_t streamId;
    std::int32_t channelLength;
    std::int8_t  channelData[1];
};
#pragma pack(pop)


class PublicationMessageFlyweight : public CorrelatedMessageFlyweight
{
public:
    using this_t = PublicationMessageFlyweight;

    inline PublicationMessageFlyweight(concurrent::AtomicBuffer& buffer, util::index_t offset)
        : CorrelatedMessageFlyweight(buffer, offset), m_struct(overlayStruct<PublicationMessageDefn>(0))
    {
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
        return stringGet(offsetof(PublicationMessageDefn, channelLength));
    }

    inline this_t& channel(const std::string& value)
    {
        stringPut(offsetof(PublicationMessageDefn, channelLength), value);
        return *this;
    }

    util::index_t length()
    {
        return offsetof(PublicationMessageDefn, channelData) + m_struct.channelLength;
    }

private:
    PublicationMessageDefn& m_struct;
};

}}

#endif
