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
#ifndef INCLUDED_AERON_COMMAND_CORRELATEDMESSAGEFLYWEIGHT__
#define INCLUDED_AERON_COMMAND_CORRELATEDMESSAGEFLYWEIGHT__

#include <cstdint>
#include <stddef.h>
#include "Flyweight.h"

namespace aeron { namespace command {

/**
* 0                   1                   2                   3
* 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
* +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
* |                            Client ID                          |
* |                                                               |
* +---------------------------------------------------------------+
* |                         Correlation ID                        |
* |                                                               |
* +---------------------------------------------------------------+
*/

#pragma pack(push)
#pragma pack(4)
struct CorrelatedMessageDefn
{
    std::int64_t clientId;
    std::int64_t correlationId;
};
#pragma pack(pop)

static const util::index_t CORRELATED_MESSAGE_LENGTH = sizeof(struct CorrelatedMessageDefn);

class CorrelatedMessageFlyweight : public Flyweight<CorrelatedMessageDefn>
{
public:
    typedef CorrelatedMessageFlyweight this_t;

    inline CorrelatedMessageFlyweight (concurrent::AtomicBuffer& buffer, util::index_t offset)
            : Flyweight<CorrelatedMessageDefn>(buffer, offset)
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

    inline std::int64_t clientId() const
    {
        return m_struct.clientId;
    }

    inline this_t& clientId(std::int64_t value)
    {
        m_struct.clientId = value;
        return *this;
    }
};

}}
#endif
