/*
 * Copyright 2014-2019 Real Logic Ltd.
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
#ifndef AERON_COMMAND_COUNTER_READY_FLYWEIGHT_H
#define AERON_COMMAND_COUNTER_READY_FLYWEIGHT_H

#include <cstdint>
#include <stddef.h>
#include "Flyweight.h"

namespace aeron { namespace command {

/**
 * Message to denote that a Counter has been successfully set up or removed.
 *
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                         Correlation ID                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                           Counter ID                          |
 *  +---------------------------------------------------------------+
 */
#pragma pack(push)
#pragma pack(4)
struct CounterUpdateDefn
{
    std::int64_t correlationId;
    std::int32_t counterId;
};
#pragma pack(pop)

static const util::index_t COUNTER_READY_LENGTH = sizeof(struct CounterUpdateDefn);

class CounterUpdateFlyweight : public Flyweight<CounterUpdateDefn>
{
public:
    typedef CounterUpdateFlyweight this_t;

    inline CounterUpdateFlyweight(concurrent::AtomicBuffer& buffer, util::index_t offset) :
        Flyweight<CounterUpdateDefn>(buffer, offset)
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

    inline std::int32_t counterId() const
    {
        return m_struct.counterId;
    }

    inline this_t& counterId(std::int32_t value)
    {
        m_struct.counterId = value;
        return *this;
    }
};

}}
#endif
