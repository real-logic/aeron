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
#ifndef AERON_CLIENT_TIMEOUT_FLYWEIGHT_H
#define AERON_CLIENT_TIMEOUT_FLYWEIGHT_H

#include <cstdint>
#include <stddef.h>
#include "Flyweight.h"

namespace aeron { namespace command {

/**
 * Message to denote that a client has timed out wrt the driver.
 *
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                         Client ID                             |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 */
#pragma pack(push)
#pragma pack(4)
struct ClientTimeoutDefn
{
    std::int64_t clientId;
};
#pragma pack(pop)

static const util::index_t CLIENT_TIMEOUT_LENGTH = sizeof(struct ClientTimeoutDefn);

class ClientTimeoutFlyweight : public Flyweight<ClientTimeoutDefn>
{
public:
    typedef ClientTimeoutFlyweight this_t;

    inline ClientTimeoutFlyweight(concurrent::AtomicBuffer& buffer, util::index_t offset) :
        Flyweight<ClientTimeoutDefn>(buffer, offset)
    {
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
