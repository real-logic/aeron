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

#ifndef AERON_TERMINATE_DRIVER_FLYWEIGHT_H
#define AERON_TERMINATE_DRIVER_FLYWEIGHT_H

#include <cstdint>
#include <string>
#include <stddef.h>
#include <util/BitUtil.h>
#include "CorrelatedMessageFlyweight.h"

namespace aeron { namespace command {

/**
 * Command message flyweight to ask the driver process to terminate
 *
 * @see ControlProtocolEvents
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                         Correlation ID                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                         Token Length                          |
 *  +---------------------------------------------------------------+
 *  |                         Token Buffer                         ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */
#pragma pack(push)
#pragma pack(4)
struct TerminateDriverDefn
{
    CorrelatedMessageDefn correlatedMessage;
    std::int32_t tokenLength;
};
#pragma pack(pop)

class TerminateDriverFlyweight : public CorrelatedMessageFlyweight
{
public:
    typedef TerminateDriverFlyweight this_t;

    inline TerminateDriverFlyweight(concurrent::AtomicBuffer& buffer, util::index_t offset) :
    CorrelatedMessageFlyweight(buffer, offset), m_struct(overlayStruct<TerminateDriverDefn>(0))
    {
    }

    inline const uint8_t *tokenBuffer() const
    {
        return bytesAt(sizeof(TerminateDriverDefn));
    }

    inline std::int32_t tokenLength() const
    {
        return m_struct.tokenLength;
    }

    inline this_t& tokenBuffer(const uint8_t *tokenBuffer, size_t tokenLength)
    {
        m_struct.tokenLength = static_cast<std::int32_t>(tokenLength);

        if (tokenLength > 0)
        {
            putBytes(sizeof(TerminateDriverDefn), tokenBuffer, static_cast<util::index_t>(tokenLength));
        }

        return *this;
    }

    inline util::index_t length() const
    {
        return sizeof(TerminateDriverDefn) + m_struct.tokenLength;
    }

private:
    TerminateDriverDefn& m_struct;
};

}}

#endif //AERON_TERMINATE_DRIVER_FLYWEIGHT_H
