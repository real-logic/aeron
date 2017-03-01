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

#ifndef INCLUDED_AERON_COMMAND_SETUPFLYWEIGHT__
#define INCLUDED_AERON_COMMAND_SETUPFLYWEIGHT__

#include <cstdint>
#include <string>
#include <stddef.h>
#include <command/Flyweight.h>
#include <concurrent/AtomicBuffer.h>
#include <util/Index.h>
#include "HeaderFlyweight.h"

namespace aeron { namespace protocol {

/**
 * HeaderFlyweight for Setup Frames
 * <p>
 * <a href="https://github.com/real-logic/Aeron/wiki/Protocol-Specification#stream-setup">Stream Setup</a>
 */

#pragma pack(push)
#pragma pack(4)
struct SetupDefn
{
    HeaderDefn headerDefn;
    std::int32_t termOffset;
    std::int32_t sessionId;
    std::int32_t streamId;
    std::int32_t initialTermId;
    std::int32_t actionTermId;
    std::int32_t termLength;
    std::int32_t mtu;
};
#pragma pack(pop)

class SetupFlyweight : public HeaderFlyweight
{
public:
    typedef SetupFlyweight this_t;

    SetupFlyweight(concurrent::AtomicBuffer &buffer, std::int32_t offset)
        : HeaderFlyweight(buffer, offset), m_struct(overlayStruct<SetupDefn>(0))
    {
    }

    inline std::int32_t termOffset() const
    {
        return m_struct.termOffset;
    }

    inline this_t& termOffset(std::int32_t value)
    {
        m_struct.termOffset = value;
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

    inline std::int32_t initialTermId() const
    {
        return m_struct.initialTermId;
    }

    inline this_t& initialTermId(std::int32_t value)
    {
        m_struct.initialTermId = value;
        return *this;
    }

    inline std::int32_t actionTermId() const
    {
        return m_struct.actionTermId;
    }

    inline this_t& actionTermId(std::int32_t value)
    {
        m_struct.actionTermId = value;
        return *this;
    }

    inline std::int32_t termLength() const
    {
        return m_struct.termLength;
    }

    inline this_t& termLength(std::int32_t value)
    {
        m_struct.termLength = value;
        return *this;
    }

    inline std::int32_t mtu() const
    {
        return m_struct.mtu;
    }

    inline this_t& mtu(std::int32_t value)
    {
        m_struct.mtu = value;
        return *this;
    }

    inline static std::int32_t headerLength()
    {
        return sizeof(SetupDefn);
    }

private:
    SetupDefn &m_struct;
};

}}

#endif //INCLUDED_AERON_COMMAND_SETUPFLYWEIGHT__
