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


#ifndef AERON_DATAHEADERFLYWEIGHT_H
#define AERON_DATAHEADERFLYWEIGHT_H

#include <cstdint>
#include <string>
#include <stddef.h>
#include <command/Flyweight.h>
#include <concurrent/AtomicBuffer.h>
#include <util/Index.h>
#include "HeaderFlyweight.h"

namespace aeron { namespace protocol
{

/**
 * HeaderFlyweight for Data Header
 * <p>
 * <a href="https://github.com/real-logic/Aeron/wiki/Protocol-Specification#data-frame">Data Frame</a>
 */
#pragma pack(push)
#pragma pack(4)
struct DataHeaderDefn
{
    HeaderDefn header;
    std::int32_t termOffset;
    std::int32_t sessionId;
    std::int32_t streamId;
    std::int32_t termId;
    std::uint8_t data[0];
};
#pragma pack(pop)

class DataHeaderFlyweight : public HeaderFlyweight
{
public:
    typedef DataHeaderFlyweight this_t;

    DataHeaderFlyweight(concurrent::AtomicBuffer& buffer, std::int32_t offset)
        : HeaderFlyweight(buffer, offset), m_struct(overlayStruct<DataHeaderDefn>(0))
    {
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

    inline std::int32_t termId() const
    {
        return m_struct.termId;
    }

    inline this_t& termId(std::int32_t value)
    {
        m_struct.termId = value;
        return *this;
    }

    inline std::int32_t termOffset()
    {
        return m_struct.termOffset;
    }

    inline this_t& termOffset(std::int32_t value)
    {
        m_struct.termOffset = value;
        return *this;
    }

    inline std::uint8_t* data()
    {
        return m_struct.data;
    }

    inline static constexpr std::int32_t headerLength()
    {
        return sizeof(DataHeaderDefn);
    }

private:
    DataHeaderDefn& m_struct;
};

}}

#endif //AERON_DATAHEADERFLYWEIGHT_H
