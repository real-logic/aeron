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
#ifndef AERON_COUNTER_MESSAGE_FLYWEIGHT_H
#define AERON_COUNTER_MESSAGE_FLYWEIGHT_H

#include <cstdint>
#include <string>
#include <stddef.h>
#include <util/BitUtil.h>
#include "CorrelatedMessageFlyweight.h"

namespace aeron { namespace command {

/**
 * Message to denote a new counter.
 *
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                         Correlation ID                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        Counter Type ID                        |
 *  +---------------------------------------------------------------+
 *  |                           Key Length                          |
 *  +---------------------------------------------------------------+
 *  |                           Key Buffer                         ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                          Label Length                         |
 *  +---------------------------------------------------------------+
 *  |                          Label (ASCII)                       ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 */
#pragma pack(push)
#pragma pack(4)
struct CounterMessageDefn
{
    CorrelatedMessageDefn correlatedMessage;
    std::int32_t typeId;
};
#pragma pack(pop)

class CounterMessageFlyweight : public CorrelatedMessageFlyweight
{
public:
    typedef CounterMessageFlyweight this_t;

    inline CounterMessageFlyweight(concurrent::AtomicBuffer& buffer, util::index_t offset) :
        CorrelatedMessageFlyweight(buffer, offset), m_struct(overlayStruct<CounterMessageDefn>(0))
    {
    }

    inline std::int32_t typeId() const
    {
        return m_struct.typeId;
    }

    inline this_t& typeId(std::int32_t value)
    {
        m_struct.typeId = value;
        return *this;
    }

    inline const uint8_t *keyBuffer() const
    {
        return bytesAt(keyLengthOffset() + sizeof(std::int32_t));
    }

    inline std::int32_t keyLength() const
    {
        std::int32_t length;

        getBytes(keyLengthOffset(), reinterpret_cast<uint8_t *>(&length), sizeof(length));
        return length;
    }

    inline this_t& keyBuffer(const uint8_t *key, size_t keyLength)
    {
        std::int32_t length = static_cast<std::int32_t>(keyLength);

        putBytes(keyLengthOffset(), reinterpret_cast<const uint8_t *>(&length), sizeof(length));

        if (length > 0)
        {
            putBytes(keyLengthOffset() + sizeof(std::int32_t), key, static_cast<util::index_t>(keyLength));
        }

        return *this;
    }

    inline std::int32_t labelLength() const
    {
        return stringGetLength(labelLengthOffset());
    }

    inline std::string label() const
    {
        return stringGet(labelLengthOffset());
    }

    inline this_t& label(const std::string& label)
    {
        stringPut(labelLengthOffset(), label);

        return *this;
    }

    inline util::index_t length() const
    {
        return labelLengthOffset() + sizeof(std::int32_t) + labelLength();
    }

private:
    CounterMessageDefn& m_struct;

    inline util::index_t keyLengthOffset() const
    {
        return sizeof(CounterMessageDefn);
    }

    inline util::index_t labelLengthOffset() const
    {
        const util::index_t offset = keyLengthOffset();
        const util::index_t unalignedKeyLength = keyLength();
        const util::index_t alignment = static_cast<util::index_t>(sizeof(std::int32_t));
        const util::index_t alignedKeyLength = aeron::util::BitUtil::align(unalignedKeyLength, alignment);

        return offset + sizeof(std::int32_t) + alignedKeyLength;
    }
};

}}
#endif
