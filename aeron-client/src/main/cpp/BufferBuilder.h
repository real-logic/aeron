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
#ifndef AERON_BUFFERBUILDER_H
#define AERON_BUFFERBUILDER_H

#include <limits>
#include "Aeron.h"

namespace aeron {

static const std::uint32_t BUFFER_BUILDER_MAX_CAPACITY = std::numeric_limits<std::uint32_t>::max() - 8;

class BufferBuilder
{
public:
    using this_t = BufferBuilder;

    BufferBuilder(std::uint32_t initialLength) :
        m_capacity(BitUtil::findNextPowerOfTwo(initialLength)),
        m_limit(static_cast<std::uint32_t>(DataFrameHeader::LENGTH)),
        m_buffer(new std::uint8_t[m_capacity])
    {
    }

    virtual ~BufferBuilder()
    {
    }

    BufferBuilder(BufferBuilder &&builder) :
        m_capacity(builder.m_capacity), m_limit(builder.m_limit), m_buffer(std::move(builder.m_buffer))
    {
    }

    std::uint8_t *buffer() const
    {
        return &m_buffer[0];
    }

    std::uint32_t limit() const
    {
        return m_limit;
    }

    void limit(std::uint32_t limit)
    {
        if (limit >= m_capacity)
        {
            throw IllegalArgumentException(
                strPrintf("Limit outside range: capacity=%d limit=%d", m_capacity, limit), SOURCEINFO);
        }

        m_limit = limit;
    }

    this_t &reset()
    {
        m_limit = static_cast<std::uint32_t>(DataFrameHeader::LENGTH);
        return *this;
    }

    this_t &append(AtomicBuffer &buffer, util::index_t offset, util::index_t length, Header &header)
    {
        ensureCapacity(static_cast<std::uint32_t>(length));

        ::memcpy(&m_buffer[0] + m_limit, buffer.buffer() + offset, static_cast<std::uint32_t>(length));
        m_limit += length;
        return *this;
    }

private:
    std::uint32_t m_capacity;
    std::uint32_t m_limit = 0;
    std::unique_ptr<std::uint8_t[]> m_buffer;

    inline static std::uint32_t findSuitableCapacity(std::uint32_t currentCapacity, std::uint32_t requiredCapacity)
    {
        std::uint32_t capacity = currentCapacity;

        do
        {
            const std::uint32_t newCapacity = capacity + (capacity >> 1);

            if (newCapacity < capacity || newCapacity > BUFFER_BUILDER_MAX_CAPACITY)
            {
                if (capacity == BUFFER_BUILDER_MAX_CAPACITY)
                {
                    throw util::IllegalStateException(
                        util::strPrintf("Max capacity reached: %d", BUFFER_BUILDER_MAX_CAPACITY), SOURCEINFO);
                }

                capacity = BUFFER_BUILDER_MAX_CAPACITY;
            }
            else
            {
                capacity = newCapacity;
            }
        }
        while (capacity < requiredCapacity);

        return capacity;
    }

    void ensureCapacity(std::uint32_t additionalCapacity)
    {
        const std::uint32_t requiredCapacity = m_limit + additionalCapacity;

        if (requiredCapacity > m_capacity)
        {
            const std::uint32_t newCapacity = findSuitableCapacity(m_capacity, requiredCapacity);
            std::unique_ptr < std::uint8_t[] > newBuffer(new std::uint8_t[newCapacity]);

            ::memcpy(&newBuffer[0], &m_buffer[0], m_limit);
            m_buffer = std::move(newBuffer);
            m_capacity = newCapacity;
        }
    }
};

}

#endif
