/*
 * Copyright 2015 Real Logic Ltd.
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

#ifndef AERON_UNSAFEBUFFERPOSITION_H
#define AERON_UNSAFEBUFFERPOSITION_H

#include <concurrent/AtomicBuffer.h>
#include <concurrent/CountersManager.h>
#include "Position.h"

namespace aeron { namespace concurrent { namespace status {

class UnsafeBufferPosition
{
public:
    UnsafeBufferPosition(AtomicBuffer& buffer, std::int32_t id) :
        m_buffer(buffer),
        m_id(id),
        m_offset(CountersManager::counterOffset(id))
    {
    }

    UnsafeBufferPosition(UnsafeBufferPosition& position)
    {
        m_buffer.wrap(position.m_buffer);
        m_id = position.m_id;
        m_offset = position.m_offset;
    }

    UnsafeBufferPosition() :
        m_id(-1),
        m_offset(0)
    {
    }

    inline void wrap(UnsafeBufferPosition& position)
    {
        m_buffer.wrap(position.m_buffer);
        m_id = position.m_id;
        m_offset = position.m_offset;
    }

    inline std::int32_t id()
    {
        return m_id;
    }

    inline std::int64_t get()
    {
        return m_buffer.getInt64(m_offset);
    }

    inline std::int64_t getVolatile()
    {
        return m_buffer.getInt64Volatile(m_offset);
    }

    inline void set(std::int64_t value)
    {
        m_buffer.putInt64(m_offset, value);
    }

    inline void setOrdered(std::int64_t value)
    {
        m_buffer.putInt64Ordered(m_offset, value);
    }

    inline void close()
    {
    }

private:
    AtomicBuffer m_buffer;
    std::int32_t m_id;
    std::int32_t m_offset;
};

}}}

#endif //AERON_UNSAFEBUFFERPOSITION_H
