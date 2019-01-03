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
#ifndef AERON_COMMON_FLYWEIGHT_H
#define AERON_COMMON_FLYWEIGHT_H

#include <string>
#include <concurrent/AtomicBuffer.h>

namespace aeron { namespace command {

template<typename struct_t>
class Flyweight
{
public:
    Flyweight (concurrent::AtomicBuffer& buffer, util::index_t offset) :
        m_struct(buffer.overlayStruct<struct_t>(offset)),
        m_buffer(buffer),
        m_baseOffset(offset)
    {
    }

protected:
    struct_t& m_struct;

    inline std::string stringGet(util::index_t offset) const
    {
        return m_buffer.getString(m_baseOffset + offset);
    }

    inline util::index_t stringGetLength(util::index_t offset) const
    {
        return m_buffer.getStringLength(m_baseOffset + offset);
    }

    inline util::index_t stringPut(util::index_t offset, const std::string& s)
    {
        return m_buffer.putString(m_baseOffset + offset, s);
    }

    inline util::index_t stringPutWithoutLength(util::index_t offset, const std::string& s)
    {
        return m_buffer.putStringWithoutLength(m_baseOffset + offset, s);
    }

    inline std::string stringGetWithoutLength(util::index_t offset, std::int32_t size) const
    {
        return m_buffer.getStringWithoutLength(m_baseOffset + offset, size);
    }

    inline std::int32_t getInt32(util::index_t offset) const
    {
        return m_buffer.getInt32(m_baseOffset + offset);
    }

    inline void putInt32(util::index_t offset, std::int32_t value)
    {
        m_buffer.putInt32(m_baseOffset + offset, value);
    }

    inline const uint8_t *bytesAt(util::index_t offset) const
    {
        return m_buffer.buffer() + m_baseOffset + offset;
    }

    inline void putBytes(util::index_t offset, const uint8_t *src, util::index_t length)
    {
        m_buffer.putBytes(m_baseOffset + offset, src, length);
    }

    inline void getBytes(util::index_t offset, uint8_t *dest, util::index_t length) const
    {
        m_buffer.getBytes(offset, dest, length);
    }

    template <typename struct_t2>
    inline struct_t2& overlayStruct (util::index_t offset)
    {
        return m_buffer.overlayStruct<struct_t2>(m_baseOffset + offset);
    }

    template <typename struct_t2>
    inline const struct_t2& overlayStruct (util::index_t offset) const
    {
        return m_buffer.overlayStruct<struct_t2>(m_baseOffset + offset);
    }

private:
    concurrent::AtomicBuffer m_buffer;
    util::index_t m_baseOffset;
};

}}

#endif
