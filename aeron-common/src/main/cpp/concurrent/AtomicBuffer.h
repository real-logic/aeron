/*
 * Copyright 2014 Real Logic Ltd.
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

#ifndef INCLUDED_AERON_CONCURRENT_ATOMIC_BUFFER__
#define INCLUDED_AERON_CONCURRENT_ATOMIC_BUFFER__

#include <cstdint>
#include <string.h>
#include <string>
#include <mintomic/mintomic.h>

#include <util/Exceptions.h>
#include <util/StringUtil.h>
#include <util/Index.h>

namespace aeron { namespace common { namespace concurrent {


// note: Atomic Buffer does not own the memory it wraps.
class AtomicBuffer
{
public:
    AtomicBuffer(std::uint8_t *buffer, util::index_t length);

    template <typename struct_t>
    struct_t& overlayStruct (util::index_t offset)
    {
        boundsCheck(offset, sizeof(struct_t));
        return *reinterpret_cast<struct_t*>(m_buffer + offset);
    }

    template <typename struct_t>
    const struct_t& overlayStruct (util::index_t offset) const
    {
        boundsCheck(offset, sizeof(struct_t));
        return *reinterpret_cast<struct_t*>(m_buffer + offset);
    }

    inline void putInt64(util::index_t offset, std::int64_t v)
    {
//        printf("putInt64 %d\n", offset);
        boundsCheck(offset, sizeof(std::int64_t));
        *reinterpret_cast<std::int64_t *>(m_buffer + offset) = v;
    }

    inline std::int64_t getInt64(util::index_t offset) const
    {
        boundsCheck(offset, sizeof(std::int64_t));
        return *reinterpret_cast<std::int64_t *>(m_buffer + offset);
    }

    inline void putInt32(util::index_t offset, std::int32_t v)
    {
//        printf("putInt32 %d\n", offset);
        boundsCheck(offset, sizeof(std::int32_t));
        *reinterpret_cast<std::int32_t *>(m_buffer + offset) = v;
    }

    inline std::int32_t getInt32(util::index_t offset) const
    {
//        printf("getInt32 %d\n", offset);
        boundsCheck(offset, sizeof(std::int32_t));
        return *reinterpret_cast<std::int32_t *>(m_buffer + offset);
    }

    inline void putUInt16(util::index_t offset, std::uint16_t v)
    {
        boundsCheck(offset, sizeof(std::uint16_t));
        *reinterpret_cast<std::uint16_t *>(m_buffer + offset) = v;
    }

    inline void putUInt8(util::index_t offset, std::uint8_t v)
    {
        boundsCheck(offset, sizeof(std::uint8_t));
        *reinterpret_cast<std::uint8_t *>(m_buffer + offset) = v;
    }

    inline void putInt64Ordered(util::index_t offset, std::int64_t v)
    {
//        printf("putInt64 %d Ordered\n", offset);
        boundsCheck(offset, sizeof(std::int64_t));
        mint_thread_fence_release();
        mint_store_64_relaxed((mint_atomic64_t*)(m_buffer + offset), v);
    }

    inline std::int64_t getInt64Ordered(util::index_t offset) const
    {
//        printf("getInt64Ordered %d\n", offset);
        boundsCheck(offset, sizeof(std::int64_t));
        std::int64_t v = mint_load_64_relaxed((mint_atomic64_t*)(m_buffer + offset));
        mint_thread_fence_acquire();
        return v;
    }

    inline void putInt32Ordered(util::index_t offset, std::int32_t v)
    {
//        printf("putInt32 %d %d Ordered\n", offset, v);
        boundsCheck(offset, sizeof(std::int32_t));
        mint_thread_fence_release();
        mint_store_32_relaxed((mint_atomic32_t*)(m_buffer + offset), v);
    }

    inline std::int32_t getInt32Ordered(util::index_t offset) const
    {
//        printf("getInt32 %d Ordered\n", offset);
        boundsCheck(offset, sizeof(std::int32_t));
        std::int32_t v = mint_load_32_relaxed((mint_atomic32_t*)(m_buffer + offset));
        mint_thread_fence_acquire();
        return v;
    }

    inline void putInt64Atomic(util::index_t offset, std::int64_t v)
    {
        boundsCheck(offset, sizeof(std::int64_t));
        mint_thread_fence_release();
        mint_store_64_relaxed((mint_atomic64_t*)(m_buffer + offset), v);
        mint_thread_fence_seq_cst();
    }

    inline std::int64_t getInt64Atomic(util::index_t offset) const
    {
        boundsCheck(offset, sizeof(std::int64_t));
        mint_thread_fence_seq_cst();
        std::int64_t v = mint_load_64_relaxed((mint_atomic64_t*)(m_buffer + offset));
        mint_thread_fence_acquire();
        return v;
    }

    inline void putInt32Atomic(util::index_t offset, std::int32_t v)
    {
        boundsCheck(offset, sizeof(std::int32_t));
        mint_thread_fence_release();
        mint_store_32_relaxed((mint_atomic32_t*)(m_buffer + offset), v);
        mint_thread_fence_seq_cst();
    }

    inline std::int32_t getInt32Atomic(util::index_t offset) const
    {
        boundsCheck(offset, sizeof(std::int32_t));
        mint_thread_fence_seq_cst();
        std::int32_t v = mint_load_32_relaxed((mint_atomic32_t*)(m_buffer + offset));
        mint_thread_fence_acquire();
        return v;
    }

    inline void addInt64Ordered(util::index_t offset, std::int64_t increment)
    {
        boundsCheck(offset, sizeof(std::int64_t));
        mint_fetch_add_64_relaxed((mint_atomic64_t*)(m_buffer + offset), increment);
    }

    inline bool compareAndSetInt64(util::index_t offset, std::int64_t expectedValue, std::int64_t updateValue)
    {
//        printf("compareAndSetInt64 %d %d == %d\n", offset, expectedValue, updateValue);
        boundsCheck(offset, sizeof(std::int64_t));
        std::int64_t original = mint_compare_exchange_strong_64_relaxed((mint_atomic64_t*)(m_buffer + offset), expectedValue, updateValue);
        return (original == expectedValue);
    }

    inline std::int64_t getAndSetInt64(util::index_t offset, std::int64_t value) const
    {
        // TODO: no idea how to implement this with mintomic....
        return 0;
    }

    inline std::int64_t getAndAddInt64(util::index_t offset, std::int64_t delta) const
    {
        boundsCheck(offset, sizeof(std::int64_t));
        return mint_fetch_add_64_relaxed((mint_atomic64_t*)(m_buffer + offset), delta);
    }

    inline void addInt32Ordered(util::index_t offset, std::int32_t increment)
    {
        boundsCheck(offset, sizeof(std::int32_t));
        mint_fetch_add_32_relaxed((mint_atomic32_t*)(m_buffer + offset), increment);
    }

    inline bool compareAndSetInt32(util::index_t offset, std::int32_t expectedValue, std::int32_t updateValue)
    {
        boundsCheck(offset, sizeof(std::int32_t));
        mint_compare_exchange_strong_32_relaxed((mint_atomic32_t*)(m_buffer + offset), expectedValue, updateValue);
        return true; // always works??
    }

    inline std::int32_t getAndSetInt32(util::index_t offset, std::int32_t value) const
    {
        // TODO: no idea how to implement this with mintomic....
        return 0;
    }

    inline std::int32_t getAndAddInt32(util::index_t offset, std::int32_t delta) const
    {
        boundsCheck(offset, sizeof(std::int32_t));
        return mint_fetch_add_32_relaxed((mint_atomic32_t*)(m_buffer + offset), delta);
    }

    inline void putBytes(util::index_t index, concurrent::AtomicBuffer& srcBuffer, util::index_t srcIndex, util::index_t length)
    {
//        printf("putBytes %d %d\n", index, length);
        boundsCheck(index, length);
        srcBuffer.boundsCheck(srcIndex, length);

        ::memcpy(m_buffer + index, srcBuffer.m_buffer + srcIndex, length);
    }

    inline void putBytes(util::index_t index, std::uint8_t *srcBuffer, util::index_t length)
    {
        boundsCheck(index, length);
        ::memcpy(m_buffer + index, srcBuffer, length);
    }

    inline void setMemory(util::index_t offset , size_t length, std::uint8_t value)
    {
//        printf("setMemory %d %d\n", offset, length);
        boundsCheck(offset, length);
        memset(m_buffer + offset, value, length);
    }

    // Note: I am assuming that std::string is utf8 encoded
    // TODO: add std::wstring support
    inline std::string getStringUtf8(util::index_t offset) const
    {
        std::int32_t length = getInt32(offset);
        return getStringUtf8WithoutLength(offset + sizeof(std::int32_t), (size_t)length);
    }

    inline std::string getStringUtf8WithoutLength(util::index_t offset, size_t length) const
    {
        boundsCheck(offset, length);
        return std::string(m_buffer + offset, m_buffer + offset + length);
    }

    std::int32_t putStringUtf8(util::index_t offset, const std::string& value)
    {
        std::int32_t length = static_cast<std::int32_t>(value.length());

        boundsCheck(offset, value.length() + sizeof(std::int32_t));

        putInt32(offset, length);
        memcpy (m_buffer + offset + sizeof(std::int32_t), value.c_str(), length);

        return static_cast<std::int32_t>(sizeof(std::int32_t)) + length;
    }

    std::int32_t putStringUtf8WithoutLength(util::index_t offset, const std::string& value)
    {
        std::int32_t length = static_cast<std::int32_t>(value.length());

        boundsCheck(offset, value.length());

        memcpy (m_buffer + offset, value.c_str(), length);
        return length;
    }


    inline util::index_t getCapacity() const
    {
        return m_length;
    }

private:
    std::uint8_t *m_buffer;
    util::index_t m_length;

    inline void boundsCheck(util::index_t index, util::index_t length) const
    {
        if (index + length > m_length)
            throw aeron::common::util::OutOfBoundsException(std::string("Index Out of Bounds. Index: ") + util::toString(index + length) + " Capacity: " + util::toString(m_length), SOURCEINFO);
    }
};

}}}

#endif