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

#ifndef INCLUDED_AERON_CONCURRENT_ATOMIC_BUFFER__
#define INCLUDED_AERON_CONCURRENT_ATOMIC_BUFFER__

#include <cstdint>
#include <string.h>
#include <string>
#include <array>
#include <util/Exceptions.h>
#include <util/StringUtil.h>
#include <util/Index.h>

#include <util/MacroUtil.h>

#include "Atomic64.h"

namespace aeron { namespace concurrent {

// note: Atomic Buffer does not own the memory it wraps.
class AtomicBuffer
{
public:
    AtomicBuffer() :
        m_buffer(nullptr), m_length(0)
    {
    }

    AtomicBuffer(std::uint8_t *buffer, util::index_t length) :
        m_buffer(buffer), m_length(length)
    {
#if !defined(DISABLE_BOUNDS_CHECKS)
        if (AERON_COND_EXPECT(length < 0, false))
        {
            throw aeron::util::OutOfBoundsException(
                aeron::util::strPrintf("Length Out of Bounds[%p]. Length: %d", this, length),
                SOURCEINFO);
        }
#endif
    }

    AtomicBuffer(std::uint8_t *buffer, util::index_t length, std::uint8_t initialValue) :
        m_buffer(buffer), m_length(length)
    {
#if !defined(DISABLE_BOUNDS_CHECKS)
        if (AERON_COND_EXPECT(length < 0, false))
        {
            throw aeron::util::OutOfBoundsException(
                aeron::util::strPrintf("Length Out of Bounds[%p]. Length: %d", this, length),
                SOURCEINFO);
        }
#endif

        setMemory(0, (size_t)length, initialValue);
    }

    template<size_t N>
    AtomicBuffer(std::array<std::uint8_t, N>& buffer)
    {
        wrap(buffer);
    }

    template<size_t N>
    AtomicBuffer(std::array<std::uint8_t, N>& buffer, std::uint8_t initialValue)
    {
        wrap(buffer);
        buffer.fill(initialValue);
    }

    AtomicBuffer(const AtomicBuffer& buffer) = default;

    AtomicBuffer(AtomicBuffer&& buffer) :
        m_buffer(buffer.m_buffer), m_length(buffer.m_length)
    {
    }

    AtomicBuffer& operator=(AtomicBuffer& buffer) = default;

    // this class does not own the memory. It simply overlays it.
    virtual ~AtomicBuffer() = default;

    inline void wrap(std::uint8_t* buffer, util::index_t length)
    {
#if !defined(DISABLE_BOUNDS_CHECKS)
        if (AERON_COND_EXPECT(length < 0, false))
        {
            throw aeron::util::OutOfBoundsException(
                aeron::util::strPrintf("Length Out of Bounds[%p]. Length: %d", this, length),
                SOURCEINFO);
        }
#endif

        m_buffer = buffer;
        m_length = length;
    }

    inline void wrap(const AtomicBuffer& buffer)
    {
        m_buffer = buffer.m_buffer;
        m_length = buffer.m_length;
    }

    template<size_t N>
    inline void wrap(std::array<std::uint8_t, N>& buffer)
    {
        static_assert(N <= std::numeric_limits<util::index_t>::max(), "Requires the array to have a size that fits in an index_t");
        m_buffer = buffer.data();
        m_length = static_cast<util::index_t>(N);
    }

    inline util::index_t capacity() const
    {
        return m_length;
    }

    inline void capacity(util::index_t length)
    {
#if !defined(DISABLE_BOUNDS_CHECKS)
        if (AERON_COND_EXPECT(length < 0, false))
        {
            throw aeron::util::OutOfBoundsException(
                aeron::util::strPrintf("Length Out of Bounds[%p]. Length: %d", this, length),
                SOURCEINFO);
        }
#endif

        m_length = length;
    }

    inline std::uint8_t *buffer() const
    {
        return m_buffer;
    }

    template <typename struct_t>
    struct_t& overlayStruct()
    {
        boundsCheck(0, sizeof(struct_t));
        return *reinterpret_cast<struct_t*>(m_buffer);
    }

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

    inline COND_MOCK_VIRTUAL void putInt64(util::index_t offset, std::int64_t v)
    {
        boundsCheck(offset, sizeof(std::int64_t));
        *reinterpret_cast<std::int64_t *>(m_buffer + offset) = v;
    }

    inline COND_MOCK_VIRTUAL std::int64_t getInt64(util::index_t offset) const
    {
        boundsCheck(offset, sizeof(std::int64_t));
        return *reinterpret_cast<std::int64_t *>(m_buffer + offset);
    }

    inline COND_MOCK_VIRTUAL void putInt32(util::index_t offset, std::int32_t v)
    {
        boundsCheck(offset, sizeof(std::int32_t));
        *reinterpret_cast<std::int32_t *>(m_buffer + offset) = v;
    }

    inline COND_MOCK_VIRTUAL std::int32_t getInt32(util::index_t offset) const
    {
        boundsCheck(offset, sizeof(std::int32_t));
        return *reinterpret_cast<std::int32_t *>(m_buffer + offset);
    }

    inline COND_MOCK_VIRTUAL std::int16_t getInt16(util::index_t offset)
    {
        boundsCheck(offset, sizeof(std::int16_t));
        return *reinterpret_cast<std::int16_t *>(m_buffer + offset);
    }

    inline COND_MOCK_VIRTUAL void putInt16(util::index_t offset, std::int16_t v)
    {
        boundsCheck(offset, sizeof(std::int16_t));
        *reinterpret_cast<std::int16_t *>(m_buffer + offset) = v;
    }

    inline COND_MOCK_VIRTUAL std::uint16_t getUInt16(util::index_t offset)
    {
        boundsCheck(offset, sizeof(std::uint16_t));
        return *reinterpret_cast<std::uint16_t *>(m_buffer + offset);
    }

    inline COND_MOCK_VIRTUAL void putUInt16(util::index_t offset, std::uint16_t v)
    {
        boundsCheck(offset, sizeof(std::uint16_t));
        *reinterpret_cast<std::uint16_t *>(m_buffer + offset) = v;
    }

    inline std::uint8_t getUInt8(util::index_t offset)
    {
        boundsCheck(offset, sizeof(std::uint8_t));
        return *reinterpret_cast<std::uint8_t *>(m_buffer + offset);
    }

    inline COND_MOCK_VIRTUAL void putUInt8(util::index_t offset, std::uint8_t v)
    {
        boundsCheck(offset, sizeof(std::uint8_t));
        *reinterpret_cast<std::uint8_t *>(m_buffer + offset) = v;
    }

    inline COND_MOCK_VIRTUAL void putInt64Ordered(util::index_t offset, std::int64_t v)
    {
        boundsCheck(offset, sizeof(std::int64_t));
        atomic::putInt64Ordered((volatile std::int64_t*)(m_buffer + offset), v);
    }

    inline COND_MOCK_VIRTUAL std::int64_t getInt64Volatile(util::index_t offset) const
    {
        boundsCheck(offset, sizeof(std::int64_t));
        return atomic::getInt64Volatile((volatile std::int64_t*)(m_buffer + offset));
    }

    inline COND_MOCK_VIRTUAL void putInt32Ordered(util::index_t offset, std::int32_t v)
    {
        boundsCheck(offset, sizeof(std::int32_t));
        atomic::putInt32Ordered((volatile std::int32_t*)(m_buffer + offset), v);
    }

    inline COND_MOCK_VIRTUAL std::int32_t getInt32Volatile(util::index_t offset) const
    {
        boundsCheck(offset, sizeof(std::int32_t));
        return atomic::getInt32Volatile((volatile std::int32_t*)(m_buffer + offset));
    }

    inline void putInt64Atomic(util::index_t offset, std::int64_t v)
    {
        boundsCheck(offset, sizeof(std::int64_t));
        atomic::putInt64Atomic((volatile std::int64_t*)(m_buffer + offset), v);
    }

    inline void putInt32Atomic(util::index_t offset, std::int32_t v)
    {
        boundsCheck(offset, sizeof(std::int32_t));
        atomic::putInt32Atomic((volatile std::int32_t*)(m_buffer + offset), v);
    }

    // increment from single thread
    inline void addInt64Ordered(util::index_t offset, std::int64_t increment)
    {
        boundsCheck(offset, sizeof(std::int64_t));

        const std::int64_t value = getInt64(offset);
        atomic::putInt64Ordered((volatile std::int64_t*)(m_buffer + offset), value + increment);
    }

    inline bool compareAndSetInt64(util::index_t offset, std::int64_t expectedValue, std::int64_t updateValue)
    {
        boundsCheck(offset, sizeof(std::int64_t));
        std::int64_t original = atomic::cmpxchg((volatile std::int64_t*)(m_buffer + offset), expectedValue, updateValue);
        return (original == expectedValue);
    }

    // increment from multiple threads
    inline COND_MOCK_VIRTUAL std::int64_t getAndAddInt64(util::index_t offset, std::int64_t delta)
    {
        boundsCheck(offset, sizeof(std::int64_t));
        return atomic::getAndAddInt64((volatile std::int64_t*)(m_buffer + offset), delta);
    }

    // increment from single thread
    inline void addInt32Ordered(util::index_t offset, std::int32_t increment)
    {
        boundsCheck(offset, sizeof(std::int32_t));

        const std::int32_t value = getInt32(offset);
        atomic::putInt32Ordered((volatile std::int32_t*)(m_buffer + offset), value + increment);
    }

    inline bool compareAndSetInt32(util::index_t offset, std::int32_t expectedValue, std::int32_t updateValue)
    {
        boundsCheck(offset, sizeof(std::int32_t));
        std::int32_t original = atomic::cmpxchg((volatile std::int32_t*)(m_buffer + offset), expectedValue, updateValue);
        return (original == expectedValue);
    }

    // increment from multiple threads
    inline COND_MOCK_VIRTUAL std::int32_t getAndAddInt32(util::index_t offset, std::int32_t delta)
    {
        boundsCheck(offset, sizeof(std::int32_t));
        return atomic::getAndAddInt32((volatile std::int32_t*)(m_buffer + offset), delta);
    }

    inline COND_MOCK_VIRTUAL void putBytes(util::index_t index, const concurrent::AtomicBuffer& srcBuffer, util::index_t srcIndex, util::index_t length)
    {
        boundsCheck(index, length);
        srcBuffer.boundsCheck(srcIndex, length);
        ::memcpy(m_buffer + index, srcBuffer.m_buffer + srcIndex, length);
    }

    inline COND_MOCK_VIRTUAL void putBytes(util::index_t index, const std::uint8_t *srcBuffer, util::index_t length)
    {
        boundsCheck(index, length);
        ::memcpy(m_buffer + index, srcBuffer, length);
    }

    inline void getBytes(util::index_t index, std::uint8_t *dst, util::index_t length) const
    {
        boundsCheck(index, length);
        ::memcpy(dst, m_buffer + index, length);
    }

    inline void setMemory(util::index_t offset , size_t length, std::uint8_t value)
    {
        boundsCheck(offset, length);
        ::memset(m_buffer + offset, value, length);
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

    inline std::int32_t getStringUtf8Length(util::index_t offset) const
    {
        return getInt32(offset);
    }

    std::int32_t putStringUtf8(util::index_t offset, const std::string& value)
    {
        std::int32_t length = static_cast<std::int32_t>(value.length());

        boundsCheck(offset, value.length() + sizeof(std::int32_t));

        putInt32(offset, length);
        ::memcpy(m_buffer + offset + sizeof(std::int32_t), value.c_str(), value.length());

        return static_cast<std::int32_t>(sizeof(std::int32_t)) + length;
    }

    std::int32_t COND_MOCK_VIRTUAL putStringUtf8WithoutLength(util::index_t offset, const std::string& value)
    {
        boundsCheck(offset, value.length());
        ::memcpy(m_buffer + offset, value.c_str(), value.length());
        return static_cast<std::int32_t>(value.length());
    }

    inline void boundsCheck(util::index_t index, std::uint64_t length) const
    {
#if !defined(DISABLE_BOUNDS_CHECKS)
        // This check disallows negative indices and makes sure the
        if (AERON_COND_EXPECT(index < 0 || (std::uint64_t)m_length - index < length, false))
        {
            throw aeron::util::OutOfBoundsException(
                aeron::util::strPrintf("Index Out of Bounds[%p]. Index: %d + %d Capacity: %d", this, index, length, m_length),
                SOURCEINFO);
        }
#endif
    }

private:
    // The internal length type used by the atomic buffer
    typedef std::uint32_t length_t;

    std::uint8_t *m_buffer;
    length_t m_length;
};

}}

#endif
