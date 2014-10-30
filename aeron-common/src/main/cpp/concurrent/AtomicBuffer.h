
#ifndef INCLUDED_AERON_CONCURRENT_ATOMIC_BUFFER__
#define INCLUDED_AERON_CONCURRENT_ATOMIC_BUFFER__

#include <cstdint>
#include <string.h>
#include <string>
#include <mintomic/mintomic.h>

#include <util/Exceptions.h>
#include <util/StringUtil.h>

namespace aeron { namespace common { namespace concurrent {


// note: Atomic Buffer does not own the memory it wraps.
class AtomicBuffer
{
public:
    AtomicBuffer(std::uint8_t *buffer, size_t length);

    template <typename struct_t>
    struct_t& overlayStruct (size_t offset)
    {
        boundsCheck(offset, sizeof(struct_t));
        return *reinterpret_cast<struct_t*>(m_buffer + offset);
    }

    template <typename struct_t>
    const struct_t& overlayStruct (size_t offset) const
    {
        boundsCheck(offset, sizeof(struct_t));
        return *reinterpret_cast<struct_t*>(m_buffer + offset);
    }

    inline void putInt64(size_t offset, std::int64_t v)
    {
        boundsCheck(offset, sizeof(std::int64_t));
        *reinterpret_cast<std::int64_t *>(m_buffer + offset) = v;
    }

    inline std::int64_t getInt64(size_t offset) const
    {
        boundsCheck(offset, sizeof(std::int64_t));
        return *reinterpret_cast<std::int64_t *>(m_buffer + offset);
    }

    inline void putInt32(size_t offset, std::int32_t v)
    {
        boundsCheck(offset, sizeof(std::int32_t));
        *reinterpret_cast<std::int32_t *>(m_buffer + offset) = v;
    }

    inline std::int32_t getInt32(size_t offset) const
    {
        boundsCheck(offset, sizeof(std::int32_t));
        return *reinterpret_cast<std::int32_t *>(m_buffer + offset);
    }

    inline void putInt64Ordered(size_t offset, std::int64_t v)
    {
        boundsCheck(offset, sizeof(std::int64_t));
        mint_thread_fence_release();
        mint_store_64_relaxed((mint_atomic64_t*)(m_buffer + offset), v);
    }

    inline std::int64_t getInt64Ordered(size_t offset) const
    {
        boundsCheck(offset, sizeof(std::int64_t));
        std::int64_t v = mint_load_64_relaxed((mint_atomic64_t*)(m_buffer + offset));
        mint_thread_fence_acquire();
        return v;
    }

    inline void putInt32Ordered(size_t offset, std::int32_t v)
    {
        boundsCheck(offset, sizeof(std::int32_t));
        mint_thread_fence_release();
        mint_store_32_relaxed((mint_atomic32_t*)(m_buffer + offset), v);
    }

    inline std::int32_t getInt32Ordered(size_t offset) const
    {
        boundsCheck(offset, sizeof(std::int32_t));
        std::int32_t v = mint_load_32_relaxed((mint_atomic32_t*)(m_buffer + offset));
        mint_thread_fence_acquire();
        return v;
    }

    inline void putInt64Atomic(size_t offset, std::int64_t v)
    {
        boundsCheck(offset, sizeof(std::int64_t));
        mint_thread_fence_release();
        mint_store_64_relaxed((mint_atomic64_t*)(m_buffer + offset), v);
        mint_thread_fence_seq_cst();
    }

    inline std::int64_t getInt64Atomic(size_t offset) const
    {
        boundsCheck(offset, sizeof(std::int64_t));
        mint_thread_fence_seq_cst();
        std::int64_t v = mint_load_64_relaxed((mint_atomic64_t*)(m_buffer + offset));
        mint_thread_fence_acquire();
        return v;
    }

    inline void putInt32Atomic(size_t offset, std::int32_t v)
    {
        boundsCheck(offset, sizeof(std::int32_t));
        mint_thread_fence_release();
        mint_store_32_relaxed((mint_atomic32_t*)(m_buffer + offset), v);
        mint_thread_fence_seq_cst();
    }

    inline std::int32_t getInt32Atomic(size_t offset) const
    {
        boundsCheck(offset, sizeof(std::int32_t));
        mint_thread_fence_seq_cst();
        std::int32_t v = mint_load_32_relaxed((mint_atomic32_t*)(m_buffer + offset));
        mint_thread_fence_acquire();
        return v;
    }

    inline void addInt64Ordered(size_t offset, std::int64_t increment)
    {
        boundsCheck(offset, sizeof(std::int64_t));
        mint_fetch_add_64_relaxed((mint_atomic64_t*)(m_buffer + offset), increment);
    }

    inline bool compareAndSetInt64(size_t offset, std::int64_t expectedValue, std::int64_t updateValue)
    {
        boundsCheck(offset, sizeof(std::int64_t));
        mint_compare_exchange_strong_64_relaxed((mint_atomic64_t*)(m_buffer + offset), expectedValue, updateValue);
        return true; // always works??
    }

    inline std::int64_t getAndSetInt64(size_t offset, std::int64_t value) const
    {
        // TODO: no idea how to implement this with mintomic....
        return 0;
    }

    inline std::int64_t getAndAddInt64(size_t offset, std::int64_t delta) const
    {
        boundsCheck(offset, sizeof(std::int64_t));
        return mint_fetch_add_64_relaxed((mint_atomic64_t*)(m_buffer + offset), delta);
    }

    inline void addInt32Ordered(size_t offset, std::int32_t increment)
    {
        boundsCheck(offset, sizeof(std::int32_t));
        mint_fetch_add_32_relaxed((mint_atomic32_t*)(m_buffer + offset), increment);
    }

    inline bool compareAndSetInt32(size_t offset, std::int32_t expectedValue, std::int32_t updateValue)
    {
        boundsCheck(offset, sizeof(std::int32_t));
        mint_compare_exchange_strong_32_relaxed((mint_atomic32_t*)(m_buffer + offset), expectedValue, updateValue);
        return true; // always works??
    }

    inline std::int32_t getAndSetInt32(size_t offset, std::int32_t value) const
    {
        // TODO: no idea how to implement this with mintomic....
        return 0;
    }

    inline std::int32_t getAndAddInt32(size_t offset, std::int32_t delta) const
    {
        boundsCheck(offset, sizeof(std::int32_t));
        return mint_fetch_add_32_relaxed((mint_atomic32_t*)(m_buffer + offset), delta);
    }

    inline void setMemory(size_t offset , size_t length, std::uint8_t value)
    {
        boundsCheck(offset, length);
        memset(m_buffer, value, length);
    }

    // Note: I am assuming that std::string is utf8 encoded
    // TODO: add std::wstring support
    inline std::string getStringUtf8(size_t offset) const
    {
        std::int32_t length = getInt32(offset);
        return getStringUtf8(offset, (size_t)length);
    }

    inline std::string getStringUtf8(size_t offset, size_t length) const
    {
        boundsCheck(offset, length);
        return std::string(m_buffer + offset + sizeof(std::int32_t), m_buffer + offset + sizeof(std::int32_t) + length);
    }

    std::int32_t putStringUtf8(size_t offset, const std::string& value)
    {
        std::int32_t length = static_cast<std::int32_t>(value.length());

        boundsCheck(offset, value.length() + sizeof(std::int32_t));

        putInt32(offset, length);
        memcpy (m_buffer + offset + sizeof(std::int32_t), value.c_str(), length);

        return static_cast<std::int32_t>(sizeof(std::int32_t)) + length;
    }

    inline size_t getCapacity() const
    {
        return m_length;
    }

private:
    std::uint8_t *m_buffer;
    size_t m_length;

    inline void boundsCheck(size_t index, size_t length) const
    {
        if (index + length > m_length)
            throw aeron::common::util::OutOfBoundsException(std::string("Index Out of Bounds. Index: ") + util::toString(index + length) + " Capacity: " + util::toString(m_length), SOURCEINFO);
    }
};

}}}

#endif