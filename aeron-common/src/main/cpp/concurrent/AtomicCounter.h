#ifndef INCLUDED_AERON_CONCURRENT_ATOMIC_COUNTER__
#define INCLUDED_AERON_CONCURRENT_ATOMIC_COUNTER__

#include <cstdint>
#include <mintomic/mintomic.h>
#include "AtomicBuffer.h"


namespace aeron { namespace common { namespace concurrent {

class CountersManager;

class AtomicCounter
{
public:
    AtomicCounter(const AtomicBuffer buffer, std::int32_t counterId, CountersManager& countersManager);

    void increment()
    {
        m_buffer.getAndAddInt64(m_offset, 1);
    }

    void orderedIncrement()
    {
        m_buffer.addInt64Ordered(m_offset, 1);
    }

    void set(std::int64_t value)
    {
        m_buffer.putInt64Atomic(m_offset, value);
    }

    void setOrdered(long value)
    {
        m_buffer.putInt64Atomic(m_offset, value);
    }

    void addOrdered(long increment)
    {
        m_buffer.addInt64Ordered(m_offset, increment);
    }

    std::int64_t get()
    {
        return m_buffer.getInt64Atomic(m_offset);
    }

    void close();

private:
    AtomicBuffer m_buffer;
    std::int32_t m_counterId;
    CountersManager& m_countersManager;
    size_t m_offset;
};

}}}

#endif


