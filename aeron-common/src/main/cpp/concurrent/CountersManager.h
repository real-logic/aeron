#ifndef INCLUDED_AERON_CONCURRENT_COUNTERS_MANAGER__
#define INCLUDED_AERON_CONCURRENT_COUNTERS_MANAGER__

#include <functional>
#include <cstdint>
#include <deque>

#include "AtomicBuffer.h"
#include "AtomicCounter.h"

namespace aeron { namespace common { namespace concurrent {

class CountersManager
{
public:
    CountersManager(const AtomicBuffer& labelsBuffer, const AtomicBuffer& countersBuffer);

    void forEach (const std::function<void(int, const std::string&)>& f);

    std::int32_t allocate(const std::string& label);
    AtomicCounter::ptr_t newCounter(const std::string& label);
    void free(std::int32_t counterId);

    size_t counterOffset(std::int32_t counterId);
    size_t labelOffset(std::int32_t counterId);

    static const size_t LABEL_SIZE = 1024;
    static const size_t COUNTER_SIZE = 64;  // cache line size

private:
    static const std::int32_t UNREGISTERED_LABEL_SIZE = -1;

    std::int32_t m_highwaterMark = 0;

    std::deque<std::int32_t> m_freeList;

    AtomicBuffer m_countersBuffer;
    AtomicBuffer m_labelsBuffer;

    std::int32_t counterId ();
};

}}}

#endif