
#include "AtomicCounter.h"
#include "CountersManager.h"

namespace aeron { namespace common { namespace concurrent {

AtomicCounter::AtomicCounter(const AtomicBuffer buffer, std::int32_t counterId, CountersManager& countersManager)
        : m_buffer(buffer)
        , m_counterId(counterId)
        , m_countersManager(countersManager)
        , m_offset (countersManager.counterOffset(counterId))
{
    m_buffer.putInt64(m_offset, 0);
}

AtomicCounter::~AtomicCounter()
{
    m_countersManager.free(m_counterId);
}

}}}

