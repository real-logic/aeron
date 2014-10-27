
#include <util/Exceptions.h>
#include <memory>

#include "CountersManager.h"

namespace aeron { namespace common { namespace concurrent {

CountersManager::CountersManager(const AtomicBuffer& labelsBuffer, const AtomicBuffer& countersBuffer)
    : m_countersBuffer(countersBuffer), m_labelsBuffer(labelsBuffer)
{
}

void CountersManager::forEach(const std::function<void(int, const std::string &)> &f)
{
    int labelsOffset = 0;
    int size;
    int id = 0;

    while ((size = m_labelsBuffer.getInt32(labelsOffset)) != 0)
    {
        if (size != UNREGISTERED_LABEL_SIZE)
        {
            std::string label = m_labelsBuffer.getStringUtf8(labelsOffset);
            f(id, label);
        }

        labelsOffset += LABEL_SIZE;
        id++;
    }
}

std::int32_t CountersManager::allocate(const std::string& label)
{
    std::int32_t ctrId = counterId();
    size_t labelsOffset = labelOffset(ctrId);

    if (label.length() > LABEL_SIZE - sizeof(std::int32_t))
        throw util::IllegalArgumentException("Label too long", SOURCEINFO);

    if ((counterOffset(ctrId) + COUNTER_SIZE) > m_countersBuffer.getCapacity())
        throw util::IllegalArgumentException("Unable to allocated counter, counter buffer is full", SOURCEINFO);

    if ((labelsOffset + LABEL_SIZE) > m_labelsBuffer.getCapacity())
        throw util::IllegalArgumentException("Unable to allocate counter, labels buffer is full", SOURCEINFO);

    m_labelsBuffer.putStringUtf8(labelsOffset, label);

    return ctrId;
}

AtomicCounter::ptr_t CountersManager::newCounter(const std::string &label)
{
    return std::make_shared<AtomicCounter>(m_countersBuffer, allocate(label), *this);
}

void CountersManager::free(std::int32_t counterId)
{
    m_labelsBuffer.putInt32(labelOffset(counterId), UNREGISTERED_LABEL_SIZE);
    m_countersBuffer.putInt64Ordered(counterOffset(counterId), 0L);
    m_freeList.push_back(counterId);
}

size_t CountersManager::counterOffset(std::int32_t counterId)
{
    return counterId * COUNTER_SIZE;
}

size_t CountersManager::labelOffset(std::int32_t counterId)
{
    return counterId * LABEL_SIZE;
}

std::int32_t CountersManager::counterId()
{
    if (m_freeList.empty())
        return m_highwaterMark++;

    std::int32_t id = m_freeList.front();
    m_freeList.pop_front();
    return id;
}

}}}