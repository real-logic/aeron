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

int CountersManager::allocate(const std::string &label)
{
    return 0;
}

AtomicCounter CountersManager::newCounter(const std::string &label)
{
    std::int32_t counterId = 0;

    return AtomicCounter(m_countersBuffer, counterId, *this);
}

void CountersManager::free(int counterId)
{

}

size_t CountersManager::counterOffset(std::int32_t counterId)
{
    return counterId * COUNTER_SIZE;
}

}}}