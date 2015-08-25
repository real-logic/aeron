//
// Created by Michael Barker on 18/08/15.
//

#ifndef INCLUDED_AERON_DRIVER_ONETOONECONCURRENTARRAYQUEUE_
#define INCLUDED_AERON_DRIVER_ONETOONECONCURRENTARRAYQUEUE_

#include <cstdint>
#include <util/BitUtil.h>
#include <concurrent/Atomic64.h>

namespace aeron { namespace driver { namespace concurrent {

using namespace aeron::util;
using namespace aeron::concurrent::atomic;

template<typename T>
class OneToOneConcurrentArrayQueue
{
public:
    OneToOneConcurrentArrayQueue(std::int32_t requestedCapacity) :
        m_head(0), m_tail(0), m_capacity(BitUtil::findNextPowerOfTwo(requestedCapacity))
    {
        m_mask = m_capacity - 1;
        m_buffer = new volatile T*[m_capacity];
    }

    ~OneToOneConcurrentArrayQueue()
    {
        delete[] m_buffer;
    }

    bool offer(T* t)
    {
        if (nullptr == t)
        {
            return false;
        }

        std::int64_t currentTail = getInt64Volatile(&m_tail);

        volatile T** source = &m_buffer[currentTail];
        volatile T* ptr = getValueVolatile(source);
        if (nullptr == ptr)
        {
            putValueOrdered(source, t);
            putInt64Ordered(&m_tail, currentTail + 1);

            return true;
        }

        return false;
    }

    T* poll()
    {
        std::int64_t currentHead = getInt64Volatile(&m_head);
        int index = (int) (currentHead & m_mask);
        volatile T** source = &m_buffer[index];
        volatile T* t = getValueVolatile(source);

        if (nullptr != t)
        {
            putValueOrdered(source, (T*) nullptr);
            putInt64Ordered(&m_head, currentHead + 1);
        }

        return const_cast<T*>(t);
    }

private:
    // TODO: Alignment?
    __declspec(align(8)) volatile T** m_buffer;
    __declspec(align(8)) std::int64_t m_head;
    __declspec(align(8)) std::int64_t m_mask;
    __declspec(align(8)) std::int64_t m_tail;
    __declspec(align(4)) std::int32_t m_capacity;
};

}}};

#endif //AERON_ONETOONECONCURRENTARRAYQUEUE_H
