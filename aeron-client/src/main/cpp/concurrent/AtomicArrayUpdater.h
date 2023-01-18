/*
 * Copyright 2014-2023 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef AERON_ATOMIC_ARRAY_UPDATER_H
#define AERON_ATOMIC_ARRAY_UPDATER_H

#include <cstddef>
#include <utility>
#include <atomic>

#include "concurrent/Atomic64.h"

namespace aeron
{

namespace util
{

template<typename E>
std::pair<E *, std::size_t> addToArray(E *oldArray, std::size_t oldLength, E element)
{
    std::size_t newLength = oldLength + 1;
    E *newArray = new E[newLength];

    for (std::size_t i = 0; i < oldLength; i++)
    {
        newArray[i] = oldArray[i];
    }

    newArray[oldLength] = element;

    return { newArray, newLength };
}

template<typename E>
std::pair<E *, std::size_t> removeFromArray(E *oldArray, std::size_t oldLength, std::size_t index)
{
    std::size_t newLength = oldLength - 1;
    E *newArray = new E[newLength];

    for (std::size_t i = 0, j = 0; i < oldLength; i++)
    {
        if (i != index)
        {
            newArray[j++] = oldArray[i];
        }
    }

    return { newArray, newLength };
}

}

namespace concurrent
{

template<typename E>
class AtomicArrayUpdater
{
public:
    AtomicArrayUpdater() = default;
    ~AtomicArrayUpdater() = default;

    inline std::pair<E *, std::size_t> load() const
    {
        while (true)
        {
            std::int64_t changeNumber = m_endChange.load(std::memory_order_acquire);

            E *array = m_array;
            std::size_t length = m_length;
            aeron::concurrent::atomic::acquire();

            if (changeNumber == m_beginChange.load(std::memory_order_acquire))
            {
                return { array, length };
            }
        }
    }

    inline std::pair<E *, std::size_t> store(E *newArray, std::size_t length)
    {
        E *oldArray = m_array;
        std::size_t oldLength = m_length;
        std::int64_t changeNumber = m_beginChange.load(std::memory_order_relaxed) + 1;

        m_beginChange.store(changeNumber, std::memory_order_release);

        aeron::concurrent::atomic::release();
        m_array = newArray;
        m_length = length;

        m_endChange.store(changeNumber, std::memory_order_release);

        return { oldArray, oldLength };
    }

    std::pair<E *, std::size_t> addElement(E element)
    {
        std::pair<E *, std::size_t> newArray = aeron::util::addToArray(m_array, m_length, element);

        return store(newArray.first, newArray.second);
    }

    template<typename F>
    std::pair<E *, std::size_t> removeElement(F &&func)
    {
        for (std::size_t i = 0, length = m_length; i < length; i++)
        {
            if (func(m_array[i]))
            {
                std::pair<E *, std::size_t> newArray = aeron::util::removeFromArray(m_array, length, i);
                std::pair<E *, std::size_t> oldArray = store(newArray.first, newArray.second);

                return { oldArray.first, i };
            }
        }

        return { nullptr, 0 };
    }

private:
    std::atomic<std::int64_t> m_beginChange = { -1 };
    E *m_array = nullptr;
    std::size_t m_length = 0;
    std::atomic<std::int64_t> m_endChange = { -1 };
};

}}

#endif //AERON_ATOMIC_ARRAY_UPDATER_H
