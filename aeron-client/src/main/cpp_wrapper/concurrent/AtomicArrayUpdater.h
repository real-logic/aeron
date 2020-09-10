/*
 * Copyright 2014-2020 Real Logic Limited.
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

namespace aeron { namespace util
{

template<typename E>
std::pair<E *, std::size_t> addToArray(E *oldArray, std::size_t oldLength, E element)
{
    std::size_t newLength = oldLength + 1;
    E *newArray = new E[newLength];

    for (size_t i = 0; i < oldLength; i++)
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

    for (size_t i = 0, j = 0; i < oldLength; i++)
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
        do
        {
            std::int64_t changeNumber = m_endChange.load(std::memory_order_acquire);

            E *array = m_array.first;
            std::size_t length = m_array.second;

            aeron::concurrent::atomic::acquire();

            if (changeNumber == m_beginChange.load(std::memory_order_acquire))
            {
                return {array, length};
            }
        }
        while (true);
    }

    inline void store(E *array, std::size_t length)
    {
        std::int64_t changeNumber = m_beginChange + 1;

        m_beginChange.store(changeNumber, std::memory_order_release);

        m_array.first = array;
        m_array.second = length;

        m_endChange.store(changeNumber, std::memory_order_release);
    }

    std::pair<E *, std::size_t> addElement(E element)
    {
        std::pair<E*, std::size_t> oldArray = load();
        std::pair<E*, std::size_t> newArray = aeron::util::addToArray(oldArray.first, oldArray.second, element);

        store(newArray.first, newArray.second);

        return oldArray;
    }

    template<typename F>
    std::pair<E *, std::size_t> removeElement(F &&func)
    {
        std::pair<E*, std::size_t> oldArray = load();
        const std::size_t length = oldArray.second;

        for (std::size_t i = 0; i < length; i++)
        {
            if (func(oldArray.first[i]))
            {
                std::pair<E *, std::size_t> newArray = aeron::util::removeFromArray(oldArray.first, length, i);

                store(newArray.first, newArray.second);

                return { oldArray.first, i };
            }
        }

        return {nullptr, 0};
    }

private:
    std::atomic<std::int64_t> m_beginChange = { -1 };
    std::atomic<std::int64_t> m_endChange = { -1 };
    std::pair<E *, std::size_t> m_array = { nullptr, 0 };
};

}}

#endif //AERON_ATOMIC_ARRAY_UPDATER_H
