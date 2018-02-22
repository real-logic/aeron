/*
 * Copyright 2014-2018 Real Logic Ltd.
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

#ifndef AERON_COUNTER_H
#define AERON_COUNTER_H

#include <cstdint>
#include <memory>
#include <atomic>

#include <util/Index.h>
#include "concurrent/AtomicCounter.h"
#include "concurrent/CountersReader.h"

namespace aeron {

using namespace aeron::concurrent;

class ClientConductor;

class Counter : public AtomicCounter
{
public:
    /// @cond HIDDEN_SYMBOLS
    Counter(
        ClientConductor* clientConductor,
        AtomicBuffer& buffer,
        std::int64_t registrationId,
        std::int32_t counterId);
    /// @endcond

    Counter(CountersReader& countersReader, std::int64_t registrationId, std::int32_t counterId) :
        AtomicCounter(countersReader.valuesBuffer(), counterId),
        m_clientConductor(nullptr),
        m_registrationId(registrationId)
    {
    }

    virtual ~Counter();

    inline std::int64_t registrationId() const
    {
        return m_registrationId;
    }

    std::int32_t state() const;
    std::string label() const;

    inline bool isClosed() const
    {
        return std::atomic_load_explicit(&m_isClosed, std::memory_order_acquire);
    }

    /// @cond HIDDEN_SYMBOLS
    inline void close()
    {
        std::atomic_store_explicit(&m_isClosed, true, std::memory_order_release);
    }
    /// @endcond

private:
    ClientConductor* m_clientConductor;
    std::int64_t m_registrationId;
    std::atomic<bool> m_isClosed = { false };
};

}
#endif //AERON_COUNTER_H
