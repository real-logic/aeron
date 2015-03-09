/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
#ifndef INCLUDED_AERON_CONCURRENT_COUNTERS_MANAGER__
#define INCLUDED_AERON_CONCURRENT_COUNTERS_MANAGER__

#include <functional>
#include <cstdint>
#include <deque>

#include <util/Index.h>

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

    util::index_t counterOffset(std::int32_t counterId);
    util::index_t labelOffset(std::int32_t counterId);

    static const util::index_t LABEL_SIZE = 1024;
    static const util::index_t COUNTER_SIZE = 64;  // cache line size

private:
    static const util::index_t UNREGISTERED_LABEL_SIZE = -1;

    util::index_t m_highwaterMark = 0;

    std::deque<std::int32_t> m_freeList;

    AtomicBuffer m_countersBuffer;
    AtomicBuffer m_labelsBuffer;

    std::int32_t counterId ();
};

}}}

#endif