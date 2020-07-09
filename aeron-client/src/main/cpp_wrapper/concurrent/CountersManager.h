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
#ifndef AERON_CONCURRENT_COUNTERS_MANAGER_H
#define AERON_CONCURRENT_COUNTERS_MANAGER_H

#include <functional>
#include <cstdint>
#include <deque>
#include <memory>
#include <iostream>
#include <algorithm>

#include "util/Exceptions.h"
#include "util/StringUtil.h"
#include "util/Index.h"
#include "util/BitUtil.h"

#include "AtomicBuffer.h"
#include "CountersReader.h"

namespace aeron { namespace concurrent {

class CountersManager : public CountersReader
{
public:
    inline CountersManager(aeron_counters_manager_t *manager, aeron_counters_reader_t *reader) :
        CountersReader(reader), m_countersManager(manager)
    {
    }

    virtual ~CountersManager()
    {
        aeron_counters_manager_close(m_countersManager);
    }

    template <typename F>
    std::int32_t allocate(
        const std::string& label,
        std::int32_t typeId,
        F&& keyFunc)
    {
        int32_t counterId = aeron_counters_manager_allocate(
            m_countersManager, typeId, NULL, 0, label.c_str(), label.length());

        if (counterId < 0)
        {
            throw util::IllegalArgumentException(aeron_errmsg(), SOURCEINFO);
        }
    
        // Less than ideal, but not sure how we can handle C++ universal references any other way...
        aeron_counter_metadata_descriptor_t *metadata = reinterpret_cast<aeron_counter_metadata_descriptor_t *>(
            metaDataBuffer().buffer() + metadataOffset(counterId));
        keyFunc(AtomicBuffer(metadata->key, sizeof(metadata->key)));

        return counterId;
    }

    std::int32_t allocate(
        std::int32_t typeId,
        const std::uint8_t *key,
        size_t keyLength,
        const std::string& label)
    {
        int32_t counterId = aeron_counters_manager_allocate(
            m_countersManager, typeId, key, keyLength, label.c_str(), label.length());

        if (counterId < 0)
        {
            throw util::IllegalArgumentException(aeron_errmsg(), SOURCEINFO);
        }

        return counterId;
    }

    inline std::int32_t allocate(const std::string& label)
    {
        return allocate(0, nullptr, 0, label);
    }

    inline void free(std::int32_t counterId)
    {
        aeron_counters_manager_free(m_countersManager, counterId);
    }

    inline void setCounterValue(std::int32_t counterId, std::int64_t value)
    {
        int64_t *addr = aeron_counters_manager_addr(m_countersManager, counterId);
        AERON_PUT_ORDERED(*addr, value);
    }

private:
    aeron_counters_manager_t *m_countersManager;
};

}}

#endif
