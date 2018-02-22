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
#ifndef INCLUDED_AERON_CONCURRENT_COUNTERS_MANAGER__
#define INCLUDED_AERON_CONCURRENT_COUNTERS_MANAGER__

#include <functional>
#include <cstdint>
#include <deque>
#include <memory>
#include <iostream>
#include <algorithm>

#include <util/Exceptions.h>
#include <util/StringUtil.h>
#include <util/Index.h>
#include <util/BitUtil.h>

#include "AtomicBuffer.h"
#include "CountersReader.h"

namespace aeron { namespace concurrent {

class CountersManager : public CountersReader
{
public:
    using clock_t = std::function<long long()>;

    inline CountersManager(const AtomicBuffer& metadataBuffer, const AtomicBuffer& valuesBuffer) :
        CountersReader(metadataBuffer, valuesBuffer)
    {
    }

    inline CountersManager(
        const AtomicBuffer& metadataBuffer,
        const AtomicBuffer& valuesBuffer,
        const clock_t& clock,
        long freeToReuseTimeoutMs) :
        CountersReader(metadataBuffer, valuesBuffer),
        m_clock(clock),
        m_freeToReuseTimeoutMs(freeToReuseTimeoutMs)
    {
    }

    template <typename F>
    std::int32_t allocate(
        const std::string& label,
        std::int32_t typeId,
        F&& keyFunc)
    {
        std::int32_t counterId = nextCounterId();

        if (label.length() > MAX_LABEL_LENGTH)
        {
            throw util::IllegalArgumentException("Label too long", SOURCEINFO);
        }

        checkCountersCapacity(counterId);

        const util::index_t recordOffset = metadataOffset(counterId);
        checkMetaDataCapacity(recordOffset);

        CounterMetaDataDefn& record =
            m_metadataBuffer.overlayStruct<CounterMetaDataDefn>(recordOffset);

        record.typeId = typeId;

        AtomicBuffer keyBuffer(m_metadataBuffer.buffer() + recordOffset + KEY_OFFSET, sizeof(CounterMetaDataDefn::key));
        keyFunc(keyBuffer);

        record.freeToReuseDeadline = NOT_FREE_TO_REUSE;

        m_metadataBuffer.putString(recordOffset + LABEL_LENGTH_OFFSET, label);
        m_metadataBuffer.putInt32Ordered(recordOffset, RECORD_ALLOCATED);

        return counterId;
    }

    std::int32_t allocate(
        std::int32_t typeId,
        const std::uint8_t *key,
        size_t keyLength,
        const std::string& label)
    {
        std::int32_t counterId = nextCounterId();

        if (label.length() > MAX_LABEL_LENGTH)
        {
            throw util::IllegalArgumentException("Label too long", SOURCEINFO);
        }

        checkCountersCapacity(counterId);

        const util::index_t recordOffset = metadataOffset(counterId);
        checkMetaDataCapacity(recordOffset);

        CounterMetaDataDefn& record =
            m_metadataBuffer.overlayStruct<CounterMetaDataDefn>(recordOffset);

        record.typeId = typeId;
        record.freeToReuseDeadline = NOT_FREE_TO_REUSE;

        if (nullptr != key && keyLength > 0)
        {
            const util::index_t maxLength = MAX_KEY_LENGTH;
            m_metadataBuffer.putBytes(
                recordOffset + KEY_OFFSET, key, std::min(maxLength, static_cast<util::index_t>(keyLength)));
        }

        m_metadataBuffer.putString(recordOffset + LABEL_LENGTH_OFFSET, label);
        m_metadataBuffer.putInt32Ordered(recordOffset, RECORD_ALLOCATED);

        return counterId;
    }

    inline std::int32_t allocate(const std::string& label)
    {
        return allocate(0, nullptr, 0, label);
    }

    inline void free(std::int32_t counterId)
    {
        const util::index_t recordOffset = metadataOffset(counterId);

        m_metadataBuffer.putInt64(recordOffset + FREE_TO_REUSE_DEADLINE_OFFSET, m_clock() + m_freeToReuseTimeoutMs);
        m_metadataBuffer.putInt32Ordered(recordOffset, RECORD_RECLAIMED);
        m_freeList.push_back(counterId);
    }

    inline void setCounterValue(std::int32_t counterId, std::int64_t value)
    {
        m_valuesBuffer.putInt64Ordered(counterOffset(counterId), value);
    }

private:
    std::deque<std::int32_t> m_freeList;
    clock_t m_clock = []() { return 0L; };
    const long m_freeToReuseTimeoutMs = 0;
    util::index_t m_highWaterMark = -1;

    inline std::int32_t nextCounterId()
    {
        const long long nowMs = m_clock();

        auto it = std::find_if(m_freeList.begin(), m_freeList.end(),
            [&](std::int32_t counterId)
            {
                return
                    nowMs >=
                        m_metadataBuffer.getInt64Volatile(metadataOffset(counterId) + FREE_TO_REUSE_DEADLINE_OFFSET);
            });

        if (it != m_freeList.end())
        {
            const std::int32_t counterId = *it;

            m_freeList.erase(it);

            m_valuesBuffer.putInt64Ordered(counterOffset(counterId), 0L);

            return counterId;
        }

        return ++m_highWaterMark;
    }

    inline void checkCountersCapacity(std::int32_t counterId)
    {
        if ((counterOffset(counterId) + COUNTER_LENGTH) > m_valuesBuffer.capacity())
        {
            throw util::IllegalArgumentException("Unable to allocated counter, values buffer is full", SOURCEINFO);
        }
    }

    inline void checkMetaDataCapacity(util::index_t recordOffset)
    {
        if ((recordOffset + METADATA_LENGTH) > m_metadataBuffer.capacity())
        {
            throw util::IllegalArgumentException("Unable to allocate counter, metadata buffer is full", SOURCEINFO);
        }
    }
};

}}

#endif
