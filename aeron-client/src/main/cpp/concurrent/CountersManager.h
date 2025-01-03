/*
 * Copyright 2014-2025 Real Logic Limited.
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

#include <deque>
#include <memory>
#include <iostream>
#include <algorithm>

#include "concurrent/CountersReader.h"

namespace aeron { namespace concurrent {

class CountersManager : public CountersReader
{
public:
    using clock_t = std::function<long long()>;

    inline CountersManager(const AtomicBuffer &metadataBuffer, const AtomicBuffer &valuesBuffer) :
        CountersReader(metadataBuffer, valuesBuffer)
    {
        if (metadataBuffer.capacity() < (valuesBuffer.capacity() * (METADATA_LENGTH / COUNTER_LENGTH)))
        {
            throw util::IllegalArgumentException("metadata buffer is too small", SOURCEINFO);
        }
    }

    inline CountersManager(
        const AtomicBuffer &metadataBuffer,
        const AtomicBuffer &valuesBuffer,
        const clock_t &clock,
        long freeToReuseTimeoutMs) :
        CountersReader(metadataBuffer, valuesBuffer),
        m_clock(clock),
        m_freeToReuseTimeoutMs(freeToReuseTimeoutMs)
    {
    }

    template <typename F>
    inline std::int32_t allocate(const std::string &label, std::int32_t typeId, F &&keyFunc)
    {
        if (label.length() > MAX_LABEL_LENGTH)
        {
            throw util::IllegalArgumentException("label too long", SOURCEINFO);
        }

        std::int32_t counterId = nextCounterId();
        const util::index_t recordOffset = metadataOffset(counterId);
        auto &record = m_metadataBuffer.overlayStruct<CounterMetaDataDefn>(recordOffset);

        record.typeId = typeId;

        AtomicBuffer keyBuffer(
            m_metadataBuffer.buffer() + recordOffset + KEY_OFFSET, sizeof(CounterMetaDataDefn::key));
        keyFunc(keyBuffer);

        record.freeToReuseDeadline = NOT_FREE_TO_REUSE;

        m_metadataBuffer.putString(recordOffset + LABEL_LENGTH_OFFSET, label);
        m_metadataBuffer.putInt32Ordered(recordOffset, RECORD_ALLOCATED);

        return counterId;
    }

    inline std::int32_t allocate(
        std::int32_t typeId, const std::uint8_t *key, std::size_t keyLength, const std::string &label)
    {
        if (label.length() > MAX_LABEL_LENGTH)
        {
            throw util::IllegalArgumentException("Label too long", SOURCEINFO);
        }

        std::int32_t counterId = nextCounterId();
        const util::index_t recordOffset = metadataOffset(counterId);
        auto &record = m_metadataBuffer.overlayStruct<CounterMetaDataDefn>(recordOffset);

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

    inline std::int32_t allocate(const std::string &label)
    {
        return allocate(0, nullptr, 0, label);
    }

    inline void free(std::int32_t counterId)
    {
        validateCounterId(counterId);
        const util::index_t recordOffset = metadataOffset(counterId);

        m_metadataBuffer.putInt64(recordOffset + FREE_FOR_REUSE_DEADLINE_OFFSET, m_clock() + m_freeToReuseTimeoutMs);
        m_metadataBuffer.setMemory(recordOffset + KEY_OFFSET, sizeof(CounterMetaDataDefn::key), UINT8_C(0));
        m_metadataBuffer.putInt32Ordered(recordOffset, RECORD_RECLAIMED);
        m_freeList.push_back(counterId);
    }

    inline void setCounterValue(std::int32_t counterId, std::int64_t value)
    {
        validateCounterId(counterId);
        m_valuesBuffer.putInt64Ordered(counterOffset(counterId), value);
    }

    inline void setCounterRegistrationId(std::int32_t counterId, std::int64_t registrationId)
    {
        validateCounterId(counterId);
        m_valuesBuffer.putInt64Ordered(counterOffset(counterId) + REGISTRATION_ID_OFFSET, registrationId);
    }

    inline void setCounterOwnerId(std::int32_t counterId, std::int64_t ownerId)
    {
        validateCounterId(counterId);
        m_valuesBuffer.putInt64(counterOffset(counterId) + OWNER_ID_OFFSET, ownerId);
    }

private:
    std::deque<std::int32_t> m_freeList;
    clock_t m_clock = []() { return 0LL; };
    const long m_freeToReuseTimeoutMs = 0;
    util::index_t m_highWaterMark = -1;

    inline std::int32_t nextCounterId()
    {
        if (!m_freeList.empty())
        {
            const long long nowMs = m_clock();

            auto it = std::find_if(m_freeList.begin(), m_freeList.end(),
                [&](std::int32_t counterId)
                {
                   return nowMs >=
                       m_metadataBuffer.getInt64Volatile(metadataOffset(counterId) + FREE_FOR_REUSE_DEADLINE_OFFSET);
                });

            if (it != m_freeList.end())
            {
                const std::int32_t counterId = *it;
                const util::index_t offset = counterOffset(counterId);

                m_freeList.erase(it);
                m_valuesBuffer.putInt64Ordered(offset + REGISTRATION_ID_OFFSET, DEFAULT_REGISTRATION_ID);
                m_valuesBuffer.putInt64(offset + OWNER_ID_OFFSET, DEFAULT_OWNER_ID);
                m_valuesBuffer.putInt64Ordered(offset, 0);

                return counterId;
            }            
        }

        if (m_highWaterMark + 1 > m_maxCounterId)
        {
            throw util::IllegalArgumentException("unable to allocated counter, values buffer is full", SOURCEINFO);
        }

        return ++m_highWaterMark;
    }
};

}}

#endif
