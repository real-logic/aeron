/*
 * Copyright 2014-2017 Real Logic Ltd.
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
    inline CountersManager(const AtomicBuffer& metadataBuffer, const AtomicBuffer& valuesBuffer) :
        CountersReader(metadataBuffer, valuesBuffer)
    {
    }

    std::int32_t allocate(
        const std::string& label,
        std::int32_t typeId,
        const std::function<void(AtomicBuffer&)>& keyFunc)
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
        m_metadataBuffer.putInt32Ordered(counterOffset(counterId), RECORD_RECLAIMED);
        m_freeList.push_back(counterId);
    }

    inline void setCounterValue(std::int32_t counterId, std::int64_t value)
    {
        m_valuesBuffer.putInt64Ordered(counterOffset(counterId), value);
    }

private:
    util::index_t m_highwaterMark = 0;

    std::deque<std::int32_t> m_freeList;

    inline std::int32_t nextCounterId()
    {
        if (m_freeList.empty())
        {
            return m_highwaterMark++;
        }

        std::int32_t id = m_freeList.front();
        m_freeList.pop_front();
        m_valuesBuffer.putInt64Ordered(counterOffset(id), 0L);

        return id;
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
