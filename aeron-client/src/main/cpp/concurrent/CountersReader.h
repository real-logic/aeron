/*
 * Copyright 2014-2019 Real Logic Ltd.
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
#ifndef AERON_COUNTERS_READER_H
#define AERON_COUNTERS_READER_H

#include <cstdint>
#include <cstddef>
#include <functional>
#include <util/BitUtil.h>

#include "AtomicBuffer.h"

namespace aeron { namespace concurrent {

/**
 * Reads the counters metadata and values buffers.
 *
 * This class is threadsafe and can be used across threads.
 *
 * <b>Values Buffer</b>
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                        Counter Value                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                     120 bytes of padding                     ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                   Repeats to end of buffer                   ...
 *  |                                                               |
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 *
 * <b>Meta Data Buffer</b>
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                        Record State                           |
 *  +---------------------------------------------------------------+
 *  |                          Type Id                              |
 *  +---------------------------------------------------------------+
 *  |                   Free-for-reuse Deadline                     |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                      112 bytes for key                       ...
 * ...                                                              |
 *  +-+-------------------------------------------------------------+
 *  |R|                      Label Length                           |
 *  +-+-------------------------------------------------------------+
 *  |                  380 bytes of Label in UTF-8                 ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                   Repeats to end of buffer                   ...
 *  |                                                               |
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */

typedef std::function<void(
    std::int32_t,
    std::int32_t,
    const AtomicBuffer&,
    const std::string&)> on_counters_metadata_t;

class CountersReader
{
public:
    inline CountersReader(const AtomicBuffer& metadataBuffer, const AtomicBuffer& valuesBuffer) :
        m_metadataBuffer(metadataBuffer),
        m_valuesBuffer(valuesBuffer),
        m_maxCounterId(valuesBuffer.capacity() / COUNTER_LENGTH)
    {
    }

    template <typename F>
    void forEach(F&& onCountersMetadata) const
    {
        std::int32_t id = 0;

        for (util::index_t i = 0, capacity = m_metadataBuffer.capacity(); i < capacity; i += METADATA_LENGTH)
        {
            std::int32_t recordStatus = m_metadataBuffer.getInt32Volatile(i);

            if (RECORD_UNUSED == recordStatus)
            {
                break;
            }
            else if (RECORD_ALLOCATED == recordStatus)
            {
                const struct CounterMetaDataDefn& record = m_metadataBuffer.overlayStruct<CounterMetaDataDefn>(i);

                const std::string label = m_metadataBuffer.getString(i + LABEL_LENGTH_OFFSET);
                const AtomicBuffer keyBuffer(m_metadataBuffer.buffer() + i + KEY_OFFSET, sizeof(CounterMetaDataDefn::key));

                onCountersMetadata(id, record.typeId, keyBuffer, label);
            }

            id++;
        }
    }

    inline std::int32_t maxCounterId() const
    {
        return m_maxCounterId;
    }

    inline std::int64_t getCounterValue(std::int32_t id) const
    {
        validateCounterId(id);

        return m_valuesBuffer.getInt64Volatile(counterOffset(id));
    }

    inline std::int32_t getCounterState(std::int32_t id) const
    {
        validateCounterId(id);

        return m_metadataBuffer.getInt32Volatile(metadataOffset(id));
    }

    inline std::int64_t getFreeToReuseDeadline(std::int32_t id) const
    {
        validateCounterId(id);

        return m_metadataBuffer.getInt64Volatile(metadataOffset(id) + FREE_TO_REUSE_DEADLINE_OFFSET);
    }

    inline std::string getCounterLabel(std::int32_t id) const
    {
        validateCounterId(id);

        return m_metadataBuffer.getString(metadataOffset(id) + LABEL_LENGTH_OFFSET);
    }

    inline static util::index_t counterOffset(std::int32_t counterId)
    {
        return counterId * COUNTER_LENGTH;
    }

    inline static util::index_t metadataOffset(std::int32_t counterId)
    {
        return counterId * METADATA_LENGTH;
    }

    inline AtomicBuffer valuesBuffer() const
    {
        return m_valuesBuffer;
    }

    inline AtomicBuffer metaDataBuffer() const
    {
        return m_metadataBuffer;
    }

#pragma pack(push)
#pragma pack(4)
    struct CounterValueDefn
    {
        std::int64_t counterValue;
        std::int8_t pad1[(2 * util::BitUtil::CACHE_LINE_LENGTH) - sizeof(std::int64_t)];
    };

    struct CounterMetaDataDefn
    {
        std::int32_t state;
        std::int32_t typeId;
        std::int64_t freeToReuseDeadline;
        std::int8_t key[(2 * util::BitUtil::CACHE_LINE_LENGTH) - (2 * sizeof(std::int32_t)) - sizeof(std::int64_t)];
        std::int32_t labelLength;
        std::int8_t label[(6 * util::BitUtil::CACHE_LINE_LENGTH) - sizeof(std::int32_t)];
    };
#pragma pack(pop)

    static const std::int32_t RECORD_UNUSED = 0;
    static const std::int32_t RECORD_ALLOCATED = 1;
    static const std::int32_t RECORD_RECLAIMED = -1;

    static const std::int64_t NOT_FREE_TO_REUSE = INT64_MAX;

    static const util::index_t COUNTER_LENGTH = sizeof(CounterValueDefn);
    static const util::index_t METADATA_LENGTH = sizeof(CounterMetaDataDefn);
    static const util::index_t FREE_TO_REUSE_DEADLINE_OFFSET = offsetof(CounterMetaDataDefn, freeToReuseDeadline);
    static const util::index_t KEY_OFFSET = offsetof(CounterMetaDataDefn, key);
    static const util::index_t LABEL_LENGTH_OFFSET = offsetof(CounterMetaDataDefn, labelLength);

    static const std::int32_t MAX_LABEL_LENGTH = sizeof(CounterMetaDataDefn::label);
    static const std::int32_t MAX_KEY_LENGTH = sizeof(CounterMetaDataDefn::key);

protected:
    AtomicBuffer m_metadataBuffer;
    AtomicBuffer m_valuesBuffer;
    const std::int32_t m_maxCounterId;

    void validateCounterId(std::int32_t counterId) const
    {
        if (counterId < 0 || counterId > m_maxCounterId)
        {
            throw util::IllegalArgumentException(
                "counter id " + std::to_string(counterId) +
                " out of range: maxCounterId=" + std::to_string(m_maxCounterId),
                SOURCEINFO);
        }
    }
};

}}

#endif
