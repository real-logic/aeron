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
#ifndef AERON_COUNTERS_READER_H
#define AERON_COUNTERS_READER_H

#include <cstdint>
#include <cstddef>
#include <functional>

#include "util/BitUtil.h"
#include "AtomicBuffer.h"

extern "C"
{
#include "aeronc.h"
#include "concurrent/aeron_counters_manager.h"
}

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
 *  |                  380 bytes of Label in ASCII                 ...
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
    inline CountersReader(aeron_counters_reader_t *countersReader) : m_countersReader(countersReader)
    {
    }

    template <typename F>
    void forEach(F&& onCountersMetadata) const
    {
        std::int32_t id = 0;

        const AtomicBuffer &metadataBuffer = metaDataBuffer();
        for (util::index_t i = 0, capacity = metadataBuffer.capacity(); i < capacity; i += METADATA_LENGTH)
        {
            std::int32_t recordStatus = metadataBuffer.getInt32Volatile(i);

            if (RECORD_UNUSED == recordStatus)
            {
                break;
            }
            else if (RECORD_ALLOCATED == recordStatus)
            {
                const auto& record = metadataBuffer.overlayStruct<CounterMetaDataDefn>(i);

                const std::string label = metadataBuffer.getString(i + LABEL_LENGTH_OFFSET);
                const AtomicBuffer keyBuffer(metadataBuffer.buffer() + i + KEY_OFFSET, sizeof(CounterMetaDataDefn::key));

                onCountersMetadata(id, record.typeId, keyBuffer, label);
            }

            id++;
        }
    }

    inline std::int32_t maxCounterId() const
    {
        // TODO: should this sit behind a function call.
        return m_countersReader->max_counter_id;
    }

    inline std::int64_t getCounterValue(std::int32_t id) const
    {
        validateCounterId(id);
        int64_t *counter_addr = aeron_counters_reader_addr(m_countersReader, id);
        return aeron_counter_get(counter_addr);
    }

    inline std::int32_t getCounterState(std::int32_t id) const
    {
        std::int32_t state;
        if (aeron_counters_reader_counter_state(m_countersReader, id, &state) < 0)
        {
            throw util::IllegalArgumentException(
                "counter id " + std::to_string(id) +
                    " out of range: maxCounterId=" + std::to_string(maxCounterId()),
                SOURCEINFO);
        }

        return state;
    }

    inline std::int64_t getFreeToReuseDeadline(std::int32_t id) const
    {
        std::int64_t deadline;
        if (aeron_counters_reader_free_to_reuse_deadline_ms(m_countersReader, id, &deadline))
        {
            throw util::IllegalArgumentException(
                "counter id " + std::to_string(id) +
                    " out of range: maxCounterId=" + std::to_string(maxCounterId()),
                SOURCEINFO);
        }

        return deadline;
    }

    inline std::string getCounterLabel(std::int32_t id) const
    {
        char buffer[AERON_COUNTERS_MANAGER_METADATA_LENGTH];
        int length = aeron_counters_reader_counter_label(
            m_countersReader, id, buffer, AERON_COUNTERS_MANAGER_METADATA_LENGTH);
        if (length < 0)
        {
            throw util::IllegalArgumentException(
                "counter id " + std::to_string(id) +
                    " out of range: maxCounterId=" + std::to_string(maxCounterId()),
                SOURCEINFO);
        }

        return std::string(buffer, (size_t)length);
    }

    inline static util::index_t counterOffset(std::int32_t counterId)
    {
        return AERON_COUNTER_OFFSET(counterId);
    }

    inline static util::index_t metadataOffset(std::int32_t counterId)
    {
        return AERON_COUNTER_METADATA_OFFSET(counterId);
    }

    inline AtomicBuffer valuesBuffer() const
    {
        return AtomicBuffer(m_countersReader->values, m_countersReader->values_length);
    }

    inline AtomicBuffer metaDataBuffer() const
    {
        return AtomicBuffer(m_countersReader->metadata, m_countersReader->metadata_length);
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

    static const std::int32_t NULL_COUNTER_ID = -1;

    static const std::int32_t RECORD_UNUSED = 0;
    static const std::int32_t RECORD_ALLOCATED = 1;
    static const std::int32_t RECORD_RECLAIMED = -1;

    static const std::int64_t NOT_FREE_TO_REUSE = INT64_MAX;

    static const util::index_t COUNTER_LENGTH = sizeof(CounterValueDefn);
    static const util::index_t METADATA_LENGTH = sizeof(CounterMetaDataDefn);
    static const util::index_t TYPE_ID_OFFSET = offsetof(CounterMetaDataDefn, typeId);
    static const util::index_t FREE_TO_REUSE_DEADLINE_OFFSET = offsetof(CounterMetaDataDefn, freeToReuseDeadline);
    static const util::index_t KEY_OFFSET = offsetof(CounterMetaDataDefn, key);
    static const util::index_t LABEL_LENGTH_OFFSET = offsetof(CounterMetaDataDefn, labelLength);

    static const std::int32_t MAX_LABEL_LENGTH = sizeof(CounterMetaDataDefn::label);
    static const std::int32_t MAX_KEY_LENGTH = sizeof(CounterMetaDataDefn::key);

protected:
    aeron_counters_reader_t *m_countersReader;

    void validateCounterId(std::int32_t counterId) const
    {
        if (counterId < 0 || counterId > maxCounterId())
        {
            throw util::IllegalArgumentException(
                "counter id " + std::to_string(counterId) +
                " out of range: maxCounterId=" + std::to_string(maxCounterId()),
                SOURCEINFO);
        }
    }
};

}}

#endif
