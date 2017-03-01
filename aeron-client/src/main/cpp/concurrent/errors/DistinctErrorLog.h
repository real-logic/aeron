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
#ifndef AERON_DISTINCTERRORLOG_H
#define AERON_DISTINCTERRORLOG_H

#include <functional>
#include <typeinfo>
#include <vector>
#include <algorithm>
#include <mutex>
#include <atomic>
#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include <util/BitUtil.h>
#include "ErrorLogDescriptor.h"

namespace aeron {

namespace concurrent {

namespace errors {

class DistinctErrorLog
{
public:
    typedef std::function<std::int64_t()> clock_t;

    inline DistinctErrorLog(AtomicBuffer& buffer, clock_t clock) :
        m_buffer(buffer),
        m_clock(clock),
        m_observations(buffer.capacity() / ErrorLogDescriptor::HEADER_LENGTH),
        m_numObservations(0),
        m_nextOffset(0)
    {
    }

    inline bool record(std::exception& observation)
    {
        return record(typeid(observation).hash_code(), observation.what(), "no message");
    }

    inline bool record(util::SourcedException& observation)
    {
        return record(typeid(observation).hash_code(), observation.where(), observation.what());
    }

    bool record(std::size_t errorCode, const std::string& description, const std::string& message)
    {
        std::int64_t timestamp = m_clock();
        std::size_t originalNumObservations = std::atomic_load(&m_numObservations);

        auto it = findObservation(m_observations, originalNumObservations, errorCode, description);

        if (it == m_observations.end())
        {
            std::lock_guard<std::recursive_mutex> lock(m_lock);

            it = newObservation(originalNumObservations, timestamp, errorCode, description, message);
            if (it == m_observations.end())
            {
                return false;
            }
        }

        DistinctObservation& observation = *it;

        util::index_t offset = observation.m_offset;

        m_buffer.getAndAddInt32(offset + ErrorLogDescriptor::OBSERVATION_COUNT_OFFSET, 1);
        m_buffer.putInt64Ordered(offset + ErrorLogDescriptor::LAST_OBERSATION_TIMESTAMP_OFFSET, timestamp);

        return true;
    }

private:
    struct DistinctObservation
    {
        std::size_t m_errorCode;
        std::string m_description;
        util::index_t m_offset;
    };

    AtomicBuffer& m_buffer;
    clock_t m_clock;
    std::recursive_mutex m_lock;

    std::vector<DistinctObservation> m_observations;
    std::atomic<std::size_t> m_numObservations;

    util::index_t m_nextOffset;

    static std::string encodeObservation(std::size_t errorCode, const std::string& description, const std::string& message)
    {
        return description + " " + message;
    }

    static std::vector<DistinctObservation>::iterator findObservation(
        std::vector<DistinctObservation>& observations,
        std::size_t numObservations,
        std::size_t errorCode,
        const std::string& description)
    {
        auto begin = observations.begin();
        auto end = begin + numObservations;

        auto result = std::find_if(begin, end,
            [errorCode, description](const DistinctObservation& observation)
            {
                return (errorCode == observation.m_errorCode && description == observation.m_description);
            });

        return (result != end) ? result : observations.end();
    }

    std::vector<DistinctObservation>::iterator newObservation(
        std::size_t existingNumObservations,
        std::int64_t timestamp,
        std::size_t errorCode,
        const std::string& description,
        const std::string& message)
    {
        std::size_t numObservations = std::atomic_load(&m_numObservations);

        auto it = m_observations.end();

        if (existingNumObservations != numObservations)
        {
            it = findObservation(m_observations, numObservations, errorCode, description);
        }

        if (it == m_observations.end())
        {
            const std::string encodedError = encodeObservation(errorCode, description, message);
            const util::index_t length = ErrorLogDescriptor::HEADER_LENGTH + encodedError.length();
            const util::index_t offset = m_nextOffset;

            if ((offset + length) > m_buffer.capacity())
            {
                return m_observations.end();
            }

            m_buffer.putStringUtf8WithoutLength(offset + ErrorLogDescriptor::ENCODED_ERROR_OFFSET, encodedError);
            m_buffer.putInt64(offset + ErrorLogDescriptor::FIRST_OBERSATION_TIMESTAMP_OFFSET, timestamp);

            m_nextOffset = util::BitUtil::align(offset + length, ErrorLogDescriptor::RECORD_ALIGNMENT);

            it = m_observations.begin() + numObservations;

            DistinctObservation& observation = *it;
            observation.m_errorCode = errorCode;
            observation.m_description = description;
            observation.m_offset = offset;

            std::atomic_store(&m_numObservations, numObservations + 1);

            m_buffer.putInt32Ordered(offset + ErrorLogDescriptor::LENGTH_OFFSET, length);
        }

        return it;
    }
};

}}}

#endif
