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
#ifndef AERON_CONCURRENT_ERROR_LOG_READER_H
#define AERON_CONCURRENT_ERROR_LOG_READER_H

#include <functional>
#include "concurrent/AtomicBuffer.h"
extern "C"
{
#include "concurrent/aeron_distinct_error_log.h"
}

namespace aeron { namespace concurrent { namespace errors {

namespace ErrorLogReader {

using namespace aeron::concurrent;

typedef std::function<void(
    std::int32_t observationCount,
    std::int64_t firstObservationTimestamp,
    std::int64_t lastObservationTimestamp,
    const std::string &encodedException)> error_consumer_t;

inline static int read(AtomicBuffer &buffer, const error_consumer_t &consumer, std::int64_t sinceTimestamp)
{
    aeron_error_log_reader_func_t aeron_error_log_consumer = [](
        int32_t observation_count,
        int64_t first_observation_timestamp,
        int64_t last_observation_timestamp,
        const char *encoded_exception,
        size_t length,
        void *client_id)
    {
        auto consumer = reinterpret_cast<error_consumer_t *>(client_id);
        (*consumer)(observation_count,
            first_observation_timestamp,
            last_observation_timestamp,
            std::string(encoded_exception, length));
    };
    aeron_error_log_read(
        buffer.buffer(),
        buffer.capacity(),
        aeron_error_log_consumer,
        const_cast<void *>(reinterpret_cast<const void *>(&consumer)),
        sinceTimestamp);
}

}}}}

#endif
