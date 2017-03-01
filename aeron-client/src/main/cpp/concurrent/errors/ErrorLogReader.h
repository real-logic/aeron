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
#ifndef AERON_ERRORLOGREADER_H
#define AERON_ERRORLOGREADER_H

#include <functional>
#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include <util/BitUtil.h>
#include "ErrorLogDescriptor.h"

namespace aeron {

namespace concurrent {

namespace errors {

namespace ErrorLogReader {

typedef std::function<void(
    std::int32_t observationCount,
    std::int64_t firstObservationTimestamp,
    std::int64_t lastObservationTimestamp,
    const std::string &encodedException)> error_consumer_t;

inline static int read(
    AtomicBuffer& buffer,
    const error_consumer_t &consumer,
    std::int64_t sinceTimestamp)
{
    int entries = 0;
    int offset = 0;
    const int capacity = buffer.capacity();

    while (offset < capacity)
    {
        const std::int32_t length = buffer.getInt32Volatile(offset + ErrorLogDescriptor::LENGTH_OFFSET);
        if (0 == length)
        {
            break;
        }

        const std::int64_t lastObservationTimestamp =
            buffer.getInt64Volatile(offset + ErrorLogDescriptor::LAST_OBERSATION_TIMESTAMP_OFFSET);

        if (lastObservationTimestamp >= sinceTimestamp)
        {
            ErrorLogDescriptor::ErrorLogEntryDefn& entry =
                buffer.overlayStruct<ErrorLogDescriptor::ErrorLogEntryDefn>(offset);

            ++entries;

            consumer(
                entry.observationCount,
                entry.firstObservationTimestamp,
                lastObservationTimestamp,
                buffer.getStringUtf8WithoutLength(
                    offset + ErrorLogDescriptor::ENCODED_ERROR_OFFSET,
                    length - ErrorLogDescriptor::HEADER_LENGTH));
        }

        offset += util::BitUtil::align(length, ErrorLogDescriptor::RECORD_ALIGNMENT);
    }

    return entries;
}

}}}}

#endif
