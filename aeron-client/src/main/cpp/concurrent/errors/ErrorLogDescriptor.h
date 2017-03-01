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
#ifndef AERON_ERRORLOGDESCRIPTOR_H
#define AERON_ERRORLOGDESCRIPTOR_H

#include <cstddef>
#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>

namespace aeron {

namespace concurrent {

namespace errors {

/**
 * Distinct record of error observations. Rather than grow a record indefinitely when many errors of the same type
 * are logged, this log takes the approach of only recording distinct errors of the same type type and stack trace
 * and keeping a count and time of observation so that the record only grows with new distinct observations.
 *
 * The provided {@link AtomicBuffer} can wrap a memory-mapped file so logging can be out of process. This provides
 * the benefit that if a crash or lockup occurs then the log can be read externally without loss of data.
 *
 * This class is threadsafe to be used from multiple logging threads.
 *
 * The error records are recorded to the memory mapped buffer in the following format.
 *
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |R|                         Length                              |
 *  +-+-------------------------------------------------------------+
 *  |R|                     Observation Count                       |
 *  +-+-------------------------------------------------------------+
 *  |R|                Last Observation Timestamp                   |
 *  |                                                               |
 *  +-+-------------------------------------------------------------+
 *  |R|               First Observation Timestamp                   |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                     UTF-8 Encoded Error                      ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */

namespace ErrorLogDescriptor {

#pragma pack(push)
#pragma pack(4)
struct ErrorLogEntryDefn
{
    std::int32_t length;
    std::int32_t observationCount;
    std::int64_t lastObservationTimestamp;
    std::int64_t firstObservationTimestamp;
};
#pragma pack(pop)

static const util::index_t LENGTH_OFFSET = offsetof(ErrorLogEntryDefn, length);
static const util::index_t OBSERVATION_COUNT_OFFSET = offsetof(ErrorLogEntryDefn, observationCount);
static const util::index_t LAST_OBERSATION_TIMESTAMP_OFFSET = offsetof(ErrorLogEntryDefn, lastObservationTimestamp);
static const util::index_t FIRST_OBERSATION_TIMESTAMP_OFFSET = offsetof(ErrorLogEntryDefn, firstObservationTimestamp);
static const util::index_t ENCODED_ERROR_OFFSET = sizeof(ErrorLogEntryDefn);
static const util::index_t HEADER_LENGTH = sizeof(ErrorLogEntryDefn);

static const util::index_t RECORD_ALIGNMENT = sizeof(std::int64_t);

}

}}}

#endif
