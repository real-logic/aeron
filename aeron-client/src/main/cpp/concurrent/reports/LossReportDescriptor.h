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
#ifndef AERON_LOSS_REPORT_DESCRIPTOR_H
#define AERON_LOSS_REPORT_DESCRIPTOR_H

#include "util/Index.h"
#include "util/BitUtil.h"

namespace aeron { namespace concurrent { namespace reports {

/**
 * A report of loss events on a message stream.
 * <p>
 * The provided AtomicBuffer can wrap a memory-mapped file so logging can be out of process. This provides
 * the benefit that if a crash or lockup occurs then the log can be read externally without loss of data.
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |R|                    Observation Count                        |
 *  |                                                               |
 *  +-+-------------------------------------------------------------+
 *  |R|                     Total Bytes Lost                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                 First Observation Timestamp                   |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                  Last Observation Timestamp                   |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                          Session ID                           |
 *  +---------------------------------------------------------------+
 *  |                           Stream ID                           |
 *  +---------------------------------------------------------------+
 *  |                 Channel encoded in US-ASCII                  ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                  Source encoded in US-ASCII                  ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */
namespace LossReportDescriptor
{

#pragma pack(push)
#pragma pack(4)
struct LossReportEntryDefn
{
    std::int64_t observationCount;
    std::int64_t totalBytesLost;
    std::int64_t firstObservationTimestamp;
    std::int64_t lastObservationTimestamp;
    std::int32_t sessionId;
    std::int32_t streamId;
};
#pragma pack(pop)

static const util::index_t ENTRY_ALIGNMENT = sizeof(util::BitUtil::CACHE_LINE_LENGTH);
static const util::index_t OBSERVATION_COUNT_OFFSET =
    static_cast<util::index_t>(offsetof(LossReportEntryDefn, observationCount));
static const util::index_t CHANNEL_OFFSET = sizeof(LossReportEntryDefn);

static const std::string LOSS_REPORT_FILE_NAME = "loss-report.dat";

inline static std::string file(std::string &aeronDirectoryName)
{
#if defined(_MSC_VER)
    return aeronDirectoryName + "\\" + LOSS_REPORT_FILE_NAME;
#else
    return aeronDirectoryName + "/" + LOSS_REPORT_FILE_NAME;
#endif
}

}

}}}

#endif //AERON_LOSS_REPORT_DESCRIPTOR_H
