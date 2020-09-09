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
#ifndef AERON_LOSS_REPORT_READER_H
#define AERON_LOSS_REPORT_READER_H

#include <functional>

#include "util/Index.h"
#include "concurrent/AtomicBuffer.h"
#include "util/BitUtil.h"
#include "concurrent/reports/LossReportDescriptor.h"

namespace aeron { namespace concurrent { namespace reports {

namespace LossReportReader {

typedef std::function<void(
    std::int64_t observationCount,
    std::int64_t totalBytesLost,
    std::int64_t firstObservationTimestamp,
    std::int64_t lastObservationTimestamp,
    std::int32_t sessionId,
    std::int32_t streamId,
    const std::string &channel,
    const std::string &source)> loss_report_consumer_t;

/**
 * Read a LossReport contained in the buffer. This can be done concurrently.
 *
 * @param buffer        containing the loss report.
 * @param entryConsumer to be called to accept each entry in the report.
 * @return the number of entries read.
 */
inline static int read(AtomicBuffer& buffer, const loss_report_consumer_t &consumer)
{
    int recordsRead = 0;
    util::index_t offset = 0;
    const util::index_t capacity = buffer.capacity();

    while (offset < capacity)
    {
        const std::int64_t observationCount = buffer.getInt64Volatile(
            offset + LossReportDescriptor::OBSERVATION_COUNT_OFFSET);
        if (0 == observationCount)
        {
            break;
        }

        ++recordsRead;

        const std::string channel = buffer.getString(offset + LossReportDescriptor::CHANNEL_OFFSET);
        auto alignedChannelLength = static_cast<util::index_t>(util::BitUtil::align(
            sizeof(std::int32_t) + channel.length(), sizeof(std::int32_t)));
        const std::string source = buffer.getString(
            offset + LossReportDescriptor::CHANNEL_OFFSET + alignedChannelLength);

        auto &record = buffer.overlayStruct<LossReportDescriptor::LossReportEntryDefn>(offset);

        consumer(
            observationCount,
            record.totalBytesLost,
            record.firstObservationTimestamp,
            record.lastObservationTimestamp,
            record.sessionId,
            record.streamId,
            channel,
            source);

        const util::index_t recordLength =
            LossReportDescriptor::CHANNEL_OFFSET +
            alignedChannelLength +
            static_cast<util::index_t>(sizeof(std::int32_t)) +
            static_cast<util::index_t>(source.length());

        offset += util::BitUtil::align(recordLength, LossReportDescriptor::ENTRY_ALIGNMENT);
    }

    return recordsRead;
}

}}}}

#endif //AERON_LOSS_REPORT_READER_H
