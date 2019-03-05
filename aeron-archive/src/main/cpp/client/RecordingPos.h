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
#ifndef AERON_ARCHIVE_RECORDING_POS_H
#define AERON_ARCHIVE_RECORDING_POS_H

#include "Aeron.h"

namespace aeron {
namespace archive {
namespace client {
namespace RecordingPos {

constexpr const std::int32_t RECORDING_POSITION_TYPE_ID = 100;
constexpr const std::int64_t NULL_RECORDING_ID = aeron::NULL_VALUE;

#pragma pack(push)
#pragma pack(4)
struct RecordingPosKeyDefn
{
    std::int64_t recordingId;
    std::int32_t sessionId;
    std::int32_t sourceIdentityLength;
};
#pragma pack(pop)

inline static std::int32_t findCounterIdByRecording(CountersReader& countersReader, std::int64_t recordingId)
{
    AtomicBuffer buffer = countersReader.metaDataBuffer();

    for (std::int32_t i = 0, size = countersReader.maxCounterId(); i < size; i++)
    {
        if (countersReader.getCounterState(i) == CountersReader::RECORD_ALLOCATED)
        {
            const util::index_t recordOffset = CountersReader::metadataOffset(i);
            auto key = buffer.overlayStruct<RecordingPosKeyDefn>(
                recordOffset + CountersReader::KEY_OFFSET);

            if (buffer.getInt32(recordOffset + CountersReader::TYPE_ID_OFFSET) == RECORDING_POSITION_TYPE_ID &&
                recordingId == key.recordingId)
            {
                return i;
            }
        }
    }

    return CountersReader::NULL_COUNTER_ID;
}

inline static std::int32_t findCounterIdBySession(CountersReader& countersReader, std::int32_t sessionId)
{
    AtomicBuffer buffer = countersReader.metaDataBuffer();

    for (std::int32_t i = 0, size = countersReader.maxCounterId(); i < size; i++)
    {
        if (countersReader.getCounterState(i) == CountersReader::RECORD_ALLOCATED)
        {
            const util::index_t recordOffset = CountersReader::metadataOffset(i);
            auto key = buffer.overlayStruct<RecordingPosKeyDefn>(
                recordOffset + CountersReader::KEY_OFFSET);

            if (buffer.getInt32(recordOffset + CountersReader::TYPE_ID_OFFSET) == RECORDING_POSITION_TYPE_ID &&
                sessionId == key.sessionId)
            {
                return i;
            }
        }
    }

    return CountersReader::NULL_COUNTER_ID;
}

inline static std::int64_t getRecordingId(CountersReader& countersReader, std::int32_t counterId)
{
    AtomicBuffer buffer = countersReader.metaDataBuffer();

    if (countersReader.getCounterState(counterId) == CountersReader::RECORD_ALLOCATED)
    {
        const util::index_t recordOffset = CountersReader::metadataOffset(counterId);
        auto key = buffer.overlayStruct<RecordingPosKeyDefn>(recordOffset + CountersReader::KEY_OFFSET);

        if (buffer.getInt32(recordOffset + CountersReader::TYPE_ID_OFFSET) == RECORDING_POSITION_TYPE_ID)
        {
            return key.recordingId;
        }
    }

    return CountersReader::NULL_COUNTER_ID;
}

inline static std::string getSourceIdentity(CountersReader& countersReader, std::int32_t counterId)
{
    AtomicBuffer buffer = countersReader.metaDataBuffer();

    if (countersReader.getCounterState(counterId) == CountersReader::RECORD_ALLOCATED)
    {
        const util::index_t recordOffset = CountersReader::metadataOffset(counterId);
        auto key = buffer.overlayStruct<RecordingPosKeyDefn>(recordOffset + CountersReader::KEY_OFFSET);

        if (buffer.getInt32(recordOffset + CountersReader::TYPE_ID_OFFSET) == RECORDING_POSITION_TYPE_ID)
        {
            return std::string(
                buffer.sbeData() + CountersReader::KEY_OFFSET + sizeof(RecordingPosKeyDefn),
                static_cast<std::size_t>(key.sourceIdentityLength));
        }
    }

    return "";
}

inline static bool isActive(CountersReader& countersReader, std::int32_t counterId, std::int64_t recordingId)
{
    AtomicBuffer buffer = countersReader.metaDataBuffer();

    if (countersReader.getCounterState(counterId) == CountersReader::RECORD_ALLOCATED)
    {
        const util::index_t recordOffset = CountersReader::metadataOffset(counterId);
        auto key = buffer.overlayStruct<RecordingPosKeyDefn>(recordOffset + CountersReader::KEY_OFFSET);

        return
            buffer.getInt32(recordOffset + CountersReader::TYPE_ID_OFFSET) == RECORDING_POSITION_TYPE_ID &&
            recordingId == key.recordingId;
    }

    return false;
}

}
}}}
#endif //AERON_ARCHIVE_RECORDING_POS_H
