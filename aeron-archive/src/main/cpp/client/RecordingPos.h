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

namespace aeron { namespace archive { namespace client {

/**
 * The position a recording has reached when being archived.
 * <p>
 * Key has the following layout:
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                        Recording ID                           |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                         Session ID                            |
 *  +---------------------------------------------------------------+
 *  |                Source Identity for the Image                  |
 *  |                                                              ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */
namespace RecordingPos {

/// Type id of a recording position counter.
constexpr const std::int32_t RECORDING_POSITION_TYPE_ID = 100;
/// Represents a null recording id when not found.
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

/**
 * Find the active counter id for a stream based on the recording id.
 *
 * @param countersReader to search within.
 * @param recordingId    for the active recording.
 * @return the counter id if found otherwise #NULL_COUNTER_ID.
 */
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

/**
 * Find the active counter id for a stream based on the session id.
 *
 * @param countersReader to search within.
 * @param sessionId      for the active recording.
 * @return the counter id if found otherwise #NULL_COUNTER_ID.
 */
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

/**
 * Get the recording id for a given counter id.
 *
 * @param countersReader to search within.
 * @param counterId      for the active recording.
 * @return the counter id if found otherwise {#NULL_RECORDING_ID.
 */
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

/**
 * Get the Image#sourceIdentity for the recording.
 *
 * @param countersReader to search within.
 * @param counterId      for the active recording.
 * @return Image#sourceIdentity for the recording or null if not found.
 */
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

/**
 * Is the recording counter still active.
 *
 * @param countersReader to search within.
 * @param counterId      to search for.
 * @param recordingId    to confirm it is still the same value.
 * @return true if the counter is still active otherwise false.
 */
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
