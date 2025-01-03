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
#ifndef AERON_ARCHIVE_RECORDING_POS_H
#define AERON_ARCHIVE_RECORDING_POS_H

#include "Aeron.h"

namespace aeron { namespace archive { namespace client
{

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
 *  |                         Archive ID                            |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 * </pre>
 */
namespace RecordingPos
{

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

static const util::index_t RECORDING_ID_OFFSET = offsetof(RecordingPosKeyDefn, recordingId);
static const util::index_t SESSION_ID_OFFSET = offsetof(RecordingPosKeyDefn, sessionId);
static const util::index_t SOURCE_IDENTITY_LENGTH_OFFSET = offsetof(RecordingPosKeyDefn, sourceIdentityLength);
static const util::index_t SOURCE_IDENTITY_OFFSET = SOURCE_IDENTITY_LENGTH_OFFSET + sizeof(int32_t);

/**
 * Find the active counter id for a stream based on the recording id.
 *
 * @param countersReader to search within.
 * @param recordingId    for the active recording.
 * @return the counter id if found otherwise #NULL_COUNTER_ID.
 */
inline static std::int32_t findCounterIdByRecordingId(CountersReader &countersReader, std::int64_t recordingId)
{
    AtomicBuffer buffer = countersReader.metaDataBuffer();

    for (std::int32_t i = 0, size = countersReader.maxCounterId(); i < size; i++)
    {
        const int32_t counterState = countersReader.getCounterState(i);
        if (CountersReader::RECORD_ALLOCATED == counterState)
        {
            if (countersReader.getCounterTypeId(i) == RECORDING_POSITION_TYPE_ID &&
                buffer.getInt64(
                    CountersReader::metadataOffset(i) + CountersReader::KEY_OFFSET + RECORDING_ID_OFFSET) == recordingId)
            {
                return i;
            }
        }
        else if (CountersReader::RECORD_UNUSED == counterState)
        {
            break;
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
inline static std::int32_t findCounterIdBySessionId(CountersReader &countersReader, std::int32_t sessionId)
{
    AtomicBuffer buffer = countersReader.metaDataBuffer();

    for (std::int32_t i = 0, size = countersReader.maxCounterId(); i < size; i++)
    {
        const int32_t counterState = countersReader.getCounterState(i);
        if (CountersReader::RECORD_ALLOCATED == counterState)
        {
            if (countersReader.getCounterTypeId(i) == RECORDING_POSITION_TYPE_ID &&
                buffer.getInt32(
                    CountersReader::metadataOffset(i) + CountersReader::KEY_OFFSET + SESSION_ID_OFFSET) == sessionId)
            {
                return i;
            }
        }
        else if (CountersReader::RECORD_UNUSED == counterState)
        {
            break;
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
inline static std::int64_t getRecordingId(CountersReader &countersReader, std::int32_t counterId)
{
    if (countersReader.getCounterState(counterId) == CountersReader::RECORD_ALLOCATED &&
        countersReader.getCounterTypeId(counterId) == RECORDING_POSITION_TYPE_ID)
    {
        return countersReader.metaDataBuffer().getInt64(
            CountersReader::metadataOffset(counterId) + CountersReader::KEY_OFFSET + RECORDING_ID_OFFSET);
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
inline static std::string getSourceIdentity(CountersReader &countersReader, std::int32_t counterId)
{
    if (countersReader.getCounterState(counterId) == CountersReader::RECORD_ALLOCATED &&
        countersReader.getCounterTypeId(counterId) == RECORDING_POSITION_TYPE_ID)
    {
        AtomicBuffer buffer = countersReader.metaDataBuffer();
        const auto key_offset = CountersReader::metadataOffset(counterId) + CountersReader::KEY_OFFSET;
        const auto source_identity_length = buffer.getInt32(key_offset + SOURCE_IDENTITY_LENGTH_OFFSET);
        return
            {
                buffer.sbeData() + key_offset + SOURCE_IDENTITY_OFFSET,
                static_cast<std::size_t>(source_identity_length)
            };
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
inline static bool isActive(CountersReader &countersReader, std::int32_t counterId, std::int64_t recordingId)
{
    return countersReader.getCounterState(counterId) == CountersReader::RECORD_ALLOCATED &&
        countersReader.getCounterTypeId(counterId) == RECORDING_POSITION_TYPE_ID &&
        countersReader.metaDataBuffer().getInt64(
            CountersReader::metadataOffset(counterId) + CountersReader::KEY_OFFSET + RECORDING_ID_OFFSET) == recordingId;
}

}

}}}

#endif //AERON_ARCHIVE_RECORDING_POS_H
