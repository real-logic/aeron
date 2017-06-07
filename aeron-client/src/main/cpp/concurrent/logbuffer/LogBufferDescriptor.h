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

#ifndef INCLUDED_AERON_CONCURRENT_LOGBUFFER_LOG_BUFFER_DESCRIPTOR__
#define INCLUDED_AERON_CONCURRENT_LOGBUFFER_LOG_BUFFER_DESCRIPTOR__

#include <util/Index.h>
#include <util/StringUtil.h>
#include <util/BitUtil.h>
#include <util/Exceptions.h>
#include <concurrent/AtomicBuffer.h>
#include "FrameDescriptor.h"
#include "DataFrameHeader.h"

namespace aeron { namespace concurrent { namespace logbuffer {

namespace LogBufferDescriptor {

static const util::index_t TERM_MIN_LENGTH = 64 * 1024;
static const std::int64_t MAX_SINGLE_MAPPING_SIZE = 0x7FFFFFFF;

#if defined(__GNUC__) || _MSC_VER >= 1900
constexpr static const int PARTITION_COUNT = 3;
#else
// Visual Studio 2013 doesn't like constexpr without an update
// https://msdn.microsoft.com/en-us/library/vstudio/hh567368.aspx
// https://www.microsoft.com/en-us/download/details.aspx?id=41151
static const int PARTITION_COUNT = 3;
#endif

/*
 * Layout description for log buffers which contains partitions of terms with associated term meta data,
 * plus ending with overall log meta data.
 *
 * <pre>
 *  +----------------------------+
 *  |           Term 0           |
 *  +----------------------------+
 *  |           Term 1           |
 *  +----------------------------+
 *  |           Term 2           |
 *  +----------------------------+
 *  |        Log Meta Data       |
 *  +----------------------------+
 * </pre>
 */

static const util::index_t LOG_META_DATA_SECTION_INDEX = PARTITION_COUNT;

/**
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                       Tail Counter 0                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                       Tail Counter 1                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                       Tail Counter 2                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                   Active Partition Index                      |
 *  +---------------------------------------------------------------+
 *  |                      Cache Line Padding                      ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                 Time of Last Status Message                   |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                    End of Stream Position                     |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                      Cache Line Padding                      ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                 Registration / Correlation ID                 |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        Initial Term Id                        |
 *  +---------------------------------------------------------------+
 *  |                  Default Frame Header Length                  |
 *  +---------------------------------------------------------------+
 *  |                          MTU Length                           |
 *  +---------------------------------------------------------------+
 *  |                      Cache Line Padding                      ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                    Default Frame Header                      ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */

static const util::index_t LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH = util::BitUtil::CACHE_LINE_LENGTH * 2;

#pragma pack(push)
#pragma pack(4)
struct LogMetaDataDefn
{
    std::int64_t termTailCounters[PARTITION_COUNT];
    std::int32_t activePartitionIndex;
    std::int8_t pad1[(2 * util::BitUtil::CACHE_LINE_LENGTH) - ((PARTITION_COUNT * sizeof(std::int64_t)) + sizeof(std::int32_t))];
    std::int64_t timeOfLastStatusMessage;
    std::int64_t endOfStreamPosition;
    std::int8_t pad2[(2 * util::BitUtil::CACHE_LINE_LENGTH) - (2 * sizeof(std::int64_t))];
    std::int64_t correlationId;
    std::int32_t initialTermId;
    std::int32_t defaultFrameHeaderLength;
    std::int32_t mtuLength;
    std::int8_t pad3[(util::BitUtil::CACHE_LINE_LENGTH) - (5 * sizeof(std::int32_t))];
};
#pragma pack(pop)

static const util::index_t TERM_TAIL_COUNTER_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, termTailCounters);

static const util::index_t LOG_ACTIVE_PARTITION_INDEX_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, activePartitionIndex);
static const util::index_t LOG_TIME_OF_LAST_STATUS_MESSAGE_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, timeOfLastStatusMessage);
static const util::index_t LOG_END_OF_STREAM_POSITION_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, endOfStreamPosition);
static const util::index_t LOG_INITIAL_TERM_ID_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, initialTermId);
static const util::index_t LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, defaultFrameHeaderLength);
static const util::index_t LOG_MTU_LENGTH_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, mtuLength);
static const util::index_t LOG_DEFAULT_FRAME_HEADER_OFFSET = (util::index_t)sizeof(LogMetaDataDefn);
static const util::index_t LOG_META_DATA_LENGTH = (util::index_t)sizeof(LogMetaDataDefn) + LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH;

inline static void checkTermLength(std::int64_t termLength)
{
    if (termLength < TERM_MIN_LENGTH)
    {
        throw util::IllegalStateException(
            util::strPrintf("Term length less than min size of %d, length=%d",
                TERM_MIN_LENGTH, termLength), SOURCEINFO);
    }

    if ((termLength & (FrameDescriptor::FRAME_ALIGNMENT - 1)) != 0)
    {
        throw util::IllegalStateException(
            util::strPrintf("Term length not a multiple of %d, length=%d",
                FrameDescriptor::FRAME_ALIGNMENT, termLength), SOURCEINFO);
    }
}

inline static std::int32_t initialTermId(AtomicBuffer& logMetaDataBuffer)
{
    return logMetaDataBuffer.getInt32(LOG_INITIAL_TERM_ID_OFFSET);
}

inline static std::int32_t mtuLength(AtomicBuffer& logMetaDataBuffer)
{
    return logMetaDataBuffer.getInt32(LOG_MTU_LENGTH_OFFSET);
}

inline static std::int32_t activePartitionIndex(AtomicBuffer& logMetaDataBuffer)
{
    return logMetaDataBuffer.getInt32Volatile(LOG_ACTIVE_PARTITION_INDEX_OFFSET);
}

inline static void activePartitionIndex(AtomicBuffer& logMetaDataBuffer, std::int32_t activeTermId)
{
    logMetaDataBuffer.putInt32Ordered(LOG_ACTIVE_PARTITION_INDEX_OFFSET, activeTermId);
}

inline static int nextPartitionIndex(int currentIndex) AERON_NOEXCEPT
{
    return (currentIndex + 1) % PARTITION_COUNT;
}

inline static int previousPartitionIndex(int currentIndex) AERON_NOEXCEPT
{
    return (currentIndex + (PARTITION_COUNT - 1)) % PARTITION_COUNT;
}

inline static std::int64_t timeOfLastStatusMessage(AtomicBuffer &logMetaDataBuffer) AERON_NOEXCEPT
{
    return logMetaDataBuffer.getInt64Volatile(LOG_TIME_OF_LAST_STATUS_MESSAGE_OFFSET);
}

inline static void timeOfLastStatusMessage(AtomicBuffer &logMetaDataBuffer, std::int64_t value) AERON_NOEXCEPT
{
    logMetaDataBuffer.putInt64Ordered(LOG_TIME_OF_LAST_STATUS_MESSAGE_OFFSET, value);
}

inline static std::int64_t endOfStreamPosition(AtomicBuffer &logMetaDataBuffer) AERON_NOEXCEPT
{
    return logMetaDataBuffer.getInt64Volatile(LOG_END_OF_STREAM_POSITION_OFFSET);
}

inline static void endOfStreamPosition(AtomicBuffer &logMetaDataBuffer, std::int64_t position) AERON_NOEXCEPT
{
    logMetaDataBuffer.putInt64Ordered(LOG_END_OF_STREAM_POSITION_OFFSET, position);
}

inline static int indexByTerm(std::int32_t initialTermId, std::int32_t activeTermId) AERON_NOEXCEPT
{
    return (activeTermId - initialTermId) % PARTITION_COUNT;
}

inline static int indexByPosition(std::int64_t position, std::int32_t positionBitsToShift) AERON_NOEXCEPT
{
    return (int)((std::uint64_t)position >> positionBitsToShift) % PARTITION_COUNT;
}

inline static std::int64_t computePosition(
    std::int32_t activeTermId, std::int32_t termOffset, std::int32_t positionBitsToShift, std::int32_t initialTermId) AERON_NOEXCEPT
{
    const std::int64_t termCount = activeTermId - initialTermId;

    return (termCount << positionBitsToShift) + termOffset;
}

inline static std::int64_t computeTermBeginPosition(
    std::int32_t activeTermId, std::int32_t positionBitsToShift, std::int32_t initialTermId) AERON_NOEXCEPT
{
    const std::int64_t termCount = activeTermId - initialTermId;

    return termCount << positionBitsToShift;
}

inline static std::int64_t computeLogLength(std::int64_t termLength)
{
    return (termLength * PARTITION_COUNT) + LOG_META_DATA_LENGTH;
}

inline static std::int64_t computeTermLength(std::int64_t logLength)
{
    return (logLength - LOG_META_DATA_LENGTH) / PARTITION_COUNT;
}

inline static AtomicBuffer defaultFrameHeader(AtomicBuffer& logMetaDataBuffer)
{
    std::uint8_t *header =
        logMetaDataBuffer.buffer() + LOG_DEFAULT_FRAME_HEADER_OFFSET;

    return AtomicBuffer(header, DataFrameHeader::LENGTH);
}

inline static std::int32_t termId(const std::int64_t rawTail)
{
    return static_cast<std::int32_t>(rawTail >> 32);
}

inline static std::int32_t termOffset(const std::int64_t rawTail, const std::int64_t termLength)
{
    const std::int64_t tail = rawTail & 0xFFFFFFFFl;

    return static_cast<std::int32_t>(std::min(tail, termLength));
}

inline static std::int64_t rawTailVolatile(AtomicBuffer& logMetaDataBuffer)
{
    const std::int32_t partitionIndex = activePartitionIndex(logMetaDataBuffer);
    return logMetaDataBuffer.getInt64Volatile(TERM_TAIL_COUNTER_OFFSET + (partitionIndex * sizeof(std::int64_t)));
}

}

}}}

#endif
