/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

static const std::int32_t CLEAN = 0;
static const std::int32_t NEEDS_CLEANING = 1;

static const util::index_t TERM_MIN_LENGTH = 64 * 1024;

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
 *  |      Term Meta Data 0      |
 *  +----------------------------+
 *  |      Term Meta Data 1      |
 *  +----------------------------+
 *  |      Term Meta Data 2      |
 *  +----------------------------+
 *  |        Log Meta Data       |
 *  +----------------------------+
 * </pre>
 */

static const util::index_t TERM_TAIL_COUNTER_OFFSET = (util::BitUtil::CACHE_LINE_LENGTH * 2);
static const util::index_t TERM_STATUS_OFFSET = (util::BitUtil::CACHE_LINE_LENGTH * 2) * 2;
static const util::index_t TERM_META_DATA_LENGTH = (util::BitUtil::CACHE_LINE_LENGTH * 2) * 3;

static const util::index_t LOG_META_DATA_SECTION_INDEX = PARTITION_COUNT * 2;

/**
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                   Active Partition Index                      |
 *  +---------------------------------------------------------------+
 *  |                      Cache Line Padding                      ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                       Time of Last SM                         |
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
    std::int32_t activePartitionIndex;
    std::int8_t pad1[(2 * util::BitUtil::CACHE_LINE_LENGTH) - sizeof(std::int32_t)];
    std::int64_t timeOfLastSm;
    std::int8_t pad2[(2 * util::BitUtil::CACHE_LINE_LENGTH) - sizeof(std::int64_t)];
    std::int64_t correlationId;
    std::int32_t initialTermId;
    std::int32_t defaultFrameHeaderLength;
    std::int32_t mtuLength;
    std::int8_t pad3[(util::BitUtil::CACHE_LINE_LENGTH) - (5 * sizeof(std::int32_t))];
};
#pragma pack(pop)

static const util::index_t LOG_ACTIVE_PARTITION_INDEX_OFFSET = offsetof(LogMetaDataDefn, activePartitionIndex);
static const util::index_t LOG_TIME_OF_LAST_SM_OFFSET = offsetof(LogMetaDataDefn, timeOfLastSm);
static const util::index_t LOG_INITIAL_TERM_ID_OFFSET = offsetof(LogMetaDataDefn, initialTermId);
static const util::index_t LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET = offsetof(LogMetaDataDefn, defaultFrameHeaderLength);
static const util::index_t LOG_MTU_LENGTH_OFFSET = offsetof(LogMetaDataDefn, mtuLength);
static const util::index_t LOG_DEFAULT_FRAME_HEADER_OFFSET = sizeof(LogMetaDataDefn);
static const util::index_t LOG_META_DATA_LENGTH = sizeof(LogMetaDataDefn) + LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH;

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

inline static void checkMetaDataBuffer(AtomicBuffer &buffer)
{
    const util::index_t capacity = buffer.capacity();
    if (capacity < TERM_META_DATA_LENGTH)
    {
        throw util::IllegalStateException(
            util::strPrintf("Meta Data buffer capacity less than min size of %d, capacity=%d",
                TERM_META_DATA_LENGTH, capacity), SOURCEINFO);
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
    static_assert(PARTITION_COUNT==3, "PARTITION_COUNT must be 3");
    return util::BitUtil::fastMod3(currentIndex + 1);
}

inline static int previousPartitionIndex(int currentIndex) AERON_NOEXCEPT
{
    static_assert(PARTITION_COUNT==3, "PARTITION_COUNT must be 3");
    return util::BitUtil::fastMod3(currentIndex + (PARTITION_COUNT - 1));
}

inline static std::int64_t timeOfLastSm(AtomicBuffer& logMetaDataBuffer) AERON_NOEXCEPT
{
    return logMetaDataBuffer.getInt64Volatile(LOG_TIME_OF_LAST_SM_OFFSET);
}

inline static int indexByTerm(std::int32_t initialTermId, std::int32_t activeTermId) AERON_NOEXCEPT
{
    static_assert(PARTITION_COUNT==3, "PARTITION_COUNT must be 3");
    return util::BitUtil::fastMod3(activeTermId - initialTermId);
}

inline static int indexByPosition(std::int64_t position, std::int32_t positionBitsToShift) AERON_NOEXCEPT
{
    static_assert(PARTITION_COUNT==3, "PARTITION_COUNT must be 3");
    return (int)(util::BitUtil::fastMod3((std::uint64_t)position >> positionBitsToShift));
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
    return (termLength * PARTITION_COUNT) + (TERM_META_DATA_LENGTH * PARTITION_COUNT) + LOG_META_DATA_LENGTH;
}

inline static std::int64_t computeTermLength(std::int64_t logLength)
{
    const std::int64_t metaDataSectionLength = (TERM_META_DATA_LENGTH * PARTITION_COUNT) + LOG_META_DATA_LENGTH;
    return (logLength - metaDataSectionLength) / 3;
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

};

}}}

#endif
