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

static const std::int32_t TERM_MIN_LENGTH = 64 * 1024;
static const std::int32_t TERM_MAX_LENGTH = 1024 * 1024 * 1024;
static const std::int32_t PAGE_MIN_SIZE = 4 * 1024;
static const std::int32_t PAGE_MAX_SIZE = 1024 * 1024 * 1024;

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
 *  |                      Active Term Count                        |
 *  +---------------------------------------------------------------+
 *  |                      Cache Line Padding                      ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                    End of Stream Position                     |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        Is Connected                           |
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
 *  |                         Term Length                           |
 *  +---------------------------------------------------------------+
 *  |                          Page Size                            |
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
    std::int32_t activeTermCount;
    std::int8_t pad1[(2 * util::BitUtil::CACHE_LINE_LENGTH) - ((PARTITION_COUNT * sizeof(std::int64_t)) + sizeof(std::int32_t))];
    std::int64_t endOfStreamPosition;
    std::int32_t isConnected;
    std::int8_t pad2[(2 * util::BitUtil::CACHE_LINE_LENGTH) - (sizeof(std::int64_t) + sizeof(std::int32_t))];
    std::int64_t correlationId;
    std::int32_t initialTermId;
    std::int32_t defaultFrameHeaderLength;
    std::int32_t mtuLength;
    std::int32_t termLength;
    std::int32_t pageSize;
    std::int8_t pad3[(util::BitUtil::CACHE_LINE_LENGTH) - (7 * sizeof(std::int32_t))];
};
#pragma pack(pop)

static const util::index_t TERM_TAIL_COUNTER_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, termTailCounters);

static const util::index_t LOG_ACTIVE_TERM_COUNT_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, activeTermCount);
static const util::index_t LOG_END_OF_STREAM_POSITION_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, endOfStreamPosition);
static const util::index_t LOG_IS_CONNECTED_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, isConnected);
static const util::index_t LOG_INITIAL_TERM_ID_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, initialTermId);
static const util::index_t LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, defaultFrameHeaderLength);
static const util::index_t LOG_MTU_LENGTH_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, mtuLength);
static const util::index_t LOG_TERM_LENGTH_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, termLength);
static const util::index_t LOG_PAGE_SIZE_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, pageSize);
static const util::index_t LOG_DEFAULT_FRAME_HEADER_OFFSET = (util::index_t)sizeof(LogMetaDataDefn);
static const util::index_t LOG_META_DATA_LENGTH = 4 * 1024;

inline static void checkTermLength(std::int32_t termLength)
{
    if (termLength < TERM_MIN_LENGTH)
    {
        throw util::IllegalStateException(
            util::strPrintf("Term length less than min size of %d, length=%d",
                TERM_MIN_LENGTH, termLength), SOURCEINFO);
    }

    if (termLength > TERM_MAX_LENGTH)
    {
        throw util::IllegalStateException(
            util::strPrintf("Term length greater than max size of %d, length=%d",
                TERM_MAX_LENGTH, termLength), SOURCEINFO);
    }

    if (!util::BitUtil::isPowerOfTwo(termLength))
    {
        throw util::IllegalStateException(
            util::strPrintf("Term length not a power of 2, length=%d", termLength), SOURCEINFO);
    }
}

inline static void checkPageSize(std::int32_t pageSize)
{
    if (pageSize < PAGE_MIN_SIZE)
    {
        throw util::IllegalStateException(
            util::strPrintf("Page size less than min size of %d, size=%d",
                PAGE_MIN_SIZE, pageSize), SOURCEINFO);
    }

    if (pageSize > PAGE_MAX_SIZE)
    {
        throw util::IllegalStateException(
            util::strPrintf("Page Size greater than max size of %d, size=%d",
                PAGE_MAX_SIZE, pageSize), SOURCEINFO);
    }

    if (!util::BitUtil::isPowerOfTwo(pageSize))
    {
        throw util::IllegalStateException(
            util::strPrintf("Page size not a power of 2, size=%d", pageSize), SOURCEINFO);
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

inline static std::int32_t termLength(AtomicBuffer& logMetaDataBuffer)
{
    return logMetaDataBuffer.getInt32(LOG_TERM_LENGTH_OFFSET);
}

inline static std::int32_t pageSize(AtomicBuffer& logMetaDataBuffer)
{
    return logMetaDataBuffer.getInt32(LOG_PAGE_SIZE_OFFSET);
}

inline static std::int32_t activeTermCount(AtomicBuffer& logMetaDataBuffer)
{
    return logMetaDataBuffer.getInt32Volatile(LOG_ACTIVE_TERM_COUNT_OFFSET);
}

inline static void activeTermCountOrdered(AtomicBuffer& logMetaDataBuffer, std::int32_t activeTermId)
{
    logMetaDataBuffer.putInt32Ordered(LOG_ACTIVE_TERM_COUNT_OFFSET, activeTermId);
}

inline static bool casActiveTermCount(
    AtomicBuffer& logMetaDataBuffer, std::int32_t expectedTermCount, std::int32_t updateTermCount)
{
    return logMetaDataBuffer.compareAndSetInt32(LOG_ACTIVE_TERM_COUNT_OFFSET, expectedTermCount, updateTermCount);
}

inline static int nextPartitionIndex(int currentIndex) AERON_NOEXCEPT
{
    return (currentIndex + 1) % PARTITION_COUNT;
}

inline static int previousPartitionIndex(int currentIndex) AERON_NOEXCEPT
{
    return (currentIndex + (PARTITION_COUNT - 1)) % PARTITION_COUNT;
}

inline static bool isConnected(AtomicBuffer &logMetaDataBuffer) AERON_NOEXCEPT
{
    return (logMetaDataBuffer.getInt32Volatile(LOG_IS_CONNECTED_OFFSET) == 1);
}

inline static void isConnected(AtomicBuffer &logMetaDataBuffer, bool isConnected) AERON_NOEXCEPT
{
    logMetaDataBuffer.putInt32Ordered(LOG_IS_CONNECTED_OFFSET, isConnected ? 1 : 0);
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

inline static int indexByTermCount(std::int64_t termCount) AERON_NOEXCEPT
{
    return static_cast<int>(termCount % PARTITION_COUNT);
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

inline static std::int64_t rawTailVolatile(AtomicBuffer& logMetaDataBuffer)
{
    const std::int32_t partitionIndex = indexByTermCount(activeTermCount(logMetaDataBuffer));
    return logMetaDataBuffer.getInt64Volatile(TERM_TAIL_COUNTER_OFFSET + (partitionIndex * sizeof(std::int64_t)));
}

inline static std::int64_t rawTail(AtomicBuffer& logMetaDataBuffer)
{
    const std::int32_t partitionIndex = indexByTermCount(activeTermCount(logMetaDataBuffer));
    return logMetaDataBuffer.getInt64(TERM_TAIL_COUNTER_OFFSET + (partitionIndex * sizeof(std::int64_t)));
}

inline static std::int64_t rawTail(AtomicBuffer& logMetaDataBuffer, int partitionIndex)
{
    return logMetaDataBuffer.getInt64(TERM_TAIL_COUNTER_OFFSET + (partitionIndex * sizeof(std::int64_t)));
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

inline static bool casRawTail(
    AtomicBuffer& logMetaDataBuffer, int partitionIndex, std::int64_t expectedRawTail, std::int64_t updateRawTail)
{
    return logMetaDataBuffer.compareAndSetInt64(
        TERM_TAIL_COUNTER_OFFSET + (partitionIndex * sizeof(std::int64_t)), expectedRawTail, updateRawTail);
}

inline static AtomicBuffer defaultFrameHeader(AtomicBuffer& logMetaDataBuffer)
{
    std::uint8_t *header =
        logMetaDataBuffer.buffer() + LOG_DEFAULT_FRAME_HEADER_OFFSET;

    return AtomicBuffer(header, DataFrameHeader::LENGTH);
}

inline static void rotateLog(AtomicBuffer& logMetaDataBuffer, std::int32_t currentTermCount, std::int32_t currentTermId)
{
    const std::int32_t nextTermId = currentTermId + 1;
    const std::int32_t nextTermCount = currentTermCount + 1;
    const int nextIndex = indexByTermCount(nextTermCount);
    const std::int32_t expectedTermId = nextTermId - PARTITION_COUNT;

    std::int64_t rawTail;
    do
    {
        rawTail = LogBufferDescriptor::rawTail(logMetaDataBuffer, nextIndex);
        if (expectedTermId != LogBufferDescriptor::termId(rawTail))
        {
            break;
        }
    }
    while (!LogBufferDescriptor::casRawTail(
        logMetaDataBuffer, nextIndex, rawTail, (static_cast<std::int64_t>(nextTermId)) << 32));

    LogBufferDescriptor::casActiveTermCount(logMetaDataBuffer, currentTermCount, nextTermCount);
}

inline static void initializeTailWithTermId(AtomicBuffer& logMetaDataBuffer, int partitionIndex, std::int32_t termId)
{
    logMetaDataBuffer.putInt64(
        TERM_TAIL_COUNTER_OFFSET + (partitionIndex * sizeof(std::int64_t)), (static_cast<std::int64_t>(termId)) << 32);
}

}

}}}

#endif
