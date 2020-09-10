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

#ifndef AERON_CONCURRENT_LOGBUFFER_DESCRIPTOR_H
#define AERON_CONCURRENT_LOGBUFFER_DESCRIPTOR_H

#include "util/Index.h"
#include "util/StringUtil.h"
#include "util/BitUtil.h"
#include "concurrent/AtomicBuffer.h"
#include "FrameDescriptor.h"
#include "DataFrameHeader.h"

namespace aeron { namespace concurrent { namespace logbuffer { namespace LogBufferDescriptor
{

const std::int32_t TERM_MIN_LENGTH = 64 * 1024;
const std::int32_t TERM_MAX_LENGTH = 1024 * 1024 * 1024;
const std::int32_t AERON_PAGE_MIN_SIZE = 4 * 1024;
const std::int32_t AERON_PAGE_MAX_SIZE = 1024 * 1024 * 1024;

#if defined(__GNUC__) || _MSC_VER >= 1900
constexpr const int PARTITION_COUNT = 3;
#else
// Visual Studio 2013 doesn't like constexpr without an update
// https://msdn.microsoft.com/en-us/library/vstudio/hh567368.aspx
// https://www.microsoft.com/en-us/download/details.aspx?id=41151
const int PARTITION_COUNT = 3;
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

const util::index_t LOG_META_DATA_SECTION_INDEX = PARTITION_COUNT;

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
 *  |                    Active Transport Count                     |
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

const util::index_t LOG_DEFAULT_FRAME_HEADER_MAX_LENGTH = util::BitUtil::CACHE_LINE_LENGTH * 2;

#pragma pack(push)
#pragma pack(4)
struct LogMetaDataDefn
{
    std::int64_t termTailCounters[PARTITION_COUNT];
    std::int32_t activeTermCount;
    std::int8_t pad1[
        (2 * util::BitUtil::CACHE_LINE_LENGTH) - ((PARTITION_COUNT * sizeof(std::int64_t)) + sizeof(std::int32_t))];
    std::int64_t endOfStreamPosition;
    std::int32_t isConnected;
    std::int32_t activeTransportCount;
    std::int8_t pad2[(2 * util::BitUtil::CACHE_LINE_LENGTH) - (sizeof(std::int64_t) + (2 * sizeof(std::int32_t)))];
    std::int64_t correlationId;
    std::int32_t initialTermId;
    std::int32_t defaultFrameHeaderLength;
    std::int32_t mtuLength;
    std::int32_t termLength;
    std::int32_t pageSize;
    std::int8_t pad3[(util::BitUtil::CACHE_LINE_LENGTH) - (7 * sizeof(std::int32_t))];
};
#pragma pack(pop)

const util::index_t TERM_TAIL_COUNTER_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, termTailCounters);

const util::index_t LOG_ACTIVE_TERM_COUNT_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, activeTermCount);
const util::index_t LOG_END_OF_STREAM_POSITION_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, endOfStreamPosition);
const util::index_t LOG_IS_CONNECTED_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, isConnected);
const util::index_t LOG_ACTIVE_TRANSPORT_COUNT = (util::index_t)offsetof(LogMetaDataDefn, activeTransportCount);
const util::index_t LOG_INITIAL_TERM_ID_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, initialTermId);
const util::index_t LOG_DEFAULT_FRAME_HEADER_LENGTH_OFFSET =
    (util::index_t)offsetof(LogMetaDataDefn, defaultFrameHeaderLength);
const util::index_t LOG_MTU_LENGTH_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, mtuLength);
const util::index_t LOG_TERM_LENGTH_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, termLength);
const util::index_t LOG_PAGE_SIZE_OFFSET = (util::index_t)offsetof(LogMetaDataDefn, pageSize);
const util::index_t LOG_DEFAULT_FRAME_HEADER_OFFSET = (util::index_t)
sizeof(LogMetaDataDefn);
const util::index_t LOG_META_DATA_LENGTH = 4 * 1024;

inline void checkTermLength(std::int32_t termLength)
{
    if (termLength < TERM_MIN_LENGTH)
    {
        throw util::IllegalStateException(
            "term length less than min size of " + std::to_string(TERM_MIN_LENGTH) +
            ", length=" + std::to_string(termLength), SOURCEINFO);
    }

    if (termLength > TERM_MAX_LENGTH)
    {
        throw util::IllegalStateException(
            "term length greater than max size of " + std::to_string(TERM_MAX_LENGTH) +
            ", length=" + std::to_string(termLength), SOURCEINFO);
    }

    if (!util::BitUtil::isPowerOfTwo(termLength))
    {
        throw util::IllegalStateException(
            "term length not a power of 2, length=" + std::to_string(termLength), SOURCEINFO);
    }
}

inline void checkPageSize(std::int32_t pageSize)
{
    if (pageSize < AERON_PAGE_MIN_SIZE)
    {
        throw util::IllegalStateException(
            "page size less than min size of " + std::to_string(AERON_PAGE_MIN_SIZE) +
            ", size=" + std::to_string(pageSize), SOURCEINFO);
    }

    if (pageSize > AERON_PAGE_MAX_SIZE)
    {
        throw util::IllegalStateException(
            "page size greater than max size of " + std::to_string(AERON_PAGE_MAX_SIZE) +
            ", size=" + std::to_string(pageSize), SOURCEINFO);
    }

    if (!util::BitUtil::isPowerOfTwo(pageSize))
    {
        throw util::IllegalStateException(
            "page size not a power of 2, size=" + std::to_string(pageSize), SOURCEINFO);
    }
}

inline std::int32_t initialTermId(const AtomicBuffer &logMetaDataBuffer)
{
    return logMetaDataBuffer.getInt32(LOG_INITIAL_TERM_ID_OFFSET);
}

inline std::int32_t mtuLength(const AtomicBuffer &logMetaDataBuffer)
{
    return logMetaDataBuffer.getInt32(LOG_MTU_LENGTH_OFFSET);
}

inline std::int32_t termLength(const AtomicBuffer &logMetaDataBuffer)
{
    return logMetaDataBuffer.getInt32(LOG_TERM_LENGTH_OFFSET);
}

inline std::int32_t pageSize(const AtomicBuffer &logMetaDataBuffer)
{
    return logMetaDataBuffer.getInt32(LOG_PAGE_SIZE_OFFSET);
}

inline std::int32_t activeTermCount(const AtomicBuffer &logMetaDataBuffer)
{
    return logMetaDataBuffer.getInt32Volatile(LOG_ACTIVE_TERM_COUNT_OFFSET);
}

inline void activeTermCountOrdered(AtomicBuffer &logMetaDataBuffer, std::int32_t activeTermId)
{
    logMetaDataBuffer.putInt32Ordered(LOG_ACTIVE_TERM_COUNT_OFFSET, activeTermId);
}

inline bool casActiveTermCount(
    AtomicBuffer &logMetaDataBuffer, std::int32_t expectedTermCount, std::int32_t updateTermCount)
{
    return logMetaDataBuffer.compareAndSetInt32(LOG_ACTIVE_TERM_COUNT_OFFSET, expectedTermCount, updateTermCount);
}

inline int nextPartitionIndex(int currentIndex) noexcept
{
    return (currentIndex + 1) % PARTITION_COUNT;
}

inline int previousPartitionIndex(int currentIndex) noexcept
{
    return (currentIndex + (PARTITION_COUNT - 1)) % PARTITION_COUNT;
}

inline bool isConnected(const AtomicBuffer &logMetaDataBuffer) noexcept
{
    return logMetaDataBuffer.getInt32Volatile(LOG_IS_CONNECTED_OFFSET) == 1;
}

inline void isConnected(AtomicBuffer &logMetaDataBuffer, bool isConnected) noexcept
{
    logMetaDataBuffer.putInt32Ordered(LOG_IS_CONNECTED_OFFSET, isConnected ? 1 : 0);
}

inline std::int32_t activeTransportCount(AtomicBuffer &logMegaDataBuffer) noexcept
{
    return logMegaDataBuffer.getInt32Volatile(LOG_ACTIVE_TRANSPORT_COUNT);
}

inline void activeTransportCount(AtomicBuffer &logMetaDataBuffer, std::int32_t numberOfActiveTransports) noexcept
{
    logMetaDataBuffer.putInt32Ordered(LOG_ACTIVE_TRANSPORT_COUNT, numberOfActiveTransports);
}

inline std::int64_t endOfStreamPosition(const AtomicBuffer &logMetaDataBuffer) noexcept
{
    return logMetaDataBuffer.getInt64Volatile(LOG_END_OF_STREAM_POSITION_OFFSET);
}

inline void endOfStreamPosition(AtomicBuffer &logMetaDataBuffer, std::int64_t position) noexcept
{
    logMetaDataBuffer.putInt64Ordered(LOG_END_OF_STREAM_POSITION_OFFSET, position);
}

inline int indexByTerm(std::int32_t initialTermId, std::int32_t activeTermId) noexcept
{
    return (activeTermId - initialTermId) % PARTITION_COUNT;
}

inline int indexByTermCount(std::int64_t termCount) noexcept
{
    return static_cast<int>(termCount % PARTITION_COUNT);
}

inline int indexByPosition(std::int64_t position, std::int32_t positionBitsToShift) noexcept
{
    return static_cast<int>((static_cast<std::uint64_t>(position) >> positionBitsToShift) % PARTITION_COUNT);
}

inline std::int64_t computePosition(
    std::int32_t activeTermId,
    std::int32_t termOffset,
    std::int32_t positionBitsToShift,
    std::int32_t initialTermId) noexcept
{
    const std::int64_t termCount = activeTermId - initialTermId;
    return (termCount << positionBitsToShift) + termOffset;
}

inline std::int64_t computeTermBeginPosition(
    std::int32_t activeTermId, std::int32_t positionBitsToShift, std::int32_t initialTermId) noexcept
{
    const std::int64_t termCount = activeTermId - initialTermId;
    return termCount << positionBitsToShift;
}

inline std::int64_t rawTailVolatile(const AtomicBuffer &logMetaDataBuffer)
{
    const std::int32_t partitionIndex = indexByTermCount(activeTermCount(logMetaDataBuffer));
    return logMetaDataBuffer.getInt64Volatile(TERM_TAIL_COUNTER_OFFSET + (partitionIndex * sizeof(std::int64_t)));
}

inline std::int64_t rawTail(const AtomicBuffer &logMetaDataBuffer)
{
    const std::int32_t partitionIndex = indexByTermCount(activeTermCount(logMetaDataBuffer));
    return logMetaDataBuffer.getInt64(TERM_TAIL_COUNTER_OFFSET + (partitionIndex * sizeof(std::int64_t)));
}

inline std::int64_t rawTail(const AtomicBuffer &logMetaDataBuffer, int partitionIndex)
{
    return logMetaDataBuffer.getInt64(TERM_TAIL_COUNTER_OFFSET + (partitionIndex * sizeof(std::int64_t)));
}

inline std::int32_t termId(const std::int64_t rawTail)
{
    return static_cast<std::int32_t>(rawTail >> 32);
}

inline std::int32_t termOffset(const std::int64_t rawTail, const std::int64_t termLength)
{
    const std::int64_t tail = rawTail & 0xFFFFFFFFl;
    return static_cast<std::int32_t>(std::min(tail, termLength));
}

inline bool casRawTail(
    AtomicBuffer &logMetaDataBuffer, int partitionIndex, std::int64_t expectedRawTail, std::int64_t updateRawTail)
{
    return logMetaDataBuffer.compareAndSetInt64(
        TERM_TAIL_COUNTER_OFFSET + (partitionIndex * sizeof(std::int64_t)), expectedRawTail, updateRawTail);
}

inline AtomicBuffer defaultFrameHeader(AtomicBuffer &logMetaDataBuffer)
{
    std::uint8_t *header = logMetaDataBuffer.buffer() + LOG_DEFAULT_FRAME_HEADER_OFFSET;
    return { header, static_cast<std::size_t>(DataFrameHeader::LENGTH) };
}

inline void rotateLog(AtomicBuffer &logMetaDataBuffer, std::int32_t currentTermCount, std::int32_t currentTermId)
{
    const std::int32_t nextTermId = currentTermId + 1;
    const std::int32_t nextTermCount = currentTermCount + 1;
    const int nextIndex = indexByTermCount(nextTermCount);
    const std::int32_t expectedTermId = nextTermId - PARTITION_COUNT;
    const std::int64_t newRawTail = (nextTermId * ((INT64_C(1) << 32)));

    std::int64_t rawTail;
    do
    {
        rawTail = LogBufferDescriptor::rawTail(logMetaDataBuffer, nextIndex);
        if (expectedTermId != LogBufferDescriptor::termId(rawTail))
        {
            break;
        }
    }
    while (!LogBufferDescriptor::casRawTail(logMetaDataBuffer, nextIndex, rawTail, newRawTail));

    LogBufferDescriptor::casActiveTermCount(logMetaDataBuffer, currentTermCount, nextTermCount);
}

inline void initializeTailWithTermId(AtomicBuffer &logMetaDataBuffer, int partitionIndex, std::int32_t termId)
{
    const std::int64_t rawTail = (termId * ((INT64_C(1) << 32)));
    logMetaDataBuffer.putInt64(TERM_TAIL_COUNTER_OFFSET + (partitionIndex * sizeof(std::int64_t)), rawTail);
}

}

}}}

#endif
