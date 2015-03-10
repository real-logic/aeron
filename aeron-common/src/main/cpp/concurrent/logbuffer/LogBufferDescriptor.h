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
#include <concurrent/AtomicBuffer.h>
#include "FrameDescriptor.h"

namespace aeron { namespace common { namespace concurrent { namespace logbuffer {

namespace LogBufferDescriptor {

static const std::int32_t CLEAN = 0;
static const std::int32_t NEEDS_CLEANING = 1;

//static const util::index_t HIGH_WATER_MARK_OFFSET = 0;
//static const util::index_t TAIL_COUNTER_OFFSET = sizeof(std::int32_t);
//static const util::index_t STATUS_OFFSET = sizeof(std::int32_t) + util::BitUtil::CACHE_LINE_LENGTH;
//static const util::index_t STATE_BUFFER_LENGTH = util::BitUtil::CACHE_LINE_LENGTH * 2;

static const util::index_t TERM_MIN_LENGTH = 64 * 1024;

static const int PARTITION_COUNT = 3;

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

static const util::index_t TERM_HIGH_WATER_MARK_OFFSET = 0;
static const util::index_t TERM_TAIL_COUNTER_OFFSET = sizeof(std::int32_t);
static const util::index_t TERM_STATUS_OFFSET = sizeof(std::int32_t) + util::BitUtil::CACHE_LINE_LENGTH;
static const util::index_t TERM_META_DATA_LENGTH = util::BitUtil::CACHE_LINE_LENGTH * 2;

/**
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                        Initial Term Id                        |
 *  +---------------------------------------------------------------+
 *  |                        Active Term Id                         |
 *  +---------------------------------------------------------------+
 *  |                  Default Frame Header Length                  |
 *  +---------------------------------------------------------------+
 *  |                      Cache Line Padding                      ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                    Default Frame Header 0                    ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                    Default Frame Header 1                    ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                    Default Frame Header 2                    ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */

static const util::index_t LOG_META_DATA_LENGTH = util::BitUtil::CACHE_LINE_LENGTH * 4;

inline static void checkTermBuffer(AtomicBuffer &buffer)
{
    const util::index_t capacity = buffer.getCapacity();
    if (capacity < TERM_MIN_LENGTH)
    {
        throw util::IllegalStateException(
            util::strPrintf("Term buffer capacity less than min size of %d, capacity=%d",
                TERM_MIN_LENGTH, capacity), SOURCEINFO);
    }

    if ((capacity & (FrameDescriptor::FRAME_ALIGNMENT - 1)) != 0)
    {
        throw util::IllegalStateException(
            util::strPrintf("Term buffer capacity not a multiple of %d, capacity=%d",
                FrameDescriptor::FRAME_ALIGNMENT, capacity), SOURCEINFO);
    }
}

inline static void checkMetaDataBuffer(AtomicBuffer &buffer)
{
    const util::index_t capacity = buffer.getCapacity();
    if (capacity < TERM_META_DATA_LENGTH)
    {
        throw util::IllegalStateException(
            util::strPrintf("Meta Data buffer capacity less than min size of %d, capacity=%d",
                TERM_META_DATA_LENGTH, capacity), SOURCEINFO);
    }
}

inline static void checkMsgTypeId(std::int32_t msgTypeId)
{
    if (msgTypeId < 1)
    {
        throw util::IllegalArgumentException(
            util::strPrintf("Message type id must be greater than zero, msgTypeId=%d", msgTypeId), SOURCEINFO);
    }
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

};

}}}}

#endif
