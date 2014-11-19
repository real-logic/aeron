/*
 * Copyright 2014 Real Logic Ltd.
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
static const std::int32_t IN_CLEANING = 2;

static const util::index_t HIGH_WATER_MARK_OFFSET = 0;
static const util::index_t TAIL_COUNTER_OFFSET = sizeof(std::int32_t);
static const util::index_t STATUS_OFFSET = sizeof(std::int32_t) + util::BitUtil::CACHE_LINE_SIZE;
static const util::index_t STATE_BUFFER_LENGTH = util::BitUtil::CACHE_LINE_SIZE * 2;

static const util::index_t MIN_LOG_SIZE = 64 * 1024;

inline static void checkLogBuffer(AtomicBuffer& buffer)
{
    const util::index_t capacity = buffer.getCapacity();
    if (capacity < MIN_LOG_SIZE)
    {
        throw util::IllegalStateException(
            util::strPrintf("Log buffer capacity less than min size of %d, capacity=%d",
                MIN_LOG_SIZE, capacity), SOURCEINFO);
    }

    if ((capacity & (FrameDescriptor::FRAME_ALIGNMENT - 1)) != 0)
    {
        throw util::IllegalStateException(
            util::strPrintf("Log buffer capacity not a multiple of %d, capacity=%d",
                FrameDescriptor::FRAME_ALIGNMENT, capacity), SOURCEINFO);
    }
}

inline static void checkStateBuffer(AtomicBuffer& buffer)
{
    const util::index_t capacity = buffer.getCapacity();
    if (capacity < STATE_BUFFER_LENGTH)
    {
        throw util::IllegalStateException(
            util::strPrintf("State buffer capacity less than min size of %d, capacity=%d",
                STATE_BUFFER_LENGTH, capacity), SOURCEINFO);
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
};

}}}}

#endif
