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

#ifndef INCLUDED_AERON_CONCURRENT_BROADCAST_RECORD_DESCRIPTOR__
#define INCLUDED_AERON_CONCURRENT_BROADCAST_RECORD_DESCRIPTOR__

#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include <util/BitUtil.h>

namespace aeron { namespace concurrent { namespace broadcast {

/*
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |R|                          Length                             |
 *  +-+-------------------------------------------------------------+
 *  |                             Type                              |
 *  +---------------------------------------------------------------+
 *  |                       Encoded Message                        ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 */

namespace RecordDescriptor {

static const std::int32_t PADDING_MSG_TYPE_ID = -1;

static const util::index_t LENGTH_OFFSET = 0;
static const util::index_t TYPE_OFFSET = 4;

static const util::index_t HEADER_LENGTH = 8;
static const util::index_t RECORD_ALIGNMENT = HEADER_LENGTH;

inline static std::int32_t calculateMaxMessageLength(util::index_t capacity)
{
    return capacity / 8;
}

inline static util::index_t lengthOffset(util::index_t recordOffset)
{
    return recordOffset + LENGTH_OFFSET;
}

inline static util::index_t typeOffset(util::index_t recordOffset)
{
    return recordOffset + TYPE_OFFSET;
}

inline static util::index_t msgOffset(util::index_t recordOffset)
{
    return recordOffset + HEADER_LENGTH;
}

inline static void checkMsgTypeId(std::int32_t msgTypeId)
{
    if (msgTypeId < 1)
    {
        throw util::IllegalArgumentException(
            util::strPrintf("Message type id must be greater than zero, msgTypeId=%d", msgTypeId), SOURCEINFO);
    }
}

}}}}

#endif
