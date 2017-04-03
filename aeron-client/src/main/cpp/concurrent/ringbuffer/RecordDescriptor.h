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

#ifndef INCLUDED_AERON_CONCURRENT_RINGBUFFER_RECORD_DESCRIPTOR__
#define INCLUDED_AERON_CONCURRENT_RINGBUFFER_RECORD_DESCRIPTOR__

#include <util/Index.h>
#include <util/StringUtil.h>

namespace aeron { namespace concurrent { namespace ringbuffer {

/**
* Header length made up of fields for message length, message type, and then the encoded message.
* <p>
* Writing of the record length signals the message recording is complete.
* <pre>
*   0                   1                   2                   3
*   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
*  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*  |R|                       Record Length                         |
*  +-+-------------------------------------------------------------+
*  |                              Type                             |
*  +---------------------------------------------------------------+
*  |                       Encoded Message                        ...
* ...                                                              |
*  +---------------------------------------------------------------+
* </pre>
*/

namespace RecordDescriptor {

    static const util::index_t HEADER_LENGTH = sizeof(std::int32_t) * 2;
    static const util::index_t ALIGNMENT = HEADER_LENGTH;
    static const std::int32_t PADDING_MSG_TYPE_ID = -1;

    inline static util::index_t lengthOffset(util::index_t recordOffset)
    {
        return recordOffset;
    }

    inline static util::index_t typeOffset(util::index_t recordOffset)
    {
        return recordOffset + sizeof(std::int32_t);
    }

    inline static util::index_t encodedMsgOffset(util::index_t recordOffset)
    {
        return recordOffset + HEADER_LENGTH;
    }

    inline static std::int64_t makeHeader(std::int32_t length, std::int32_t msgTypeId)
    {
        return (((std::int64_t)msgTypeId & 0xFFFFFFFF) << 32) | (length & 0xFFFFFFFF);
    }

    inline static std::int32_t recordLength(std::int64_t header)
    {
        return (std::int32_t)header;
    }

    inline static std::int32_t messageTypeId(std::int64_t header)
    {
        return (std::int32_t)(header >> 32);
    }

    inline static void checkMsgTypeId(std::int32_t msgTypeId)
    {
        if (msgTypeId < 1)
        {
            throw util::IllegalArgumentException(
                util::strPrintf("Message type id must be greater than zero, msgTypeId=%d", msgTypeId), SOURCEINFO);
        }
    }
}

}}}

#endif
