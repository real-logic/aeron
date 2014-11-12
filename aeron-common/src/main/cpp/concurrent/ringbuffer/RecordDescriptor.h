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

#ifndef INCLUDED_AERON_CONCURRENT_RINGBUFFER_RECORD_DESCRIPTOR__
#define INCLUDED_AERON_CONCURRENT_RINGBUFFER_RECORD_DESCRIPTOR__

#include <util/Index.h>
#include <util/StringUtil.h>

namespace aeron { namespace common { namespace concurrent { namespace ringbuffer {

/**
* Header length made up of fields for record length, message length, message type, and reserved,
* and then the encoded message.
* <p>
* Writing of the record length signals the message recording is complete.
* <pre>
*   0        4        8        12       16 -byte position
*   +--------+--------+--------+--------+------------------------+
*   |rec len |msg len |msg type|reserve |encoded message.........|
*   +--------+--------+--------+--------+------------------------+
* </pre>
*/

namespace RecordDescriptor {

    static const util::index_t HEADER_LENGTH = sizeof(std::int32_t) * 4;
    static const util::index_t ALIGNMENT = 32;

    inline static util::index_t lengthOffset(util::index_t recordOffset)
    {
        return recordOffset;
    }

    inline static util::index_t msgLengthOffset(util::index_t recordOffset)
    {
        return recordOffset + sizeof(std::int32_t);
    }

    inline static util::index_t msgTypeOffset(util::index_t recordOffset)
    {
        return recordOffset + (sizeof(std::int32_t) * 2);
    }

    inline static util::index_t encodedMsgOffset(util::index_t recordOffset)
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
};

}}}}

#endif
