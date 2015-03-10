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

#ifndef INCLUDED_AERON_CONCURRENT_LOGBUFFER_FRAME_DESCRIPTOR__
#define INCLUDED_AERON_CONCURRENT_LOGBUFFER_FRAME_DESCRIPTOR__

#include <util/Index.h>
#include <util/StringUtil.h>
#include <concurrent/AtomicBuffer.h>

namespace aeron { namespace common { namespace concurrent { namespace logbuffer {

/**
* Description of the structure for message framing in a log buffer.
*
* All messages are logged in frames that have a minimum header layout as follows plus a reserve then
* the encoded message follows:
*
* <pre>
*   0                   1                   2                   3
*   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
*  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*  |  Version      |B|E| Flags     |             Type              |
*  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
*  |R|                       Frame Length                          |
*  +-+-------------------------------------------------------------+
*  |R|                       Term Offset                           |
*  +-+-------------------------------------------------------------+
*  |                      Additional Fields                       ...
* ...                                                              |
*  +---------------------------------------------------------------+
*  |                        Encoded Message                       ...
* ...                                                              |
*  +---------------------------------------------------------------+
* </pre>
*
* The (B)egin and (E)nd flags are used for message fragmentation. R is for reserved bit.
* Both are set for a message that does not span frames.
*/

namespace FrameDescriptor {

static const util::index_t FRAME_ALIGNMENT = 8;
static const util::index_t WORD_ALIGNMENT = sizeof(std::int64_t);

static const std::uint8_t BEGIN_FRAG = 0x80;
static const std::uint8_t END_FRAG = 0x40;
static const std::uint8_t UNFRAGMENTED = BEGIN_FRAG | END_FRAG;

static const util::index_t BASE_HEADER_LENGTH = 12;

static const util::index_t VERSION_OFFSET = 0;
static const util::index_t FLAGS_OFFSET = 1;
static const util::index_t TYPE_OFFSET = 2;
static const util::index_t LENGTH_OFFSET = 4;
static const util::index_t TERM_OFFSET = 8;

static const std::uint16_t PADDING_FRAME_TYPE = 0;

inline static void checkHeaderLength(util::index_t length)
{
    if (length < BASE_HEADER_LENGTH)
    {
        throw util::IllegalStateException(
            util::strPrintf("Frame header length must not be less than %d, length=%d", BASE_HEADER_LENGTH, length), SOURCEINFO);
    }

    if (length % WORD_ALIGNMENT != 0)
    {
        throw util::IllegalStateException(
            util::strPrintf("Frame header length must be a multiple of %d, length=%d", WORD_ALIGNMENT, length), SOURCEINFO);
    }
}

inline static void checkOffsetAlignment(util::index_t offset)
{
    if ((offset & (FRAME_ALIGNMENT - 1)) != 0)
    {
        throw util::IllegalArgumentException(
            util::strPrintf("Cannot seek to an offset that isn't a multiple of %d", FRAME_ALIGNMENT), SOURCEINFO);
    }
}

inline static void checkMaxFrameLength(util::index_t length)
{
    if ((length & (FRAME_ALIGNMENT - 1)) != 0)
    {
        throw util::IllegalStateException(
            util::strPrintf("Max frame length must be a multiple of %d, length=%d", FRAME_ALIGNMENT, length), SOURCEINFO);
    }
}

inline static util::index_t calculateMaxMessageLength(util::index_t capacity)
{
    return capacity / 8;
}

inline static util::index_t typeOffset(util::index_t frameOffset)
{
    return frameOffset + TYPE_OFFSET;
}

inline static util::index_t flagsOffset(util::index_t frameOffset)
{
    return frameOffset + FLAGS_OFFSET;
}

inline static util::index_t lengthOffset(util::index_t frameOffset)
{
    return frameOffset + LENGTH_OFFSET;
}

inline static util::index_t termOffsetOffset(util::index_t frameOffset)
{
    return frameOffset + TERM_OFFSET;
}

inline static void frameType(AtomicBuffer& logBuffer, util::index_t frameOffset, std::uint16_t type)
{
    logBuffer.putUInt16(typeOffset(frameOffset), type);
}

inline static void frameFlags(AtomicBuffer& logBuffer, util::index_t frameOffset, std::uint8_t flags)
{
    logBuffer.putUInt8(flagsOffset(frameOffset), flags);
}

inline static void frameTermOffset(AtomicBuffer& logBuffer, util::index_t frameOffset, std::int32_t termOffset)
{
    logBuffer.putInt32(termOffsetOffset(frameOffset), termOffset);
}

inline static bool isPaddingFrame(AtomicBuffer& logBuffer, util::index_t frameOffset)
{
    return logBuffer.getUInt16(typeOffset(frameOffset)) == PADDING_FRAME_TYPE;
}

inline static std::int32_t frameLengthVolatile(AtomicBuffer& logBuffer, util::index_t frameOffset)
{
    // TODO: need to byte order to LITTLE_ENDIAN
    return logBuffer.getInt32Volatile(lengthOffset(frameOffset));
}

inline static void frameLengthOrdered(AtomicBuffer& logBuffer, util::index_t frameOffset, std::int32_t frameLength)
{
    // TODO: need to byte order to LITTLE_ENDIAN
    logBuffer.putInt32Ordered(lengthOffset(frameOffset), frameLength);
}

};

}}}}

#endif
