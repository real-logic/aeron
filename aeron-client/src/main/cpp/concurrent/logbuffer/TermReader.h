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

#ifndef INCLUDED_AERON_CONCURRENT_LOGBUFFER_TERM_READER__
#define INCLUDED_AERON_CONCURRENT_LOGBUFFER_TERM_READER__

#include <functional>
#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include "LogBufferDescriptor.h"
#include "Header.h"

namespace aeron {

/** Concurrent operations and data structures */
namespace concurrent {

/** Logbuffer data structure */
namespace logbuffer {

/**
 * Callback for handling fragments of data being read from a log.
 *
 * Handler for reading data that is coming from a log buffer. The frame will either contain a whole message
 * or a fragment of a message to be reassembled. Messages are fragmented if greater than the frame for MTU in length.

 * @param buffer containing the data.
 * @param offset at which the data begins.
 * @param length of the data in bytes.
 * @param header representing the meta data for the data.
 */
typedef std::function<void(
    concurrent::AtomicBuffer& buffer,
    util::index_t offset,
    util::index_t length,
    Header& header)> fragment_handler_t;

/**
 * Callback to indicate an exception has occurred.
 *
 * @param exception that has occurred.
 */
typedef std::function<void(
    const std::exception& exception)> exception_handler_t;

namespace TermReader {

struct ReadOutcome
{
    std::int32_t offset;
    int fragmentsRead;
};

template <typename F>
inline void read(
    ReadOutcome& outcome,
    AtomicBuffer& termBuffer,
    std::int32_t termOffset,
    F&& handler,
    int fragmentsLimit,
    Header& header,
    const exception_handler_t & exceptionHandler)
{
    outcome.fragmentsRead = 0;
    outcome.offset = termOffset;
    const util::index_t capacity = termBuffer.capacity();

    try
    {
        do
        {
            const std::int32_t frameLength = FrameDescriptor::frameLengthVolatile(termBuffer, termOffset);
            if (frameLength <= 0)
            {
                break;
            }

            const std::int32_t fragmentOffset = termOffset;
            termOffset += util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);

            if (!FrameDescriptor::isPaddingFrame(termBuffer, fragmentOffset))
            {
                header.buffer(termBuffer);
                header.offset(fragmentOffset);
                handler(termBuffer, fragmentOffset + DataFrameHeader::LENGTH, frameLength - DataFrameHeader::LENGTH,
                    header);

                ++outcome.fragmentsRead;
            }
        }
        while (outcome.fragmentsRead < fragmentsLimit && termOffset < capacity);
    }
    catch (const std::exception& ex)
    {
        exceptionHandler(ex);
    }

    outcome.offset = termOffset;
}

}

}}}

#endif
