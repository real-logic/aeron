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

#ifndef AERON_CONCURRENT_LOGBUFFER_TERM_READER_H
#define AERON_CONCURRENT_LOGBUFFER_TERM_READER_H

#include <functional>

#include "util/Index.h"
#include "concurrent/AtomicBuffer.h"
#include "Header.h"

namespace aeron { namespace concurrent { namespace logbuffer {

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
    concurrent::AtomicBuffer &buffer,
    util::index_t offset,
    util::index_t length,
    Header &header)> fragment_handler_t;

}}}

#endif
