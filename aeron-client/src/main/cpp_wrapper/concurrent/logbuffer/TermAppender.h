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

#ifndef AERON_CONCURRENT_LOGBUFFER_TERM_APPENDER_H
#define AERON_CONCURRENT_LOGBUFFER_TERM_APPENDER_H

#include <functional>

#include "util/Index.h"
#include "concurrent/AtomicBuffer.h"

namespace aeron { namespace concurrent { namespace logbuffer {

/**
 * Supplies the reserved value field for a data frame header. The returned value will be set in the header as
 * Little Endian format.
 *
 * This will be called as the last action of encoding a data frame right before the length is set. All other fields
 * in the header plus the body of the frame will have been written at the point of supply.
 *
 * @param termBuffer for the message
 * @param termOffset of the start of the message
 * @param length of the message in bytes
 */
typedef std::function<std::int64_t(
    AtomicBuffer &termBuffer,
    util::index_t termOffset,
    util::index_t length)> on_reserved_value_supplier_t;

static const on_reserved_value_supplier_t DEFAULT_RESERVED_VALUE_SUPPLIER =
    [](AtomicBuffer &, util::index_t, util::index_t) -> std::int64_t { return 0; };

}}}

#endif
