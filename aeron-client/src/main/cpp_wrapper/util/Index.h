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
#ifndef AERON_UTIL_INDEX_FILE_H
#define AERON_UTIL_INDEX_FILE_H

#include <cstddef>
#include <cstdint>
#include <limits>

namespace aeron { namespace util
{

/**
 * a 32-bit signed int that is used for lengths and offsets to be compatible with Java's 32-bit int.
 */
typedef std::int32_t index_t;

inline static index_t convertSizeToIndex(std::size_t size)
{
    if (size > static_cast<std::size_t>(std::numeric_limits<index_t>::max()))
    {
        return std::numeric_limits<index_t>::max();
    }

    return static_cast<index_t>(size);
}

}}

#endif
