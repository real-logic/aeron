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

#ifndef INCLUDED_AERON_UTIL_INDEX_FILE__
#define INCLUDED_AERON_UTIL_INDEX_FILE__

#include <cstdint>

namespace aeron { namespace common { namespace util {

// a 32bit signed int that is to be used for sizes and offsets to be compatible with
// java's signed 32 bit int.
typedef std::int32_t index_t;

}}}

#endif
