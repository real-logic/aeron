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

#ifndef INCLUDED_AERON_COMMON_TERMHELPER__
#define INCLUDED_AERON_COMMON_TERMHELPER__

#include <type_traits>
#include <cstdint>

#include <util/BitUtil.h>

namespace aeron { namespace common { namespace common {

namespace TermHelper
{
    const std::int32_t BUFFER_COUNT = 3;

    inline std::int32_t rotateNext(std::int32_t current)
    {
        return util::BitUtil::next(current, BUFFER_COUNT);
    }

    inline std::int32_t rotatePrevious(std::int32_t current)
    {
        return util::BitUtil::previous(current, BUFFER_COUNT);
    }
}

}}};

#endif