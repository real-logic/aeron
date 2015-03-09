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
#ifndef INCLUDED_AERON_COMMAND_READYFLYWEIGHT__
#define INCLUDED_AERON_COMMAND_READYFLYWEIGHT__

#include <cstdint>
#include <stddef.h>
#include <string>

namespace aeron { namespace common { namespace command {

// interface
template <typename concrete_t>
struct ReadyFlyweight
{
    virtual std::int32_t bufferOffset(std::int32_t index) const = 0;
    virtual concrete_t& bufferOffset(std::int32_t index, std::int32_t value) = 0;
    virtual std::int32_t bufferLength(std::int32_t index) const = 0;
    virtual concrete_t&  bufferLength(std::int32_t index, std::int32_t value) = 0;
    virtual std::string location(std::int32_t index) const = 0;
    virtual concrete_t& location(std::int32_t index, const std::string& value) = 0;

    virtual ~ReadyFlyweight() {}
};

}}};
#endif