/*
 * Copyright 2014-2019 Real Logic Ltd.
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
#ifndef AERON_ARCHIVE_CONFIGURATION_H
#define AERON_ARCHIVE_CONFIGURATION_H

#include <cstdint>
#include "util/MacroUtil.h"

namespace aeron {
namespace archive {
namespace client {

namespace Configuration
{
constexpr const std::uint8_t ARCHIVE_MAJOR_VERSION = 0;
constexpr const std::uint8_t ARCHIVE_MINOR_VERSION = 1;
constexpr const std::uint8_t ARCHIVE_PATCH_VERSION = 1;
constexpr const std::int32_t ARCHIVE_SEMANTIC_VERSION = aeron::util::semanticVersionCompose(
    ARCHIVE_MAJOR_VERSION, ARCHIVE_MINOR_VERSION, ARCHIVE_PATCH_VERSION);


}}}}
#endif //AERON_ARCHIVE_CONFIGURATION_H
