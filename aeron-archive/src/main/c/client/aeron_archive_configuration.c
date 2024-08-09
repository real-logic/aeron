/*
 * Copyright 2014-2024 Real Logic Limited.
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

#include "aeron_archive_configuration.h"

int32_t aeron_semantic_version_compose(uint8_t major, uint8_t minor, uint8_t patch)
{
    return (major << 16) | (minor << 8) | patch;
}

int32_t aeron_archive_semantic_version(void)
{
    return aeron_semantic_version_compose(AERON_ARCHIVE_MAJOR_VERSION, AERON_ARCHIVE_MINOR_VERSION, AERON_ARCHIVE_PATCH_VERSION);
}

int32_t aeron_archive_protocol_version_with_archive_id(void)
{
    return aeron_semantic_version_compose(1, 11, 0);
}
