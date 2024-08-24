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

#ifndef AERON_C_ARCHIVE_CONFIGURATION_H
#define AERON_C_ARCHIVE_CONFIGURATION_H

#include "aeron_archive_proxy.h"

#define AERON_ARCHIVE_MAJOR_VERSION 1
#define AERON_ARCHIVE_MINOR_VERSION 11
#define AERON_ARCHIVE_PATCH_VERSION 0

int32_t aeron_archive_semantic_version(void);

int32_t aeron_archive_protocol_version_with_archive_id(void);

#endif //AERON_C_ARCHIVE_CONFIGURATION_H
