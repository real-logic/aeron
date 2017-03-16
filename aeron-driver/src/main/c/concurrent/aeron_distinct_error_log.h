/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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

#ifndef AERON_AERON_DISTINCT_ERROR_LOG_H
#define AERON_AERON_DISTINCT_ERROR_LOG_H

#include <stdint.h>
#include "aeronmd.h"

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_error_log_entry_stct
{
    int32_t length;
    int32_t observation_count;
    int64_t last_observation_timestamp;
    int64_t first_observation_timestamp;
}
aeron_error_log_entry_t;
#pragma pack(pop)

#define AERON_ERROR_LOG_HEADER_LENGTH (sizeof(aeron_error_log_entry_t))
#define AERON_ERROR_LOG_RECORD_ALIGNMENT (sizeof(int64_t))

typedef struct aeron_distinct_error_log_stct
{
    uint8_t *buffer;
    aeron_clock_func_t clock;
}
aeron_distinct_error_log_t;

#endif //AERON_AERON_DISTINCT_ERROR_LOG_H
