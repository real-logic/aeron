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

#ifndef AERON_AERON_SYSTEM_COUNTERS_H
#define AERON_AERON_SYSTEM_COUNTERS_H

#include <stdint.h>
#include "concurrent/aeron_counters_manager.h"

typedef enum aeron_system_counter_enum_stct
{
    AERON_SYSTEM_COUNTER_BYTES_SENT = 1,
    AERON_SYSTEM_COUNTER_BYTES_RECEIVED = 2
}
aeron_system_counter_enum_t;

typedef struct aeron_system_counter_stct
{
    const char *label;
    int32_t id;
}
aeron_system_counter_t;

typedef struct aeron_system_counter_map_stct
{
    aeron_system_counter_enum_t type;
    int32_t counter_id;
    int64_t *addr;
}
aeron_system_counter_map_t;

typedef struct aeron_system_counters_stct
{
    aeron_counters_manager_t *manager;
    aeron_system_counter_map_t counters[];
}
aeron_system_counters_t;

int aeron_system_counters_init(aeron_system_counters_t *counters, aeron_counters_manager_t *manager);

#endif //AERON_AERON_SYSTEM_COUNTERS_H
