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

#include <string.h>
#include "aeron_system_counters.h"
#include "aeron_alloc.h"

static aeron_system_counter_t system_counters[] =
    {
        { "Bytes sent", AERON_SYSTEM_COUNTER_BYTES_SENT },
        { "Bytes received", AERON_SYSTEM_COUNTER_BYTES_RECEIVED }
    };

static size_t num_system_counters = sizeof(system_counters)/sizeof(aeron_system_counter_t);

static void system_counter_key_func(uint8_t *key, size_t key_max_length, void *clientd)
{
    int32_t key_value = *(int32_t *)(clientd);

    *(int32_t *)key = key_value;
}

int aeron_system_counters_init(aeron_system_counters_t *counters, aeron_counters_manager_t *manager)
{
    if (NULL == counters || NULL == manager)
    {
        /* TODO: EINVAL */
        return -1;
    }

    counters->manager = manager;
    if (aeron_alloc((void **)&counters->counters, sizeof(aeron_system_counter_map_t) * num_system_counters) < 0)
    {
        return -1;
    }

    for (size_t i = 0; i < num_system_counters; i++)
    {
        if ((counters->counters[i].counter_id =
            aeron_counters_manager_allocate(
                manager, system_counters[i].label, strlen(system_counters[i].label), 0, system_counter_key_func, &i)) < 0)
        {
            return -1;
        }

        counters->counters[i].addr = aeron_counter_addr(manager, counters->counters[i].counter_id);
    }

    return 0;
}
