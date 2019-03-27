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

#ifndef AERON_DRIVER_COMMON_H
#define AERON_DRIVER_COMMON_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#define AERON_MAX_PATH (384)
#define AERON_CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED (-1)

typedef void (*aeron_idle_strategy_func_t)(void *, int);

typedef int (*aeron_idle_strategy_init_func_t)(void **, const char *, const char *);

typedef int64_t (*aeron_feedback_delay_generator_func_t)();

typedef bool (*aeron_driver_termination_validator_func_t)(void *, uint8_t *, int32_t);

typedef struct aeron_driver_managed_resource_stct
{
    int64_t registration_id;
    int64_t time_of_last_status_change;
    void *clientd;
    void (*decref)(void *);
    void (*incref)(void *);
}
aeron_driver_managed_resource_t;

typedef struct aeron_position_stct
{
    int64_t *value_addr;
    int64_t counter_id;
}
aeron_position_t;

typedef struct aeron_position_stct aeron_counter_t;

typedef struct aeron_subscribable_stct
{
    aeron_position_t *array;
    size_t length;
    size_t capacity;
    void (*add_position_hook_func)(void *clientd, int64_t *value_addr);
    void (*remove_position_hook_func)(void *clientd, int64_t *value_addr);
    void *clientd;
}
aeron_subscribable_t;

typedef struct aeron_command_base_stct
{
    void (*func)(void *clientd, void *command);
    void *item;
}
aeron_command_base_t;

int aeron_driver_subscribable_add_position(
    aeron_subscribable_t *subscribable, int64_t counter_id, int64_t *value_addr);

void aeron_driver_subscribable_remove_position(aeron_subscribable_t *subscribable, int64_t counter_id);

inline void aeron_driver_subscribable_null_hook(void *clientd, int64_t *value_addr)
{
}

void aeron_command_on_delete_cmd(void *clientd, void *cmd);

#endif //AERON_DRIVER_COMMON_H
