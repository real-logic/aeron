/*
 * Copyright 2014-2020 Real Logic Limited.
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

#ifndef AERON_COMMON_H
#define AERON_COMMON_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#define AERON_MAX_PATH (384)

typedef void (*aeron_idle_strategy_func_t)(void *state, int work_count);
typedef int (*aeron_idle_strategy_init_func_t)(void **state, const char *env_var, const char *init_args);

#endif //AERON_COMMON_H
