/*
 * Copyright 2014-2019 Real Logic Ltd.
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

#ifndef AERON_CLOCK_H
#define AERON_CLOCK_H

#include <stdint.h>
#include "util/aeron_bitutil.h"

typedef struct aeron_clock_cache_stct
{
    uint8_t pre_pad[(2 * AERON_CACHE_LINE_LENGTH)];
    int64_t cached_epoch_time;
    int64_t cached_nano_time;
    uint8_t post_pad[(2 * AERON_CACHE_LINE_LENGTH) - (2 * sizeof(int64_t))];
}
aeron_clock_cache_t;

int64_t aeron_clock_epoch_time();
int64_t aeron_clock_nano_time();
int64_t aeron_clock_cached_epoch_time(aeron_clock_cache_t *cache);
int64_t aeron_clock_cached_nano_time(aeron_clock_cache_t *cache);

#endif