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

#include <stdint.h>

typedef struct aeron_clock_cache_stct
{
    // TODO: pad
    int64_t cached_ms;
    int64_t cached_ns;
    // TODO: pad
}
aeron_clock_cache_t;

int64_t aeron_clock_epoch_time();
int64_t aeron_clock_nano_time();
int64_t aeron_clock_cached_epoch_time(aeron_clock_cache_t *cache);
int64_t aeron_clock_cached_nano_time(aeron_clock_cache_t *cache);