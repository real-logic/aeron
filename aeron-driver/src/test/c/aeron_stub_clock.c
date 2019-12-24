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

#include <stdio.h>
#include "aeron_stub_clock.h"

static int64_t ns_timestamp = 0;

int64_t aeron_clock_epoch_time()
{
    return ns_timestamp / 1000000;
}

int64_t aeron_clock_nano_time()
{
    return ns_timestamp;
}

void aeron_stub_clock_set_time_ms(int64_t time_ms)
{
    ns_timestamp = time_ms * 1000000;
}

void aeron_stub_clock_set_time_ns(int64_t time_ns)
{
    ns_timestamp = time_ns;
}

void aeron_stub_clock_increment_ms(int64_t offset_ms)
{
    aeron_stub_clock_set_time_ms(aeron_clock_epoch_time() + offset_ms);
}
