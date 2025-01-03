/*
 * Copyright 2014-2025 Real Logic Limited.
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

#ifndef AERON_C_LOG_BUFFER_H
#define AERON_C_LOG_BUFFER_H

#include "aeronc.h"
#include "aeron_agent.h"
#include "aeron_context.h"

typedef struct aeron_log_buffer_stct
{
    aeron_mapped_raw_log_t mapped_raw_log;

    int64_t correlation_id;
    int64_t refcnt;
}
aeron_log_buffer_t;

int aeron_log_buffer_create(
    aeron_log_buffer_t **log_buffer, const char *log_file, int64_t correlation_id, bool pre_touch);
int aeron_log_buffer_delete(aeron_log_buffer_t *log_buffer);

#endif //AERON_C_LOG_BUFFER_H
