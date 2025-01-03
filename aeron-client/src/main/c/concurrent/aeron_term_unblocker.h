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

#ifndef AERON_TERM_UNBLOCKER_H
#define AERON_TERM_UNBLOCKER_H

#include "aeron_logbuffer_descriptor.h"

typedef enum aeron_term_unblocker_status_enum
{
    AERON_TERM_UNBLOCKER_STATUS_NO_ACTION,
    AERON_TERM_UNBLOCKER_STATUS_UNBLOCKED,
    AERON_TERM_UNBLOCKER_STATUS_UNBLOCKED_TO_END
}
aeron_term_unblocker_status_t;

aeron_term_unblocker_status_t aeron_term_unblocker_unblock(
    aeron_logbuffer_metadata_t *log_meta_data,
    uint8_t *buffer,
    size_t term_length,
    int32_t blocked_offset,
    int32_t tail_offset,
    int32_t term_id);

#endif //AERON_TERM_UNBLOCKER_H
