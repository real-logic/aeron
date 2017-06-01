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

#ifndef AERON_AERON_IPC_PUBLICATION_H
#define AERON_AERON_IPC_PUBLICATION_H

#include "aeron_driver_common.h"
#include "util/aeron_bitutil.h"
#include "aeron_driver_context.h"
#include "util/aeron_fileutil.h"

typedef struct aeron_ipc_publication_stct
{
    struct conductor_fields_stct
    {
        aeron_driver_managed_resource_t managed_resource;
        aeron_subscribeable_t subscribeable;
    }
    conductor_fields;

    /* uint8_t conductor_fields_pad[(2 * AERON_CACHE_LINE_LENGTH) - sizeof(struct conductor_fields_stct)]; */

    aeron_mapped_raw_log_t mapped_raw_log;
    aeron_position_t pub_lmt_position;
    aeron_logbuffer_metadata_t *log_meta_data;

    char *log_file_name;
    int32_t session_id;
    int32_t stream_id;
    int32_t initial_term_id;
    size_t log_file_name_length;
    size_t position_bits_to_shift;

}
aeron_ipc_publication_t;

int aeron_ipc_publication_create(
    aeron_ipc_publication_t **publication,
    aeron_driver_context_t *context,
    int32_t session_id,
    int32_t stream_id,
    int64_t registration_id,
    aeron_position_t *pub_lmt_position,
    int32_t initial_term_id,
    size_t term_buffer_length,
    size_t mtu_length);

void aeron_ipc_publication_close(aeron_ipc_publication_t *publication);

inline int64_t aeron_ipc_publication_producer_position(aeron_ipc_publication_t *publication)
{
    int64_t raw_tail;

    AERON_LOGBUFFER_RAWTAIL_VOLATILE(raw_tail, publication->log_meta_data);

    return aeron_logbuffer_compute_position(
        aeron_logbuffer_term_id(raw_tail),
        aeron_logbuffer_term_offset(raw_tail, (int32_t)publication->mapped_raw_log.term_length),
        publication->position_bits_to_shift,
        publication->initial_term_id);
}

inline int64_t aeron_ipc_publication_joining_position(aeron_ipc_publication_t *publication)
{
    return aeron_ipc_publication_producer_position(publication);
}

#endif //AERON_AERON_IPC_PUBLICATION_H
