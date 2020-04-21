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

#include <errno.h>
#include <inttypes.h>

#include "aeronc.h"
#include "aeron_common.h"
#include "aeron_exclusive_publication.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_fileutil.h"
#include "concurrent/aeron_counters_manager.h"
#include "concurrent/aeron_term_appender.h"
#include "aeron_log_buffer.h"

int aeron_exclusive_publication_create(
    aeron_exclusive_publication_t **publication,
    aeron_client_conductor_t *conductor,
    const char *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t *position_limit_addr,
    int64_t *channel_status_addr,
    aeron_log_buffer_t *log_buffer,
    int64_t original_registration_id,
    int64_t registration_id)
{
    aeron_exclusive_publication_t *_publication;

    *publication = NULL;
    if (aeron_alloc((void **)&_publication, sizeof(aeron_exclusive_publication_t)) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "aeron_exclusive_publication_create (%d): %s", errcode, strerror(errcode));
        return -1;
    }

    _publication->command_base.type = AERON_CLIENT_TYPE_EXCLUSIVE_PUBLICATION;

    _publication->log_buffer = log_buffer;
    _publication->log_meta_data = (aeron_logbuffer_metadata_t *)log_buffer->mapped_raw_log.log_meta_data.addr;

    _publication->position_limit = position_limit_addr;
    _publication->channel_status_indicator = channel_status_addr;

    _publication->conductor = conductor;
    _publication->channel = channel;
    _publication->registration_id = registration_id;
    _publication->original_registration_id = original_registration_id;
    _publication->stream_id = stream_id;
    _publication->session_id = session_id;
    _publication->is_closed = false;

    size_t term_length = (size_t)_publication->log_meta_data->term_length;

    _publication->max_possible_position = ((int64_t)term_length << 31L);
    _publication->max_payload_length = (size_t)(_publication->log_meta_data->mtu_length - AERON_DATA_HEADER_LENGTH);
    _publication->max_message_length = aeron_frame_compute_max_message_length(term_length);
    _publication->position_bits_to_shift = (size_t)aeron_number_of_trailing_zeroes((int32_t)term_length);
    _publication->initial_term_id = _publication->log_meta_data->initial_term_id;

    *publication = _publication;
    return -1;
}

int aeron_exclusive_publication_delete(aeron_exclusive_publication_t *publication)
{
    aeron_free((void *)publication->channel);
    aeron_free(publication);

    return 0;
}

int aeron_exclusive_publication_close(aeron_exclusive_publication_t *publication)
{
    return NULL != publication ?
        aeron_client_conductor_async_close_exclusive_publication(publication->conductor, publication) : 0;
}
