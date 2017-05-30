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
#include <errno.h>
#include "aeron_ipc_publication.h"
#include "util/aeron_fileutil.h"
#include "aeron_alloc.h"

int aeron_ipc_publication_create(
    aeron_ipc_publication_t **publication,
    aeron_driver_context_t *context,
    int32_t session_id,
    int32_t stream_id,
    int64_t registration_id,
    int32_t pub_lmt_counter_id,
    size_t term_buffer_length)
{
    char path[AERON_MAX_PATH];
    int path_length =
        aeron_ipc_publication_location(path, sizeof(path), context->aeron_dir, session_id, stream_id, registration_id);
    aeron_ipc_publication_t *_pub = NULL;
    const uint64_t usable_fs_space = aeron_usable_fs_space(context->aeron_dir);
    const uint64_t log_length = AERON_LOGBUFFER_COMPUTE_LOG_LENGTH(term_buffer_length);

    *publication = NULL;

    if (usable_fs_space < log_length)
    {
        errno = ENOSPC;
        return -1;
    }

    if (aeron_alloc((void **)_pub, sizeof(aeron_ipc_publication_t)) < 0)
    {
        return -1;
    }

    _pub->log_file_name = NULL;
    if (aeron_alloc((void **)(_pub->log_file_name), (size_t)path_length) < 0)
    {
        aeron_free(_pub);
        return -1;
    }

    if (aeron_map_raw_log(&_pub->mapped_raw_log, path, context->term_buffer_sparse_file, term_buffer_length) < 0)
    {
        aeron_free(_pub->log_file_name);
        aeron_free(_pub);
        return -1;
    }

    strncpy(_pub->log_file_name, path, path_length);
    _pub->conductor_fields.subscribeable.array = NULL;
    _pub->conductor_fields.subscribeable.length = 0;
    _pub->conductor_fields.subscribeable.capacity = 0;
    _pub->conductor_fields.managed_resource.registration_id = registration_id;
    _pub->conductor_fields.managed_resource.refcnt = 0;
    _pub->session_id = session_id;
    _pub->stream_id = stream_id;
    _pub->pub_lmt_counter_id = pub_lmt_counter_id;

    /* TODO: set other values */

    *publication = _pub;
    return 0;
}

void aeron_ipc_publication_close(aeron_ipc_publication_t *publication)
{
    if (NULL != publication)
    {
        aeron_map_raw_log_close(&publication->mapped_raw_log);
        aeron_free(publication->log_file_name);
    }

    aeron_free(publication);
}
