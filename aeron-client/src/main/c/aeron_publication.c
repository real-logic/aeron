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

#include "aeronc.h"
#include "aeron_common.h"
#include "aeron_client.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

int aeron_publication_init(
    aeron_publication_t **publication,
    aeron_client_conductor_t *conductor,
    const char *channel,
    int32_t stream_id,
    int32_t session_id,
    int32_t position_limit_id,
    int32_t channel_status_id,
    const char *log_file,
    int64_t original_registration_id,
    int64_t registration_id)
{
    aeron_publication_t *_publication;

    *publication = NULL;
    if (aeron_alloc((void **)&_publication, sizeof(aeron_publication_t)) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "aeron_publication_init (%d): %s", errcode, strerror(errcode));
        return -1;
    }

    _publication->conductor = conductor;
    _publication->channel = channel;

    *publication = _publication;
    return -1;
}

int aeron_publication_close(aeron_publication_t *publication)
{
    if (NULL != publication)
    {

    }

    return 0;
}
