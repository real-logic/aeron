/*
 * Copyright 2014-2024 Real Logic Limited.
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

#include "aeron_archive.h"
#include "aeron_archive_client.h"

#include "aeronc.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

struct aeron_archive_stct
{
    aeron_archive_context_t *ctx;
    aeron_archive_proxy_t *archive_proxy;
    aeron_archive_control_response_poller_t *control_response_poller;
    aeron_t *aeron;
    int64_t control_session_id;
    int64_t archive_id;
};

int aeron_archive_create(
    aeron_archive_t **client,
    aeron_archive_context_t *ctx,
    aeron_archive_proxy_t *archive_proxy,
    aeron_archive_control_response_poller_t *control_response_poller,
    void *recording_descriptor_poller,
    void *recording_subscription_descriptor_poller,
    aeron_t *aeron,
    int64_t control_session_id,
    int64_t archive_id)
{
    aeron_archive_t *_client = NULL;

    if (aeron_alloc((void **)&_client, sizeof(aeron_archive_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_t");
        return -1;
    }

    _client->ctx = ctx;
    _client->archive_proxy = archive_proxy;
    _client->control_response_poller = control_response_poller;
    _client->aeron = aeron;
    _client->control_session_id = control_session_id;
    _client->archive_id = archive_id;

    *client = _client;
    return 0;
}

int64_t aeron_archive_get_archive_id(aeron_archive_t *client)
{
    return client->archive_id;
}

aeron_archive_control_response_poller_t *aeron_archive_get_control_response_poller(aeron_archive_t *client)
{
    return client->control_response_poller;
}
