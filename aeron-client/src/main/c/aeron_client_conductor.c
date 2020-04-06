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
#include <string.h>

#include "aeron_client_conductor.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "aeron_context.h"
#include "aeron_client.h"
#include "concurrent/aeron_mpsc_concurrent_array_queue.h"

int aeron_client_conductor_init(aeron_client_conductor_t *conductor, aeron_context_t *context)
{
    if (aeron_mpsc_concurrent_array_queue_init(&conductor->command_queue, AERON_CLIENT_COMMAND_QUEUE_CAPACITY) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "aeron_client_conductor_init - command_queue: %s", strerror(errcode));
        return -1;
    }

    conductor->invoker_mode = context->use_conductor_agent_invoker;

    return 0;
}

void aeron_client_conductor_on_command(void *clientd, volatile void *item)
{
    aeron_client_command_base_t *cmd = (aeron_client_command_base_t *)item;

    cmd->func(clientd, cmd);
}

int aeron_client_conductor_do_work(aeron_client_conductor_t *conductor)
{
    int work_count = 0;

    work_count += (int)aeron_mpsc_concurrent_array_queue_drain(
        &conductor->command_queue, aeron_client_conductor_on_command, conductor, 10);

    return work_count;
}

void aeron_client_conductor_on_close(aeron_client_conductor_t *conductor)
{
    aeron_mpsc_concurrent_array_queue_close(&conductor->command_queue);
}

void aeron_client_conductor_on_cmd_add_publication(void *clientd, void *item)
{
//    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *)clientd;
//    aeron_async_add_publication_t *cmd = (aeron_async_add_publication_t *)item;

    // TODO: send ADD_PUBLCIATION command to driver
    // TODO: save cmd in registering_resources
}

int aeron_client_conductor_command_offer(aeron_mpsc_concurrent_array_queue_t *command_queue, void *cmd)
{
    int fail_count = 0;

    while (aeron_mpsc_concurrent_array_queue_offer(command_queue, cmd) != AERON_OFFER_SUCCESS)
    {
        if (++fail_count > AERON_CLIENT_COMMAND_QUEUE_FAIL_THRESHOLD)
        {
            return -1;
        }

        sched_yield();
    }

    return 0;
}

int aeron_client_conductor_async_add_publication(
    aeron_async_add_publication_t **async, aeron_client_conductor_t *conductor, const char *uri, int32_t stream_id)
{
    aeron_async_add_publication_t *cmd = NULL;
    char *uri_copy = NULL;
    size_t uri_length = strlen(uri);

    *async = NULL;

    if (aeron_alloc((void **)&cmd, sizeof(aeron_async_add_publication_t)) < 0 ||
        aeron_alloc((void **)&uri_copy, uri_length + 1) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "aeron_async_add_publication (%d): %s", errcode, strerror(errcode));
        return -1;
    }

    memcpy(uri_copy, uri, uri_length);
    uri_copy[uri_length] = '\0';

    cmd->command_base.func = aeron_client_conductor_on_cmd_add_publication;
    cmd->command_base.item = NULL;
    cmd->resource.publication = NULL;
    cmd->error_message = NULL;
    cmd->uri = uri_copy;
    cmd->filename = NULL;
    cmd->stream_id = stream_id;
    cmd->session_id = -1;
    cmd->registration_id = -1;
    cmd->registration_status = AERON_CLIENT_AWAITING_MEDIA_DRIVER;
    cmd->type = AERON_CLIENT_TYPE_PUBLICATION;

    if (conductor->invoker_mode)
    {
        *async = cmd;
        aeron_client_conductor_on_cmd_add_publication(conductor, cmd);
    }
    else
    {
        if (aeron_client_conductor_command_offer(&conductor->command_queue, cmd) < 0)
        {
            aeron_free(cmd->uri);
            aeron_free(cmd);
            return -1;
        }

        *async = cmd;
    }

    return 0;
}
