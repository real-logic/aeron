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
#include <stdio.h>

#include "aeron_client_conductor.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "aeron_context.h"
#include "aeron_client.h"
#include "command/aeron_control_protocol.h"
#include "util/aeron_arrayutil.h"

int aeron_client_conductor_init(aeron_client_conductor_t *conductor, aeron_context_t *context)
{
    if (aeron_mpsc_concurrent_array_queue_init(&conductor->command_queue, AERON_CLIENT_COMMAND_QUEUE_CAPACITY) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "aeron_client_conductor_init - command_queue: %s", strerror(errcode));
        return -1;
    }

    conductor->error_handler = context->error_handler;
    conductor->error_handler_clientd = context->error_handler_clientd;

    conductor->invoker_mode = context->use_conductor_agent_invoker;

    return 0;
}

void aeron_client_conductor_on_command(void *clientd, volatile void *item)
{
    aeron_client_command_base_t *cmd = (aeron_client_command_base_t *)item;

    cmd->func(clientd, cmd);
}

void aeron_client_conductor_on_driver_response(int32_t type_id, uint8_t *buffer, size_t length, void *clientd)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *)clientd;
    int result = 0;

    char error_message[AERON_MAX_PATH] = "\0";

    switch (type_id)
    {
        case AERON_RESPONSE_ON_ERROR:
        {
            aeron_error_response_t *response = (aeron_error_response_t *)buffer;

            if (length < sizeof(aeron_error_response_t) ||
                length < (sizeof(aeron_error_response_t) + response->error_message_length))
            {
                goto malformed_command;
            }

            result = aeron_client_conductor_on_error(conductor, response);
            break;
        }

        case AERON_RESPONSE_ON_AVAILABLE_IMAGE:
        {
            break;
        }

        case AERON_RESPONSE_ON_PUBLICATION_READY:
        {
            aeron_publication_buffers_ready_t *response = (aeron_publication_buffers_ready_t *)buffer;

            if (length < sizeof(aeron_publication_buffers_ready_t) ||
                length < (sizeof(aeron_publication_buffers_ready_t) + response->log_file_length))
            {
                goto malformed_command;
            }

            result = aeron_client_conductor_on_publication_ready(conductor, response);
            break;
        }

        default:
            AERON_CLIENT_FORMAT_BUFFER(error_message, "response=%d unknown", type_id);
            conductor->error_handler(
                conductor->error_handler_clientd, AERON_ERROR_CODE_UNKNOWN_COMMAND_TYPE_ID, error_message);
            break;
    }

    if (result < 0)
    {
        int os_errno = aeron_errcode();
        int code = os_errno < 0 ? -os_errno : AERON_ERROR_CODE_GENERIC_ERROR;
        const char *error_description = os_errno > 0 ? strerror(os_errno) : aeron_error_code_str(code);

        AERON_CLIENT_FORMAT_BUFFER(error_message, "(%d) %s: %s", os_errno, error_description, aeron_errmsg());
        conductor->error_handler(conductor->error_handler_clientd, code, error_message);
    }

    return;

    malformed_command:
    AERON_CLIENT_FORMAT_BUFFER(error_message, "command=%d too short: length=%zu", type_id, (size_t)length);
    conductor->error_handler(conductor->error_handler_clientd, AERON_ERROR_CODE_MALFORMED_COMMAND, error_message);
}

int aeron_client_conductor_do_work(aeron_client_conductor_t *conductor)
{
    int work_count = 0;

    work_count += (int)aeron_mpsc_concurrent_array_queue_drain(
        &conductor->command_queue, aeron_client_conductor_on_command, conductor, 10);

    work_count += aeron_broadcast_receiver_receive(
        &conductor->broadcast_receiver, aeron_client_conductor_on_driver_response, conductor);

    return work_count;
}

void aeron_client_conductor_on_close(aeron_client_conductor_t *conductor)
{
    aeron_mpsc_concurrent_array_queue_close(&conductor->command_queue);
}

void aeron_client_conductor_on_cmd_add_publication(void *clientd, void *item)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *)clientd;
    aeron_async_add_publication_t *async = (aeron_async_add_publication_t *)item;

    char buffer[sizeof(aeron_publication_command_t) + AERON_MAX_PATH];
    aeron_publication_command_t *command = (aeron_publication_command_t *)buffer;
    int ensure_capacity_result = 0, rb_offer_fail_count = 0;

    command->correlated.correlation_id = async->registration_id;
    command->correlated.client_id = conductor->client_id;
    command->stream_id = async->stream_id;
    command->channel_length = async->uri_length;
    memcpy(buffer + sizeof(aeron_publication_command_t), async->uri, async->uri_length);

    while (AERON_RB_SUCCESS != aeron_mpsc_rb_write(
        &conductor->to_driver_rb,
        AERON_COMMAND_ADD_PUBLICATION,
        buffer,
        sizeof(aeron_publication_command_t) + async->uri_length))
    {
        if (++rb_offer_fail_count > AERON_CLIENT_COMMAND_RB_FAIL_THRESHOLD)
        {
            // TODO: error
            return;
        }

        sched_yield();
    }

    AERON_ARRAY_ENSURE_CAPACITY(
        ensure_capacity_result, conductor->registering_resources, aeron_client_registering_resource_entry_t);
    if (ensure_capacity_result < 0)
    {
        // TODO: error
        return;
    }

    conductor->registering_resources.array[conductor->registering_resources.length++].resource = async;
    async->registration_deadline_ms = conductor->epoch_clock();
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
    cmd->epoch_clock = conductor->epoch_clock;
    cmd->registration_deadline_ms = conductor->epoch_clock() + conductor->registration_timeout_ms;
    cmd->error_message = NULL;
    cmd->uri = uri_copy;
    cmd->log_file = NULL;
    cmd->uri_length = uri_length;
    cmd->stream_id = stream_id;
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

int aeron_client_conductor_on_error(aeron_client_conductor_t *conductor, aeron_error_response_t *response)
{
    for (size_t i = 0, size = conductor->registering_resources.length, last_index = size - 1; i < size; i++)
    {
        aeron_client_registering_resource_t *resource = conductor->registering_resources.array[i].resource;

        if (response->offending_command_correlation_id == resource->registration_id)
        {
            resource->error_message_length = response->error_message_length;
            resource->error_code = response->error_code;

            if (aeron_alloc((void **)&resource->error_message, response->error_message_length + 1) < 0)
            {
                int errcode = errno;

                aeron_set_err(errcode, "aeron_client_conductor_on_error (%d): %s", errcode, strerror(errcode));
                return -1;
            }

            memcpy(
                resource->error_message,
                (const char *)response + sizeof(aeron_error_response_t),
                resource->error_message_length);
            resource->error_message[resource->error_message_length] = '\0';

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->registering_resources.array,
                sizeof(aeron_client_registering_resource_entry_t),
                i,
                last_index);
            conductor->registering_resources.length--;

            AERON_PUT_ORDERED(resource->registration_status, AERON_CLIENT_ERRORED_MEDIA_DRIVER);
            break;
        }
    }

    return 0;
}

int aeron_client_conductor_on_publication_ready(
    aeron_client_conductor_t *conductor, aeron_publication_buffers_ready_t *response)
{
    for (size_t i = 0, size = conductor->registering_resources.length, last_index = size - 1; i < size; i++)
    {
        aeron_client_registering_resource_t *resource = conductor->registering_resources.array[i].resource;

        if (response->correlation_id == resource->registration_id)
        {
            resource->log_file_length = response->log_file_length;
            resource->registration_id = response->correlation_id;
            // TODO: = response->registration_id;
            // TODO: = response->session_id;
            // TODO: = response->channel_status_indicator_id;
            // TODO: = response->position_limit_counter_id;

            if (aeron_alloc((void **)&resource->log_file, response->log_file_length + 1) < 0)
            {
                int errcode = errno;

                aeron_set_err(errcode, "aeron_client_conductor_on_publication_ready (%d): %s", errcode, strerror(errcode));
                return -1;
            }

            memcpy(
                resource->log_file,
                (const char *)response + sizeof(aeron_publication_buffers_ready_t),
                resource->log_file_length);
            resource->log_file[resource->log_file_length] = '\0';

            // TODO: create the aeron_publication_t at this point.

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->registering_resources.array,
                sizeof(aeron_client_registering_resource_entry_t),
                i,
                last_index);
            conductor->registering_resources.length--;

            AERON_PUT_ORDERED(resource->registration_status, AERON_CLIENT_REGISTERED_MEDIA_DRIVER);
            break;
        }
    }

    return 0;
}
