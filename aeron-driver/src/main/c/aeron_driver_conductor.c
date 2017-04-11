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

#include "util/aeron_arrayutil.h"
#include "command/aeron_control_protocol.h"
#include "aeron_driver_conductor.h"

static void aeron_error_log_resource_linger(uint8_t *resource)
{
    /* TODO: must be MPSC queue to delete after linger */
}

int aeron_driver_conductor_init(aeron_driver_conductor_t *conductor, aeron_driver_context_t *context)
{
    if (aeron_mpsc_rb_init(
        &conductor->to_driver_commands, context->to_driver_buffer, context->to_driver_buffer_length) < 0)
    {
        return -1;
    }

    if (aeron_broadcast_transmitter_init(
        &conductor->to_clients, context->to_clients_buffer, context->to_clients_buffer_length) < 0)
    {
        return -1;
    }

    if (aeron_counters_manager_init(
        &conductor->counters_manager,
        context->counters_metadata_buffer,
        context->counters_metadata_buffer_length,
        context->counters_values_buffer,
        context->counters_values_buffer_length) < 0)
    {
        return -1;
    }

    if (aeron_system_counters_init(&conductor->system_counters, &conductor->counters_manager) < 0)
    {
        return -1;
    }

    if (aeron_distinct_error_log_init(
        &conductor->error_log,
        context->error_buffer,
        context->error_buffer_length,
        context->epoch_clock,
        aeron_error_log_resource_linger) < 0)
    {
        return -1;
    }

    /* TODO: create and init all command queues */

    if (aeron_array_ensure_capacity((uint8_t **)&conductor->clients.array, sizeof(aeron_client_t), 0, 2) < 0)
    {
        return -1;
    }
    conductor->clients.capacity = 2;
    conductor->clients.length = 0;
    /* TODO: set func callbacks for managed resources */

    conductor->errors_counter = aeron_counter_addr(&conductor->counters_manager, AERON_SYSTEM_COUNTER_ERRORS);

    conductor->context = context;
    return 0;
}

int aeron_driver_conductor_find_client(aeron_driver_conductor_t *conductor, int64_t client_id)
{
    int index = -1;

    for (int i = (int)conductor->clients.length - 1; i >= 0; i--)
    {
        if (client_id == conductor->clients.array[i].client_id)
        {
            index = i;
            break;
        }
    }

    return index;
}

aeron_client_t *aeron_driver_conductor_client_for_publication(
    aeron_driver_conductor_t *conductor, aeron_publication_link_t *publication_link)
{
    aeron_client_t *client = NULL;

    if (publication_link->cached_client_index < (int)conductor->clients.length)
    {
        client = &conductor->clients.array[publication_link->cached_client_index];

        if (client->client_id != publication_link->client_id)
        {
            client = NULL;
        }
    }

    if (NULL == client)
    {
        int index;

        if ((index = aeron_driver_conductor_find_client(conductor, publication_link->client_id)) >= 0)
        {
            client = &conductor->clients.array[index];
        }

        publication_link->cached_client_index = index;
    }

    return client;
}

aeron_client_t *aeron_driver_conductor_get_or_add_client(aeron_driver_conductor_t *conductor, int64_t client_id)
{
    aeron_client_t *client = NULL;
    int index;

    if ((index = aeron_driver_conductor_find_client(conductor, client_id)) == -1)
    {
        if (conductor->clients.length >= conductor->clients.capacity)
        {
            size_t new_capacity = conductor->clients.capacity + (conductor->clients.capacity >> 1);

            if (aeron_array_ensure_capacity(
                (uint8_t **)&conductor->clients.array, sizeof(aeron_client_t), conductor->clients.capacity, new_capacity) < 0)
            {
                return NULL;
            }
            conductor->clients.capacity = new_capacity;
        }

        index = (int)conductor->clients.length;
        client = &conductor->clients.array[index];

        client->client_id = client_id;
        client->reached_end_of_life = false;
        client->time_of_last_keepalive = conductor->context->nano_clock();
        client->client_liveness_timeout_ns = conductor->context->client_liveness_timeout_ns;
        conductor->clients.length++;
    }
    else
    {
        client = &conductor->clients.array[index];
    }

    return client;
}

#define AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(l,t,now_ns,now_ms) \
for (int last_index = (int)l.length - 1, i = last_index; i >= 0; i--) \
{ \
    t *elem = &l.array[i]; \
    l.on_time_event(elem, now_ns, now_ms); \
    if (l.has_reached_end_of_life(elem)) \
    { \
        l.delete(elem); \
        aeron_array_fast_unordered_remove((uint8_t *)l.array, sizeof(t), i, last_index); \
        last_index--; \
    } \
}

void aeron_driver_conductor_on_check_managed_resources(
    aeron_driver_conductor_t *conductor, int64_t now_ns, int64_t now_ms)
{
    AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(conductor->clients, aeron_client_t, now_ns, now_ms);
    AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(conductor->publication_links, aeron_publication_link_t, now_ns, now_ms);
}

void aeron_driver_conductor_client_transmit(
    aeron_driver_conductor_t *conductor,
    int32_t msg_type_id,
    const void *msg,
    size_t length)
{
    aeron_broadcast_transmitter_transmit(&conductor->to_clients, AERON_RESPONSE_ON_ERROR, msg, length);
}

void aeron_driver_conductor_on_error(
    aeron_driver_conductor_t *conductor,
    int32_t error_code,
    const char *message,
    size_t length,
    int64_t correlation_id)
{
    char response_buffer[sizeof(aeron_error_response_t) + AERON_MAX_PATH];
    aeron_error_response_t *response = (aeron_error_response_t *)response_buffer;

    response->offending_command_correlation_id = correlation_id;
    response->error_code = error_code;
    response->error_message_length = (int32_t)length;
    memcpy(response->error_message_data, message, length);

    aeron_driver_conductor_client_transmit(
        conductor, AERON_RESPONSE_ON_ERROR, response, sizeof(aeron_error_response_t) + length);
}

void aeron_driver_conductor_on_command(int32_t msg_type_id, const void *message, size_t length, void *clientd)
{
    aeron_driver_conductor_t *conductor = (aeron_driver_conductor_t *)clientd;
    int64_t correlation_id = 0;
    int result = 0;

    switch (msg_type_id)
    {
        case AERON_COMMAND_ADD_PUBLICATION:
        {
            aeron_publication_command_t *cmd = (aeron_publication_command_t *)message;

            if (length < sizeof(aeron_publication_command_t) ||
                length < (sizeof(aeron_publication_command_t) + cmd->channel_length))
            {
                aeron_distinct_error_log_record(
                    &conductor->error_log,
                    AERON_ERROR_CODE_MALFORMED_COMMAND,
                    "command too short",
                    "command too short");
                aeron_counter_increment(conductor->errors_counter, 1);
                return;
            }

            correlation_id = cmd->correlated.correlation_id;

            /* TODO: handle */
            break;
        }

        case AERON_COMMAND_REMOVE_PUBLICATION:
        {
            aeron_remove_command_t *cmd = (aeron_remove_command_t *)message;

            correlation_id = cmd->correlated.correlation_id;

            /* TODO: handle */
            break;
        }

        case AERON_COMMAND_CLIENT_KEEPALIVE:
        {
            aeron_correlated_command_t *cmd = (aeron_correlated_command_t *)message;

            /* TODO: handle */
            break;
        }

        default:
            aeron_distinct_error_log_record(
                &conductor->error_log,
                AERON_ERROR_CODE_UNKNOWN_COMMAND_TYPE_ID,
                "unknown command type id",
                "unknown command type id");
            aeron_counter_increment(conductor->errors_counter, 1);
            break;
    }

    if (result < 0)
    {
        aeron_driver_conductor_on_error(conductor, AERON_ERROR_CODE_GENERIC_ERROR, "", length, correlation_id);
        aeron_distinct_error_log_record(&conductor->error_log, AERON_ERROR_CODE_GENERIC_ERROR, "", "");
        aeron_counter_increment(conductor->errors_counter, 1);
    }
}

int aeron_driver_conductor_do_work(void *clientd)
{
    aeron_driver_conductor_t *conductor = (aeron_driver_conductor_t *)clientd;
    int work_count = 0;

    work_count +=
        (int)aeron_mpsc_rb_read(&conductor->to_driver_commands, aeron_driver_conductor_on_command, conductor, 10);

    return work_count;
}

void aeron_driver_conductor_on_close(void *clientd)
{


}
