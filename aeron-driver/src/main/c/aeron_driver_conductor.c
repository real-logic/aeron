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

    conductor->context = context;
    return 0;
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
                    /* TODO: incr errors count */
                    return;
                }

                correlation_id = cmd->correlated.correlation_id;

                /* TODO: handle */
            }
            break;

        case AERON_COMMAND_CLIENT_KEEPALIVE:
            {
                aeron_correlated_command_t *cmd = (aeron_correlated_command_t *)message;

                /* TODO: handle */
            }
            break;

        default:
            aeron_distinct_error_log_record(
                &conductor->error_log,
                AERON_ERROR_CODE_UNKNOWN_COMMAND_TYPE_ID,
                "unknown command type id",
                "unknown command type id");
            /* TODO: incr errors count */
            break;
    }

    if (result < 0)
    {
        aeron_driver_conductor_on_error(conductor, AERON_ERROR_CODE_GENERIC_ERROR, "", length, correlation_id);
        aeron_distinct_error_log_record(&conductor->error_log, AERON_ERROR_CODE_GENERIC_ERROR, "", "");
        /* TODO: incr errors count */
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
