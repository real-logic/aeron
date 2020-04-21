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
#include <inttypes.h>

#include "aeron_client_conductor.h"
#include "aeron_publication.h"
#include "aeron_exclusive_publication.h"
#include "aeron_subscription.h"
#include "aeron_log_buffer.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "aeron_context.h"
#include "command/aeron_control_protocol.h"
#include "util/aeron_arrayutil.h"
#include "aeron_cnc_file_descriptor.h"
#include "aeron_image.h"

int aeron_client_conductor_init(aeron_client_conductor_t *conductor, aeron_context_t *context)
{
    aeron_cnc_metadata_t *metadata = (aeron_cnc_metadata_t *)context->cnc_map.addr;

    if (aeron_broadcast_receiver_init(
        &conductor->to_client_buffer, aeron_cnc_to_clients_buffer(metadata), metadata->to_clients_buffer_length) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "aeron_client_conductor_init - broadcast_receiver: %s", strerror(errcode));
        return -1;
    }

    if (aeron_mpsc_rb_init(
        &conductor->to_driver_buffer, aeron_cnc_to_driver_buffer(metadata), metadata->to_driver_buffer_length) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "aeron_client_conductor_init - to_driver_rb: %s", strerror(errcode));
        return -1;
    }

    if (aeron_counters_reader_init(
        &conductor->counters_reader,
        aeron_cnc_counters_metadata_buffer(metadata),
        metadata->counter_metadata_buffer_length,
        aeron_cnc_counters_values_buffer(metadata),
        metadata->counter_values_buffer_length) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "aeron_client_conductor_init - counters_reader: %s", strerror(errcode));
        return -1;
    }

    if (aeron_int64_to_ptr_hash_map_init(
        &conductor->log_buffer_by_id_map, 16, AERON_INT64_TO_PTR_HASH_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "aeron_client_conductor_init - log_buffer_by_id_map: %s", strerror(errcode));
        return -1;
    }

    if (aeron_int64_to_ptr_hash_map_init(
        &conductor->resource_by_id_map, 16, AERON_INT64_TO_PTR_HASH_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "aeron_client_conductor_init - resource_by_id_map: %s", strerror(errcode));
        return -1;
    }

    conductor->client_id = aeron_mpsc_rb_next_correlation_id(&conductor->to_driver_buffer);

    conductor->registering_resources.array = NULL;
    conductor->registering_resources.capacity = 0;
    conductor->registering_resources.length = 0;

    conductor->lingering_resources.array = NULL;
    conductor->lingering_resources.capacity = 0;
    conductor->lingering_resources.length = 0;

    conductor->heartbeat_timestamp.addr = NULL;
    conductor->heartbeat_timestamp.counter_id = AERON_NULL_COUNTER_ID;

    conductor->command_queue = &context->command_queue;

    conductor->error_handler = context->error_handler;
    conductor->error_handler_clientd = context->error_handler_clientd;

    conductor->driver_timeout_ms = context->driver_timeout_ms;
    conductor->driver_timeout_ns = context->driver_timeout_ms * 1000000;
    conductor->inter_service_timeout_ns = metadata->client_liveness_timeout;
    conductor->keepalive_interval_ns = context->keepalive_interval_ns;
    conductor->resource_linger_duration_ns = context->resource_linger_duration_ns;

    conductor->invoker_mode = context->use_conductor_agent_invoker;
    conductor->pre_touch = context->pre_touch_mapped_memory;
    conductor->is_terminating = false;

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

        case AERON_RESPONSE_ON_PUBLICATION_READY:
        case AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY:
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

        case AERON_RESPONSE_ON_SUBSCRIPTION_READY:
        {
            aeron_subscription_ready_t *response = (aeron_subscription_ready_t *)buffer;

            if (length < sizeof(aeron_subscription_ready_t))
            {
                goto malformed_command;
            }

            result = aeron_client_conductor_on_subscription_ready(conductor, response);
            break;
        }

        case AERON_RESPONSE_ON_AVAILABLE_IMAGE:
        {
            aeron_image_buffers_ready_t *response = (aeron_image_buffers_ready_t *)buffer;
            uint8_t *ptr = buffer + sizeof(aeron_image_buffers_ready_t);
            int32_t log_file_length, source_identity_length;
            const char *log_file = (const char *)(ptr + sizeof(int32_t));
            const char *source_identity;

            memcpy(&log_file_length, ptr, sizeof(int32_t));

            if (length < sizeof(aeron_image_buffers_ready_t) + 2 * sizeof(int32_t) ||
                length < sizeof(aeron_image_buffers_ready_t) + 2 * sizeof(int32_t) + log_file_length)
            {
                goto malformed_command;
            }

            ptr += sizeof(int32_t) + log_file_length;
            memcpy(&source_identity_length, ptr, sizeof(int32_t));

            if (length <
                sizeof(aeron_image_buffers_ready_t) + 2 * sizeof(int32_t) + log_file_length + source_identity_length)
            {
                goto malformed_command;
            }

            source_identity = (const char *)(ptr + sizeof(int32_t));

            result = aeron_client_conductor_on_available_image(
                conductor, response, log_file_length, log_file, source_identity_length, source_identity);
            break;
        }

        case AERON_RESPONSE_ON_UNAVAILABLE_IMAGE:
        {
            aeron_image_message_t *response = (aeron_image_message_t *)buffer;

            if (length < sizeof(aeron_image_message_t) ||
                length < (sizeof(aeron_image_message_t) + response->channel_length))
            {
                goto malformed_command;
            }

            result = aeron_client_conductor_on_unavailable_image(conductor, response);
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

int aeron_client_conductor_check_liveness(aeron_client_conductor_t *conductor, long long now_ns)
{
    if (now_ns > (conductor->time_of_last_keepalive_ns + (long long)conductor->keepalive_interval_ns))
    {
        const long long last_keepalive_ms = aeron_mpsc_rb_consumer_heartbeat_time_value(&conductor->to_driver_buffer);
        const long long now_ms = conductor->epoch_clock();

        if (now_ms > (last_keepalive_ms + (long long)conductor->driver_timeout_ms))
        {
            char buffer[AERON_MAX_PATH];

            conductor->is_terminating = true;
            // TODO: forceCloseResources
            snprintf(buffer, sizeof(buffer) - 1,
                "MediaDriver keepalive age exceeded (ms): timeout=%" PRId64 ", age=%" PRId64,
                (int64_t)conductor->driver_timeout_ms,
                (int64_t)(now_ms - last_keepalive_ms));
            conductor->error_handler(conductor->error_handler_clientd, ETIMEDOUT, buffer);
            return -1;
        }

        if (AERON_NULL_COUNTER_ID == conductor->heartbeat_timestamp.counter_id)
        {
            const int32_t id = aeron_counter_heartbeat_timestamp_find_counter_id_by_registration_id(
                &conductor->counters_reader, AERON_COUNTER_CLIENT_HEARTBEAT_TIMESTAMP_TYPE_ID, conductor->client_id);

            if (AERON_NULL_COUNTER_ID != id)
            {
                conductor->heartbeat_timestamp.counter_id = id;
                conductor->heartbeat_timestamp.addr = aeron_counters_reader_addr(
                    &conductor->counters_reader, conductor->heartbeat_timestamp.counter_id);

                aeron_counter_set_ordered(conductor->heartbeat_timestamp.addr, now_ms);
                conductor->time_of_last_keepalive_ns = now_ns;
            }
        }
        else
        {
            const int32_t id = conductor->heartbeat_timestamp.counter_id;
            if (!aeron_counter_heartbeat_timestamp_is_active(
                &conductor->counters_reader, id, AERON_COUNTER_CLIENT_HEARTBEAT_TIMESTAMP_TYPE_ID, conductor->client_id))
            {
                char buffer[AERON_MAX_PATH];

                conductor->is_terminating = true;
                // TODO: forceCloseReasources
                snprintf(buffer, sizeof(buffer) - 1, "unexpected close of heartbeat timestamp counter: %" PRId32, id);
                conductor->error_handler(conductor->error_handler_clientd, ETIMEDOUT, buffer);
                return -1;
            }

            aeron_counter_set_ordered(conductor->heartbeat_timestamp.addr, now_ms);
            conductor->time_of_last_keepalive_ns = now_ns;
        }

        return 1;
    }

    return 0;
}

int aeron_client_conductor_check_lingering_resources(aeron_client_conductor_t *conductor, long long now_ns)
{
    int work_count = 0;

    for (size_t i = 0, size = conductor->lingering_resources.length, last_index = size - 1; i < size; i++)
    {
        aeron_client_managed_resource_t *resource = &conductor->lingering_resources.array[i];

        if (now_ns > (resource->time_of_last_state_change_ns + (long long)conductor->resource_linger_duration_ns))
        {
            // TODO: delete resource
            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->lingering_resources.array,
                sizeof(aeron_client_managed_resource_t),
                i,
                last_index);
            conductor->lingering_resources.length--;
            work_count += 1;
        }
    }

    return work_count;
}

int aeron_client_conductor_check_registering_resources(aeron_client_conductor_t *conductor, long long now_ns)
{
    int work_count = 0;

    for (size_t i = 0, size = conductor->registering_resources.length, last_index = size - 1; i < size; i++)
    {
        aeron_client_registering_resource_t *resource = conductor->registering_resources.array[i].resource;

        if (now_ns > resource->registration_deadline_ns)
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->registering_resources.array,
                sizeof(aeron_client_registering_resource_entry_t),
                i,
                last_index);
            conductor->registering_resources.length--;

            AERON_PUT_ORDERED(resource->registration_status, AERON_CLIENT_TIMEOUT_MEDIA_DRIVER);

            work_count += 1;
        }
    }

    return work_count;
}

int aeron_client_conductor_on_check_timeouts(aeron_client_conductor_t *conductor)
{
    int work_count = 0, result = 0;
    const long long now_ns = conductor->nano_clock();

    if (now_ns > (conductor->time_of_last_service_ns + AERON_CLIENT_CONDUCTOR_IDLE_SLEEP_NS))
    {
        if (now_ns > (conductor->time_of_last_service_ns + (long long)conductor->inter_service_timeout_ns))
        {
            char buffer[AERON_MAX_PATH];

            conductor->is_terminating = true;
            // TODO: forceCloseResources
            snprintf(buffer, sizeof(buffer) - 1,
                "service interval exceeded (ns): timeout=%" PRId64 ", interval=%" PRId64,
                (int64_t)conductor->inter_service_timeout_ns,
                (int64_t)(now_ns - conductor->time_of_last_service_ns));
            conductor->error_handler(conductor->error_handler_clientd, ETIMEDOUT, buffer);
            return -1;
        }

        conductor->time_of_last_service_ns = now_ns;

        if ((result = aeron_client_conductor_check_liveness(conductor, now_ns)) < 0)
        {
            return -1;
        }
        work_count += result;

        if ((result = aeron_client_conductor_check_lingering_resources(conductor, now_ns)) < 0)
        {
            return -1;
        }
        work_count += result;

        if ((result = aeron_client_conductor_check_registering_resources(conductor, now_ns)) < 0)
        {
            return -1;
        }
        work_count += result;
    }

    return work_count;
}

int aeron_client_conductor_do_work(aeron_client_conductor_t *conductor)
{
    int work_count = 0, result = 0;

    if (conductor->is_terminating)
    {
        return 0;
    }

    work_count += (int)aeron_mpsc_concurrent_array_queue_drain(
        conductor->command_queue, aeron_client_conductor_on_command, conductor, 10);

    work_count += aeron_broadcast_receiver_receive(
        &conductor->to_client_buffer, aeron_client_conductor_on_driver_response, conductor);

    if ((result = aeron_client_conductor_on_check_timeouts(conductor)) < 0)
    {
        return work_count;
    }
    work_count += result;

    return work_count;
}

void aeron_client_conductor_delete_log_buffer(void *clientd, int64_t key, void *value)
{
    aeron_log_buffer_delete((aeron_log_buffer_t *)value);
}

void aeron_client_conductor_on_close(aeron_client_conductor_t *conductor)
{
    aeron_int64_to_ptr_hash_map_for_each(
        &conductor->log_buffer_by_id_map, aeron_client_conductor_delete_log_buffer, NULL);
    aeron_mpsc_concurrent_array_queue_close(conductor->command_queue);
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
        &conductor->to_driver_buffer,
        AERON_COMMAND_ADD_PUBLICATION,
        buffer,
        sizeof(aeron_publication_command_t) + async->uri_length))
    {
        if (++rb_offer_fail_count > AERON_CLIENT_COMMAND_RB_FAIL_THRESHOLD)
        {
            char err_buffer[AERON_MAX_PATH];

            snprintf(err_buffer, sizeof(err_buffer) - 1, "ADD_PUBLICATION could not be sent (%s:%d)", __FILE__, __LINE__);
            conductor->error_handler(conductor->error_handler_clientd, ETIMEDOUT, err_buffer);
            return;
        }

        sched_yield();
    }

    AERON_ARRAY_ENSURE_CAPACITY(
        ensure_capacity_result, conductor->registering_resources, aeron_client_registering_resource_entry_t);
    if (ensure_capacity_result < 0)
    {
        char err_buffer[AERON_MAX_PATH];

        snprintf(err_buffer, sizeof(err_buffer) - 1, "publication registering_resources: %s", aeron_errmsg());
        conductor->error_handler(conductor->error_handler_clientd, aeron_errcode(), err_buffer);
        return;
    }

    conductor->registering_resources.array[conductor->registering_resources.length++].resource = async;
    async->registration_deadline_ns = conductor->nano_clock() + conductor->driver_timeout_ns;
}

void aeron_client_conductor_on_cmd_close_publication(void *clientd, void *item)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *) clientd;
    aeron_publication_t *publication = (aeron_publication_t *) item;

    if (publication->is_closed)
    {
        return;
    }

    aeron_int64_to_ptr_hash_map_remove(&conductor->resource_by_id_map, publication->registration_id);

    aeron_client_conductor_release_log_buffer(conductor, publication->log_buffer);
    aeron_publication_delete(publication);
}

void aeron_client_conductor_on_cmd_add_exclusive_publication(void *clientd, void *item)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *)clientd;
    aeron_async_add_exclusive_publication_t *async = (aeron_async_add_exclusive_publication_t *)item;

    char buffer[sizeof(aeron_publication_command_t) + AERON_MAX_PATH];
    aeron_publication_command_t *command = (aeron_publication_command_t *)buffer;
    int ensure_capacity_result = 0, rb_offer_fail_count = 0;

    command->correlated.correlation_id = async->registration_id;
    command->correlated.client_id = conductor->client_id;
    command->stream_id = async->stream_id;
    command->channel_length = async->uri_length;
    memcpy(buffer + sizeof(aeron_publication_command_t), async->uri, async->uri_length);

    while (AERON_RB_SUCCESS != aeron_mpsc_rb_write(
        &conductor->to_driver_buffer,
        AERON_COMMAND_ADD_EXCLUSIVE_PUBLICATION,
        buffer,
        sizeof(aeron_publication_command_t) + async->uri_length))
    {
        if (++rb_offer_fail_count > AERON_CLIENT_COMMAND_RB_FAIL_THRESHOLD)
        {
            char err_buffer[AERON_MAX_PATH];

            snprintf(err_buffer, sizeof(err_buffer) - 1, "ADD_EXCLUSIVE_PUBLICATION could not be sent (%s:%d)",
                __FILE__, __LINE__);
            conductor->error_handler(conductor->error_handler_clientd, ETIMEDOUT, err_buffer);
            return;
        }

        sched_yield();
    }

    AERON_ARRAY_ENSURE_CAPACITY(
        ensure_capacity_result, conductor->registering_resources, aeron_client_registering_resource_entry_t);
    if (ensure_capacity_result < 0)
    {
        char err_buffer[AERON_MAX_PATH];

        snprintf(err_buffer, sizeof(err_buffer) - 1, "exclusive_publication registering_resources: %s", aeron_errmsg());
        conductor->error_handler(conductor->error_handler_clientd, aeron_errcode(), err_buffer);
        return;
    }

    conductor->registering_resources.array[conductor->registering_resources.length++].resource = async;
    async->registration_deadline_ns = conductor->nano_clock() + conductor->driver_timeout_ns;
}

void aeron_client_conductor_on_cmd_close_exclusive_publication(void *clientd, void *item)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *) clientd;
    aeron_exclusive_publication_t *publication = (aeron_exclusive_publication_t *) item;

    if (publication->is_closed)
    {
        return;
    }

    aeron_int64_to_ptr_hash_map_remove(&conductor->resource_by_id_map, publication->registration_id);

    aeron_client_conductor_release_log_buffer(conductor, publication->log_buffer);
    aeron_exclusive_publication_delete(publication);
}

void aeron_client_conductor_on_cmd_add_subscription(void *clientd, void *item)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *)clientd;
    aeron_async_add_subscription_t *async = (aeron_async_add_subscription_t *)item;

    char buffer[sizeof(aeron_subscription_command_t) + AERON_MAX_PATH];
    aeron_subscription_command_t *command = (aeron_subscription_command_t *)buffer;
    int ensure_capacity_result = 0, rb_offer_fail_count = 0;

    command->correlated.correlation_id = async->registration_id;
    command->correlated.client_id = conductor->client_id;
    command->stream_id = async->stream_id;
    command->channel_length = async->uri_length;
    memcpy(buffer + sizeof(aeron_subscription_command_t), async->uri, async->uri_length);

    while (AERON_RB_SUCCESS != aeron_mpsc_rb_write(
        &conductor->to_driver_buffer,
        AERON_COMMAND_ADD_SUBSCRIPTION,
        buffer,
        sizeof(aeron_subscription_command_t) + async->uri_length))
    {
        if (++rb_offer_fail_count > AERON_CLIENT_COMMAND_RB_FAIL_THRESHOLD)
        {
            char err_buffer[AERON_MAX_PATH];

            snprintf(err_buffer, sizeof(err_buffer) - 1, "ADD_SUBSCRIPTION could not be sent (%s:%d)",
                __FILE__, __LINE__);
            conductor->error_handler(conductor->error_handler_clientd, ETIMEDOUT, err_buffer);
            return;
        }

        sched_yield();
    }

    AERON_ARRAY_ENSURE_CAPACITY(
        ensure_capacity_result, conductor->registering_resources, aeron_client_registering_resource_entry_t);
    if (ensure_capacity_result < 0)
    {
        char err_buffer[AERON_MAX_PATH];

        snprintf(err_buffer, sizeof(err_buffer) - 1, "subscription registering_resources: %s", aeron_errmsg());
        conductor->error_handler(conductor->error_handler_clientd, aeron_errcode(), err_buffer);
        return;
    }

    conductor->registering_resources.array[conductor->registering_resources.length++].resource = async;
    async->registration_deadline_ns = conductor->nano_clock() + conductor->driver_timeout_ns;
}

void aeron_client_conductor_on_cmd_close_subscription(void *clientd, void *item)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *)clientd;
    aeron_subscription_t *subscription = (aeron_subscription_t *)item;

    if (subscription->is_closed)
    {
        return;
    }

    aeron_int64_to_ptr_hash_map_remove(&conductor->resource_by_id_map, subscription->registration_id);

    // TODO: release images
    aeron_subscription_delete(subscription);
}

int aeron_client_conductor_command_offer(aeron_mpsc_concurrent_array_queue_t *command_queue, void *cmd)
{
    int fail_count = 0;

    while (aeron_mpsc_concurrent_array_queue_offer(command_queue, cmd) != AERON_OFFER_SUCCESS)
    {
        if (++fail_count > AERON_CLIENT_COMMAND_QUEUE_FAIL_THRESHOLD)
        {
            aeron_set_err(ETIMEDOUT, "%s", "could not offer to conductor command queue");
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
    cmd->uri_length = (int32_t)uri_length;
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
        if (aeron_client_conductor_command_offer(conductor->command_queue, cmd) < 0)
        {
            aeron_free(cmd->uri);
            aeron_free(cmd);
            return -1;
        }

        *async = cmd;
    }

    return 0;
}

int aeron_client_conductor_async_close_publication(
    aeron_client_conductor_t *conductor, aeron_publication_t *publication)
{
    publication->command_base.func = aeron_client_conductor_on_cmd_close_publication;
    publication->command_base.item = NULL;

    if (conductor->invoker_mode)
    {
        aeron_client_conductor_on_cmd_close_publication(conductor, publication);
    }
    else
    {
        if (aeron_client_conductor_command_offer(conductor->command_queue, publication) < 0)
        {
            return -1;
        }
    }

    return 0;
}

int aeron_client_conductor_async_add_exclusive_publication(
    aeron_async_add_exclusive_publication_t **async, aeron_client_conductor_t *conductor, const char *uri, int32_t stream_id)
{
    aeron_async_add_exclusive_publication_t *cmd = NULL;
    char *uri_copy = NULL;
    size_t uri_length = strlen(uri);

    *async = NULL;

    if (aeron_alloc((void **)&cmd, sizeof(aeron_async_add_exclusive_publication_t)) < 0 ||
        aeron_alloc((void **)&uri_copy, uri_length + 1) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "aeron_async_add_exclusive_publication (%d): %s", errcode, strerror(errcode));
        return -1;
    }

    memcpy(uri_copy, uri, uri_length);
    uri_copy[uri_length] = '\0';

    cmd->command_base.func = aeron_client_conductor_on_cmd_add_exclusive_publication;
    cmd->command_base.item = NULL;
    cmd->resource.exclusive_publication = NULL;
    cmd->error_message = NULL;
    cmd->uri = uri_copy;
    cmd->uri_length = (int32_t)uri_length;
    cmd->stream_id = stream_id;
    cmd->registration_id = -1;
    cmd->registration_status = AERON_CLIENT_AWAITING_MEDIA_DRIVER;
    cmd->type = AERON_CLIENT_TYPE_EXCLUSIVE_PUBLICATION;

    if (conductor->invoker_mode)
    {
        *async = cmd;
        aeron_client_conductor_on_cmd_add_exclusive_publication(conductor, cmd);
    }
    else
    {
        if (aeron_client_conductor_command_offer(conductor->command_queue, cmd) < 0)
        {
            aeron_free(cmd->uri);
            aeron_free(cmd);
            return -1;
        }

        *async = cmd;
    }

    return 0;
}

int aeron_client_conductor_async_close_exclusive_publication(
    aeron_client_conductor_t *conductor, aeron_exclusive_publication_t *publication)
{
    publication->command_base.func = aeron_client_conductor_on_cmd_close_exclusive_publication;
    publication->command_base.item = NULL;

    if (conductor->invoker_mode)
    {
        aeron_client_conductor_on_cmd_close_exclusive_publication(conductor, publication);
    }
    else
    {
        if (aeron_client_conductor_command_offer(conductor->command_queue, publication) < 0)
        {
            return -1;
        }
    }

    return 0;
}

int aeron_client_conductor_async_add_subscription(
    aeron_async_add_subscription_t **async,
    aeron_client_conductor_t *conductor,
    const char *uri,
    int32_t stream_id,
    aeron_on_available_image_t on_available_image_handler,
    void *on_available_image_clientd,
    aeron_on_unavailable_image_t on_unavailable_image_handler,
    void *on_unavailable_image_clientd)
{
    aeron_async_add_subscription_t *cmd = NULL;
    char *uri_copy = NULL;
    size_t uri_length = strlen(uri);

    *async = NULL;

    if (aeron_alloc((void **)&cmd, sizeof(aeron_async_add_subscription_t)) < 0 ||
        aeron_alloc((void **)&uri_copy, uri_length + 1) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "aeron_async_add_subscription (%d): %s", errcode, strerror(errcode));
        return -1;
    }

    memcpy(uri_copy, uri, uri_length);
    uri_copy[uri_length] = '\0';

    cmd->command_base.func = aeron_client_conductor_on_cmd_add_subscription;
    cmd->command_base.item = NULL;
    cmd->resource.subscription = NULL;
    cmd->error_message = NULL;
    cmd->uri = uri_copy;
    cmd->uri_length = (int32_t)uri_length;
    cmd->stream_id = stream_id;
    cmd->registration_id = -1;
    cmd->on_available_image = on_unavailable_image_handler;
    cmd->on_available_image_clientd = on_available_image_clientd;
    cmd->on_unavailable_image = on_unavailable_image_handler;
    cmd->on_unavailable_image_clientd = on_unavailable_image_clientd;
    cmd->registration_status = AERON_CLIENT_AWAITING_MEDIA_DRIVER;
    cmd->type = AERON_CLIENT_TYPE_SUBSCRIPTION;

    if (conductor->invoker_mode)
    {
        *async = cmd;
        aeron_client_conductor_on_cmd_add_subscription(conductor, cmd);
    }
    else
    {
        if (aeron_client_conductor_command_offer(conductor->command_queue, cmd) < 0)
        {
            aeron_free(cmd->uri);
            aeron_free(cmd);
            return -1;
        }

        *async = cmd;
    }

    return 0;
}

int aeron_client_conductor_async_close_subscription(
    aeron_client_conductor_t *conductor, aeron_subscription_t *subscription)
{
    subscription->command_base.func = aeron_client_conductor_on_cmd_close_subscription;
    subscription->command_base.item = NULL;

    if (conductor->invoker_mode)
    {
        aeron_client_conductor_on_cmd_close_subscription(conductor, subscription);
    }
    else
    {
        if (aeron_client_conductor_command_offer(conductor->command_queue, subscription) < 0)
        {
            return -1;
        }
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

int aeron_client_conductor_get_or_create_log_buffer(
    aeron_client_conductor_t *conductor,
    aeron_log_buffer_t **log_buffer,
    const char *log_file,
    int64_t original_registration_id,
    bool pre_touch)
{
    if (NULL == (*log_buffer = aeron_int64_to_ptr_hash_map_get(
        &conductor->log_buffer_by_id_map, original_registration_id)))
    {
        if (aeron_log_buffer_create(log_buffer, log_file, original_registration_id, pre_touch) < 0)
        {
            return -1;
        }

        if (aeron_int64_to_ptr_hash_map_put(
            &conductor->log_buffer_by_id_map, original_registration_id, *log_buffer) < 0)
        {
            int errcode = errno;

            aeron_set_err(errcode, "aeron_client_conductor_get_or_create_log_buffer (%d): %s",
                errcode, strerror(errcode));
            aeron_log_buffer_delete(*log_buffer);
            return -1;
        }
    }

    (*log_buffer)->refcnt++;

    return 0;
}

int aeron_client_conductor_release_log_buffer(
    aeron_client_conductor_t *conductor,
    aeron_log_buffer_t *log_buffer)
{
    if (--log_buffer->refcnt <= 0)
    {
        aeron_int64_to_ptr_hash_map_remove(&conductor->log_buffer_by_id_map, log_buffer->correlation_id);

        aeron_log_buffer_delete(log_buffer);
    }

    return 0;
}

int aeron_client_conductor_on_publication_ready(
    aeron_client_conductor_t *conductor, aeron_publication_buffers_ready_t *response)
{
    for (size_t i = 0, size = conductor->registering_resources.length, last_index = size - 1; i < size; i++)
    {
        aeron_client_registering_resource_t *resource = conductor->registering_resources.array[i].resource;
        char log_file[AERON_MAX_PATH];

        if (response->correlation_id == resource->registration_id)
        {
            bool is_exclusive = (AERON_CLIENT_TYPE_EXCLUSIVE_PUBLICATION == resource->type) ? true : false;

            memcpy(
                log_file,
                (const char *)response + sizeof(aeron_publication_buffers_ready_t),
                response->log_file_length);
            log_file[response->log_file_length] = '\0';

            aeron_log_buffer_t *log_buffer;

            if (aeron_client_conductor_get_or_create_log_buffer(
                conductor, &log_buffer, log_file, response->registration_id, conductor->pre_touch) < 0)
            {
                return -1;
            }

            if (is_exclusive)
            {
                aeron_exclusive_publication_t *publication = NULL;
                int64_t *position_limit_addr = aeron_counters_reader_addr(
                    &conductor->counters_reader, response->position_limit_counter_id);
                int64_t *channel_status_indicator_addr = aeron_counters_reader_addr(
                    &conductor->counters_reader, response->channel_status_indicator_id);

                if (aeron_exclusive_publication_create(
                    &publication,
                    conductor,
                    resource->uri,
                    resource->stream_id,
                    response->session_id,
                    position_limit_addr,
                    channel_status_indicator_addr,
                    log_buffer,
                    response->registration_id,
                    response->correlation_id) < 0)
                {
                    return -1;
                }

                resource->resource.exclusive_publication = publication;

                if (aeron_int64_to_ptr_hash_map_put(
                    &conductor->resource_by_id_map, resource->registration_id, publication) < 0)
                {
                    int errcode = errno;

                    aeron_set_err(errcode, "on_publication_ready - resource_by_id_map put (%d): %s",
                        errcode, strerror(errcode));
                    return -1;
                }
            }
            else
            {
                aeron_publication_t *publication = NULL;
                int64_t *position_limit_addr = aeron_counters_reader_addr(
                    &conductor->counters_reader, response->position_limit_counter_id);
                int64_t *channel_status_indicator_addr = aeron_counters_reader_addr(
                    &conductor->counters_reader, response->channel_status_indicator_id);

                if (aeron_publication_create(
                    &publication,
                    conductor,
                    resource->uri,
                    resource->stream_id,
                    response->session_id,
                    position_limit_addr,
                    channel_status_indicator_addr,
                    log_buffer,
                    response->registration_id,
                    response->correlation_id) < 0)
                {
                    return -1;
                }

                resource->resource.publication = publication;

                if (aeron_int64_to_ptr_hash_map_put(
                    &conductor->resource_by_id_map, resource->registration_id, publication) < 0)
                {
                    int errcode = errno;

                    aeron_set_err(errcode, "on_publication_ready - resource_by_id_map put (%d): %s",
                        errcode, strerror(errcode));
                    return -1;
                }
            }

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

int aeron_client_conductor_on_subscription_ready(
    aeron_client_conductor_t *conductor, aeron_subscription_ready_t *response)
{
    for (size_t i = 0, size = conductor->registering_resources.length, last_index = size - 1; i < size; i++)
    {
        aeron_client_registering_resource_t *resource = conductor->registering_resources.array[i].resource;

        if (response->correlation_id == resource->registration_id)
        {
            aeron_subscription_t *subscription;
            int64_t *channel_status_indicator_addr = aeron_counters_reader_addr(
                &conductor->counters_reader, response->channel_status_indicator_id);

            if (aeron_subscription_create(
                &subscription,
                conductor,
                resource->uri,
                resource->stream_id,
                resource->registration_id,
                channel_status_indicator_addr,
                resource->on_available_image,
                resource->on_available_image_clientd,
                resource->on_unavailable_image,
                resource->on_unavailable_image_clientd) < 0)
            {
                return -1;
            }

            resource->resource.subscription = subscription;

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->registering_resources.array,
                sizeof(aeron_client_registering_resource_entry_t),
                i,
                last_index);
            conductor->registering_resources.length--;

            if (aeron_int64_to_ptr_hash_map_put(
                &conductor->resource_by_id_map, resource->registration_id, subscription) < 0)
            {
                int errcode = errno;

                aeron_set_err(errcode, "on_subscription_ready - resource_by_id_map put (%d): %s",
                    errcode, strerror(errcode));
                return -1;
            }

            AERON_PUT_ORDERED(resource->registration_status, AERON_CLIENT_REGISTERED_MEDIA_DRIVER);
            break;
        }
    }

    return 0;
}

aeron_subscription_t *aeron_client_conductor_find_subscription_by_id(
    aeron_client_conductor_t *conductor, int64_t registration_id)
{
    aeron_subscription_t *subscription = aeron_int64_to_ptr_hash_map_get(
        &conductor->resource_by_id_map, registration_id);

    if (NULL != subscription && AERON_CLIENT_TYPE_SUBSCRIPTION == subscription->command_base.type)
    {
        return subscription;
    }

    return NULL;
}

int aeron_client_conductor_on_available_image(
    aeron_client_conductor_t *conductor,
    aeron_image_buffers_ready_t *response,
    int32_t log_file_length,
    const char *log_file,
    int32_t source_identity_length,
    const char *source_identity)
{
    aeron_subscription_t *subscription = aeron_client_conductor_find_subscription_by_id(
        conductor, response->subscriber_registration_id);

    if (NULL != subscription)
    {
        char log_file_str[AERON_MAX_PATH], source_identity_str[AERON_MAX_PATH];

        memcpy(log_file_str, log_file, log_file_length);
        log_file_str[log_file_length] = '\0';
        memcpy(source_identity_str, source_identity, source_identity_length);
        source_identity_str[source_identity_length] = '\0';

        aeron_log_buffer_t *log_buffer;

        if (aeron_client_conductor_get_or_create_log_buffer(
            conductor, &log_buffer, log_file, response->correlation_id, conductor->pre_touch) < 0)
        {
            return -1;
        }

        aeron_image_t *image;
        int64_t *subscriber_position = aeron_counters_reader_addr(
            &conductor->counters_reader, response->subscriber_position_id);

        if (aeron_image_create(
            &image,
            conductor,
            log_buffer,
            subscriber_position,
            response->correlation_id,
            response->session_id) < 0)
        {
            return -1;
        }

        if (aeron_int64_to_ptr_hash_map_put(&conductor->resource_by_id_map, response->correlation_id, image) < 0)
        {
            int errcode = errno;

            aeron_set_err(errcode, "on_available_image - resource_by_id_map put (%d): %s",
                errcode, strerror(errcode));
            return -1;
        }

        if (NULL != subscription->on_available_image)
        {
            subscription->on_available_image(subscription->on_available_image_clientd, image);
        }

        // TODO: add image to subscription
    }

    return 0;
}

int aeron_client_conductor_on_unavailable_image(
    aeron_client_conductor_t *conductor,
    aeron_image_message_t *response)
{
    aeron_subscription_t *subscription = aeron_client_conductor_find_subscription_by_id(
        conductor, response->subscription_registration_id);

    if (NULL != subscription)
    {
        aeron_image_t *image = aeron_int64_to_ptr_hash_map_remove(
            &conductor->resource_by_id_map, response->correlation_id);

        // TODO: remove image from subscription

        if (NULL != image)
        {
            if (NULL != subscription->on_unavailable_image)
            {
                subscription->on_unavailable_image(subscription->on_unavailable_image_clientd, image);
            }
        }
    }

    return 0;
}

extern int aeron_counter_heartbeat_timestamp_find_counter_id_by_registration_id(
    aeron_counters_reader_t *counters_reader, int32_t type_id, int64_t registration_id);
extern bool aeron_counter_heartbeat_timestamp_is_active(
    aeron_counters_reader_t *counters_reader, int32_t counter_id, int32_t type_id, int64_t registration_id);
