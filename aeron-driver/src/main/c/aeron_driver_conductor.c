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

#include <stdio.h>
#include <inttypes.h>
#include <errno.h>
#include "util/aeron_error.h"
#include "media/aeron_send_channel_endpoint.h"
#include "util/aeron_arrayutil.h"
#include "aeron_driver_conductor.h"
#include "aeron_position.h"
#include "aeron_driver_sender.h"
#include "aeron_driver_sender_proxy.h"

static void aeron_error_log_resource_linger(uint8_t *resource)
{
    /* TODO: use driver conductor MPSC command queue. Then linger. */
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

    if (aeron_str_to_ptr_hash_map_init(
        &conductor->send_channel_endpoint_by_channel_map, 64, AERON_STR_TO_PTR_HASH_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        return -1;
    }

    if (aeron_str_to_ptr_hash_map_init(
        &conductor->receive_channel_endpoint_by_channel_map, 64, AERON_STR_TO_PTR_HASH_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        return -1;
    }

    conductor->conductor_proxy.command_queue = &context->conductor_command_queue;
    conductor->conductor_proxy.fail_counter =
        aeron_counter_addr(&conductor->counters_manager, AERON_SYSTEM_COUNTER_CONDUCTOR_PROXY_FAILS);
    conductor->conductor_proxy.threading_mode = context->threading_mode;
    conductor->conductor_proxy.conductor = conductor;

    conductor->clients.array = NULL;
    conductor->clients.capacity = 0;
    conductor->clients.length = 0;
    conductor->clients.on_time_event = aeron_client_on_time_event;
    conductor->clients.has_reached_end_of_life = aeron_client_has_reached_end_of_life;
    conductor->clients.delete_func = aeron_client_delete;

    conductor->ipc_publications.array = NULL;
    conductor->ipc_publications.length = 0;
    conductor->ipc_publications.capacity = 0;
    conductor->ipc_publications.on_time_event = aeron_ipc_publication_entry_on_time_event;
    conductor->ipc_publications.has_reached_end_of_life = aeron_ipc_publication_entry_has_reached_end_of_life;
    conductor->ipc_publications.delete_func = aeron_ipc_publication_entry_delete;

    conductor->ipc_subscriptions.array = NULL;
    conductor->ipc_subscriptions.length = 0;
    conductor->ipc_subscriptions.capacity = 0;

    conductor->errors_counter = aeron_counter_addr(&conductor->counters_manager, AERON_SYSTEM_COUNTER_ERRORS);
    conductor->client_keep_alives_counter =
        aeron_counter_addr(&conductor->counters_manager, AERON_SYSTEM_COUNTER_CLIENT_KEEP_ALIVES);

    conductor->nano_clock = context->nano_clock;
    conductor->epoch_clock = context->epoch_clock;
    conductor->next_session_id = aeron_randomised_int32();
    conductor->time_of_last_timeout_check_ns = context->nano_clock();

    conductor->context = context;
    return 0;
}

#define AERON_DRIVER_CONDUCTOR_ENSURE_CAPACITY(r,a,t) \
if (a.length >= a.capacity) \
{ \
    size_t new_capacity = (0 == a.capacity) ? 2 : (a.capacity + (a.capacity >> 1)); \
    r = aeron_array_ensure_capacity((uint8_t **)&a.array, sizeof(t), a.capacity, new_capacity); \
    if (r >= 0) \
    { \
       a.capacity = new_capacity; \
    } \
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

aeron_client_t *aeron_driver_conductor_get_or_add_client(aeron_driver_conductor_t *conductor, int64_t client_id)
{
    aeron_client_t *client = NULL;
    int index;

    if ((index = aeron_driver_conductor_find_client(conductor, client_id)) == -1)
    {
        int ensure_capacity_result = 0;

        AERON_DRIVER_CONDUCTOR_ENSURE_CAPACITY(ensure_capacity_result, conductor->clients, aeron_client_t);

        if (ensure_capacity_result >= 0)
        {
            index = (int) conductor->clients.length;
            client = &conductor->clients.array[index];

            client->client_id = client_id;
            client->reached_end_of_life = false;
            client->time_of_last_keepalive = conductor->context->nano_clock();
            client->client_liveness_timeout_ns = conductor->context->client_liveness_timeout_ns;
            client->publication_links.array = NULL;
            client->publication_links.length = 0;
            client->publication_links.capacity = 0;
            conductor->clients.length++;
        }
    }
    else
    {
        client = &conductor->clients.array[index];
    }

    return client;
}

void aeron_client_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_client_t *client, int64_t now_ns, int64_t now_ms)
{
    if (now_ns > (client->time_of_last_keepalive + client->client_liveness_timeout_ns))
    {
        client->reached_end_of_life = true;
    }
}

bool aeron_client_has_reached_end_of_life(aeron_driver_conductor_t *conductor, aeron_client_t *client)
{
    return client->reached_end_of_life;
}

void aeron_client_delete(aeron_driver_conductor_t *conductor, aeron_client_t *client)
{
    for (size_t i = 0; i < client->publication_links.length; i++)
    {
        aeron_driver_managed_resource_t *resource = client->publication_links.array[i].resource;

        resource->decref(resource->clientd);
    }

    for (size_t i = 0, size = conductor->ipc_subscriptions.length, last_index = size - 1; i < size; i++)
    {
        aeron_subscription_link_t *link = &conductor->ipc_subscriptions.array[i];

        if (client->client_id == link->client_id)
        {
            aeron_driver_conductor_unlink_all_subscribeable(conductor, link);

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->ipc_subscriptions.array, sizeof(aeron_subscription_link_t), i, last_index);
            conductor->ipc_subscriptions.length--;
        }
    }

    /* TODO: clean out spy and network subscriptions connected to client_id */

    client->publication_links.length = 0; /* reuse array if it exists. */
    client->client_id = -1;
}

void aeron_ipc_publication_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_ipc_publication_entry_t *entry, int64_t now_ns, int64_t now_ms)
{
    aeron_ipc_publication_t *publication = entry->publication;

    aeron_ipc_publication_on_time_event(publication, now_ns, now_ms);

    switch (publication->conductor_fields.status)
    {
        case AERON_IPC_PUBLICATION_STATUS_INACTIVE:
            if (aeron_ipc_publication_is_drained(publication))
            {
                publication->conductor_fields.status = AERON_IPC_PUBLICATION_STATUS_LINGER;
                publication->conductor_fields.managed_resource.time_of_last_status_change = now_ns;
                aeron_driver_conductor_on_unavailable_image(
                    conductor,
                    publication->conductor_fields.managed_resource.registration_id,
                    publication->stream_id,
                    AERON_IPC_CHANNEL,
                    strlen(AERON_IPC_CHANNEL));
            }
            break;

        case AERON_IPC_PUBLICATION_STATUS_LINGER:
            if (now_ns >
                (publication->conductor_fields.managed_resource.time_of_last_status_change + publication->linger_timeout_ns))
            {
                publication->conductor_fields.has_reached_end_of_life = true;
            }
            break;

        default:
            break;
    }
}

bool aeron_ipc_publication_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_ipc_publication_entry_t *entry)
{
    return aeron_ipc_publication_has_reached_end_of_life(entry->publication);
}

void aeron_ipc_publication_entry_delete(
    aeron_driver_conductor_t *conductor, aeron_ipc_publication_entry_t *entry)
{
    for (size_t i = 0, size = conductor->ipc_subscriptions.length; i < size; i++)
    {
        aeron_subscription_link_t *link = &conductor->ipc_subscriptions.array[i];

        aeron_driver_conductor_unlink_subscribeable(link, &entry->publication->conductor_fields.subscribeable);
    }

    aeron_ipc_publication_close(&conductor->counters_manager, entry->publication);
    entry->publication = NULL;
}

#define AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(c, l,t,now_ns,now_ms) \
for (int last_index = (int)l.length - 1, i = last_index; i >= 0; i--) \
{ \
    t *elem = &l.array[i]; \
    l.on_time_event(c, elem, now_ns, now_ms); \
    if (l.has_reached_end_of_life(c, elem)) \
    { \
        l.delete_func(c, elem); \
        aeron_array_fast_unordered_remove((uint8_t *)l.array, sizeof(t), i, last_index); \
        last_index--; \
        l.length--; \
    } \
}

void aeron_driver_conductor_on_check_managed_resources(
    aeron_driver_conductor_t *conductor, int64_t now_ns, int64_t now_ms)
{
    AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(
        conductor, conductor->clients, aeron_client_t, now_ns, now_ms);
    AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(
        conductor, conductor->ipc_publications, aeron_ipc_publication_entry_t, now_ns, now_ms);
}

aeron_ipc_publication_t *aeron_driver_conductor_get_or_add_ipc_publication(
    aeron_driver_conductor_t *conductor,
    aeron_client_t *client,
    int64_t registration_id,
    int32_t stream_id,
    bool is_exclusive)
{
    aeron_ipc_publication_t *publication = NULL;
    int ensure_capacity_result = 0;

    if (!is_exclusive)
    {
        for (size_t i = 0; i < conductor->ipc_publications.length; i++)
        {
            aeron_ipc_publication_t *pub_entry = conductor->ipc_publications.array[i].publication;

            if (stream_id == pub_entry->stream_id &&
                !pub_entry->is_exclusive &&
                pub_entry->conductor_fields.status == AERON_IPC_PUBLICATION_STATUS_ACTIVE)
            {
                publication = pub_entry;
                break;
            }
        }
    }

    AERON_DRIVER_CONDUCTOR_ENSURE_CAPACITY(ensure_capacity_result, client->publication_links, aeron_publication_link_t);

    if (ensure_capacity_result >= 0)
    {
        if (NULL == publication)
        {
            AERON_DRIVER_CONDUCTOR_ENSURE_CAPACITY(ensure_capacity_result, conductor->ipc_publications, aeron_ipc_publication_entry_t);

            if (ensure_capacity_result >= 0)
            {
                int32_t session_id = conductor->next_session_id++;
                int32_t initial_term_id = aeron_randomised_int32();
                aeron_position_t pub_lmt_position;

                pub_lmt_position.counter_id =
                    aeron_counter_publisher_limit_allocate(
                        &conductor->counters_manager, registration_id, session_id, stream_id, AERON_IPC_CHANNEL);
                pub_lmt_position.value_addr =
                    aeron_counter_addr(&conductor->counters_manager, (int32_t)pub_lmt_position.counter_id);

                if (pub_lmt_position.counter_id >= 0 &&
                    aeron_ipc_publication_create(
                        &publication,
                        conductor->context,
                        session_id,
                        stream_id,
                        registration_id,
                        &pub_lmt_position,
                        initial_term_id,
                        conductor->context->ipc_term_buffer_length,
                        conductor->context->mtu_length,
                        is_exclusive) >= 0)
                {
                    client->publication_links.array[client->publication_links.length++].resource =
                        &publication->conductor_fields.managed_resource;

                    conductor->ipc_publications.array[conductor->ipc_publications.length++].publication = publication;

                    publication->conductor_fields.managed_resource.time_of_last_status_change =
                        conductor->nano_clock();
                }
            }
        }
        else
        {
            client->publication_links.array[client->publication_links.length++].resource =
                &publication->conductor_fields.managed_resource;

            publication->conductor_fields.managed_resource.incref(publication->conductor_fields.managed_resource.clientd);
        }
    }

    return ensure_capacity_result >= 0 ? publication : NULL;
}

aeron_send_channel_endpoint_t *aeron_driver_conductor_get_or_add_send_channel_endpoint(
    aeron_driver_conductor_t *conductor, aeron_udp_channel_t *channel)
{
    aeron_send_channel_endpoint_t *endpoint =
        aeron_str_to_ptr_hash_map_get(
            &conductor->send_channel_endpoint_by_channel_map, channel->canonical_form, channel->canonical_length);

    if (NULL == endpoint)
    {
        aeron_counter_t status_indicator;

        status_indicator.counter_id =
            aeron_counter_send_channel_status_allocate(&conductor->counters_manager, channel->original_uri);

        status_indicator.value_addr =
            aeron_counter_addr(&conductor->counters_manager, (int32_t)status_indicator.counter_id);

        if (status_indicator.counter_id < 0 ||
            aeron_send_channel_endpoint_create(&endpoint, channel, &status_indicator, conductor->context) < 0)
        {
            return NULL;
        }

        if (aeron_str_to_ptr_hash_map_put(
            &conductor->send_channel_endpoint_by_channel_map,
            channel->canonical_form,
            channel->canonical_length,
            endpoint) < 0)
        {
            aeron_send_channel_endpoint_delete(&conductor->counters_manager, endpoint);
            return NULL;
        }

        aeron_driver_sender_proxy_add_endpoint(conductor->context->sender_proxy, endpoint);

        *status_indicator.value_addr = AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ACTIVE;
    }

    return endpoint;
}

void aeron_driver_conductor_client_transmit(
    aeron_driver_conductor_t *conductor,
    int32_t msg_type_id,
    const void *msg,
    size_t length)
{
    aeron_broadcast_transmitter_transmit(&conductor->to_clients, msg_type_id, msg, length);
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
    memcpy(response_buffer + sizeof(aeron_error_response_t), message, length);

    aeron_driver_conductor_client_transmit(
        conductor, AERON_RESPONSE_ON_ERROR, response, sizeof(aeron_error_response_t) + length);
}

void aeron_driver_conductor_on_publication_ready(
    aeron_driver_conductor_t *conductor,
    int64_t registration_id,
    int32_t stream_id,
    int32_t session_id,
    int32_t position_limit_counter_id,
    bool is_exclusive,
    const char *log_file_name,
    size_t log_file_name_length)
{
    char response_buffer[sizeof(aeron_publication_buffers_ready_t) + AERON_MAX_PATH];
    aeron_publication_buffers_ready_t *response = (aeron_publication_buffers_ready_t *)response_buffer;

    response->correlation_id = registration_id;
    response->stream_id = stream_id;
    response->session_id = session_id;
    response->position_limit_counter_id = position_limit_counter_id;
    response->log_file_length = (int32_t)log_file_name_length;
    memcpy(response_buffer + sizeof(aeron_publication_buffers_ready_t), log_file_name, log_file_name_length);

    aeron_driver_conductor_client_transmit(
        conductor,
        is_exclusive ? AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY : AERON_RESPONSE_ON_PUBLICATION_READY,
        response,
        sizeof(aeron_publication_buffers_ready_t) + log_file_name_length);
}

void aeron_driver_conductor_on_operation_succeeded(
    aeron_driver_conductor_t *conductor,
    int64_t correlation_id)
{
    char response_buffer[sizeof(aeron_correlated_command_t)];
    aeron_correlated_command_t *response = (aeron_correlated_command_t *)response_buffer;

    response->client_id = 0;
    response->correlation_id = correlation_id;

    aeron_driver_conductor_client_transmit(
        conductor, AERON_RESPONSE_ON_OPERATION_SUCCESS, response, sizeof(aeron_correlated_command_t));
}

#define AERON_MAX_SUB_POSITIONS_PER_MESSAGE 10

void aeron_driver_conductor_on_available_image(
    aeron_driver_conductor_t *conductor,
    int64_t correlation_id,
    int32_t stream_id,
    int32_t session_id,
    const char *log_file_name,
    size_t log_file_name_length,
    const aeron_image_buffers_ready_subscriber_position_t *subscriber_positions,
    size_t subscriber_positions_count,
    const char *source_identity,
    size_t source_identity_length)
{
    char response_buffer[
        sizeof(aeron_image_buffers_ready_t) +
        (sizeof(aeron_image_buffers_ready_subscriber_position_t) * AERON_MAX_SUB_POSITIONS_PER_MESSAGE) +
        (2 * AERON_MAX_PATH)];
    char *response_ptr = response_buffer;
    char *ptr = response_buffer;
    aeron_image_buffers_ready_t *response;
    size_t subscriber_positions_length =
        sizeof(aeron_image_buffers_ready_subscriber_position_t) * subscriber_positions_count;
    size_t response_length =
        sizeof(aeron_image_buffers_ready_t) +
        subscriber_positions_length +
        log_file_name_length +
        source_identity_length +
        (2 * sizeof(int32_t));

    if (response_length > sizeof(response_buffer))
    {
        if (aeron_alloc((void **)&ptr, response_length) < 0)
        {
            return;
        }

        response_ptr = ptr;
    }

    response = (aeron_image_buffers_ready_t *)ptr;

    response->correlation_id = correlation_id;
    response->stream_id = stream_id;
    response->session_id = session_id;
    response->subscriber_position_block_length = sizeof(aeron_image_buffers_ready_subscriber_position_t);
    response->subscriber_position_count = (int32_t)subscriber_positions_count;
    ptr += sizeof(aeron_image_buffers_ready_t);

    memcpy(ptr, subscriber_positions, subscriber_positions_length);
    ptr += subscriber_positions_length;

    *((int32_t *)ptr) = (int32_t)log_file_name_length;
    ptr += sizeof(int32_t);
    memcpy(ptr, log_file_name, log_file_name_length);
    ptr += log_file_name_length;

    *((int32_t *)ptr) = (int32_t)source_identity_length;
    ptr += sizeof(int32_t);
    memcpy(ptr, source_identity, source_identity_length);
    /* ptr += source_identity_length; */

    aeron_driver_conductor_client_transmit(
        conductor, AERON_RESPONSE_ON_AVAILABLE_IMAGE, response_ptr, response_length);

    if (response_buffer != response_ptr)
    {
        aeron_free(response_ptr);
    }
}

void aeron_driver_conductor_on_unavailable_image(
    aeron_driver_conductor_t *conductor,
    int64_t correlation_id,
    int32_t stream_id,
    const char *channel,
    size_t channel_length)
{
    char response_buffer[sizeof(aeron_image_message_t) + AERON_MAX_PATH];
    aeron_image_message_t *response = (aeron_image_message_t *)response_buffer;

    response->correlation_id = correlation_id;
    response->stream_id = stream_id;
    response->channel_length = (int32_t)channel_length;
    memcpy(response_buffer + sizeof(aeron_image_message_t), channel, channel_length);

    aeron_driver_conductor_client_transmit(
        conductor, AERON_RESPONSE_ON_UNAVAILABLE_IMAGE, response, sizeof(aeron_image_message_t) + channel_length);
}

void aeron_driver_conductor_error(
    aeron_driver_conductor_t *conductor, int error_code, const char *description, const char *message)
{
    aeron_distinct_error_log_record(&conductor->error_log, error_code, description, message);
    aeron_counter_increment(conductor->errors_counter, 1);
}

void aeron_driver_conductor_on_command(int32_t msg_type_id, const void *message, size_t length, void *clientd)
{
    aeron_driver_conductor_t *conductor = (aeron_driver_conductor_t *)clientd;
    int64_t correlation_id = 0;
    int result = 0;

    char error_message[AERON_MAX_PATH] = "\0";
    char *error_description = "generic error";

    switch (msg_type_id)
    {
        case AERON_COMMAND_ADD_PUBLICATION:
        {
            aeron_publication_command_t *command = (aeron_publication_command_t *)message;

            if (length < sizeof(aeron_publication_command_t) ||
                length < (sizeof(aeron_publication_command_t) + command->channel_length))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;

            if (strncmp((const char *)message + sizeof(aeron_publication_command_t), AERON_IPC_CHANNEL, strlen(AERON_IPC_CHANNEL)) == 0)
            {
                result = aeron_driver_conductor_on_add_ipc_publication(conductor, command, false);
            }
            else
            {
                result = aeron_driver_conductor_on_add_network_publication(conductor, command, false);
            }
            break;
        }

        case AERON_COMMAND_ADD_EXCLUSIVE_PUBLICATION:
        {
            aeron_publication_command_t *command = (aeron_publication_command_t *)message;

            if (length < sizeof(aeron_publication_command_t) ||
                length < (sizeof(aeron_publication_command_t) + command->channel_length))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;

            if (strncmp((const char *)message + sizeof(aeron_publication_command_t), AERON_IPC_CHANNEL, strlen(AERON_IPC_CHANNEL)) == 0)
            {
                result = aeron_driver_conductor_on_add_ipc_publication(conductor, command, true);
            }
            else
            {
                result = aeron_driver_conductor_on_add_network_publication(conductor, command, true);
            }
            break;
        }

        case AERON_COMMAND_REMOVE_PUBLICATION:
        {
            aeron_remove_command_t *command = (aeron_remove_command_t *)message;

            if (length < sizeof(aeron_remove_command_t))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;

            result = aeron_driver_conductor_on_remove_publication(conductor, command);
            break;
        }

        case AERON_COMMAND_ADD_SUBSCRIPTION:
        {
            aeron_subscription_command_t *command = (aeron_subscription_command_t *)message;

            if (length < sizeof(aeron_subscription_command_t) ||
                length < (sizeof(aeron_subscription_command_t) + command->channel_length))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;

            if (strncmp((const char *)message + sizeof(aeron_subscription_command_t), AERON_IPC_CHANNEL, strlen(AERON_IPC_CHANNEL)) == 0)
            {
                result = aeron_driver_conductor_on_add_ipc_subscription(conductor, command);
            }
            else if (strncmp((const char *)message + sizeof(aeron_subscription_command_t), AERON_SPY_PREFIX, strlen(AERON_SPY_PREFIX)) == 0)
            {
                result = aeron_driver_conductor_on_add_spy_subscription(conductor, command);
            }
            else
            {
                result = aeron_driver_conductor_on_add_network_subscription(conductor, command);
            }
            break;
        }

        case AERON_COMMAND_REMOVE_SUBSCRIPTION:
        {
            aeron_remove_command_t *command = (aeron_remove_command_t *)message;

            if (length < sizeof(aeron_remove_command_t))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;

            result = aeron_driver_conductor_on_remove_subscription(conductor, command);
            break;
        }

        case AERON_COMMAND_CLIENT_KEEPALIVE:
        {
            aeron_correlated_command_t *command = (aeron_correlated_command_t *)message;

            if (length < sizeof(aeron_correlated_command_t))
            {
                goto malformed_command;
            }

            result = aeron_driver_conductor_on_client_keepalive(conductor, command->client_id);
            break;
        }

        default:
            AERON_FORMAT_BUFFER(error_message, "command=%d unknown", msg_type_id);
            aeron_driver_conductor_error(
                conductor, AERON_ERROR_CODE_UNKNOWN_COMMAND_TYPE_ID, "unknown command type id", error_message);
            break;
    }

    if (result < 0)
    {
        int os_errno = aeron_errcode();
        int code = os_errno > 0 ? -os_errno : AERON_ERROR_CODE_GENERIC_ERROR;

        error_description = strerror(os_errno);
        AERON_FORMAT_BUFFER(error_message, "(%d) %s: %s", os_errno, error_description, aeron_errmsg());
        aeron_driver_conductor_on_error(conductor, code, error_message, strlen(error_message), correlation_id);
        aeron_driver_conductor_error(conductor, code, error_description, error_message);
    }

    return;

    malformed_command:
        AERON_FORMAT_BUFFER(error_message, "command=%d too short: length=%lu", msg_type_id, length);
        aeron_driver_conductor_error(conductor, AERON_ERROR_CODE_MALFORMED_COMMAND, "command too short", error_message);
        return;
}

void aeron_driver_conductor_on_command_queue(void *clientd, volatile void *item)
{
    aeron_command_base_t *cmd = (aeron_command_base_t *)item;

    cmd->func(clientd, cmd);
}

int aeron_driver_conductor_do_work(void *clientd)
{
    aeron_driver_conductor_t *conductor = (aeron_driver_conductor_t *)clientd;
    int work_count = 0;
    int64_t now_ns = conductor->nano_clock();

    work_count +=
        (int)aeron_mpsc_rb_read(&conductor->to_driver_commands, aeron_driver_conductor_on_command, conductor, 10);
    work_count +=
        aeron_mpsc_concurrent_array_queue_drain(
            conductor->conductor_proxy.command_queue, aeron_driver_conductor_on_command_queue, conductor, 10);

    if (now_ns > (conductor->time_of_last_timeout_check_ns + AERON_DRIVER_CONDUCTOR_TIMEOUT_CHECK_NS))
    {
        int64_t now_ms = conductor->epoch_clock();

        aeron_mpsc_rb_consumer_heartbeat_time(&conductor->to_driver_commands, now_ms);
        aeron_driver_conductor_on_check_managed_resources(conductor, now_ns, now_ms);
        /* TODO: checkUnblock */
        conductor->time_of_last_timeout_check_ns = now_ns;
        work_count++;
    }

    for (size_t i = 0, length = conductor->ipc_publications.length; i < length; i++)
    {
        work_count += aeron_ipc_publication_update_pub_lmt(conductor->ipc_publications.array[i].publication);
    }

    return work_count;
}

void aeron_driver_conductor_on_close(void *clientd)
{
    aeron_driver_conductor_t *conductor = (aeron_driver_conductor_t *)clientd;

    for (size_t i = 0, length = conductor->clients.length; i < length; i++)
    {
        aeron_free(conductor->clients.array[i].publication_links.array);
    }
    aeron_free(conductor->clients.array);

    for (size_t i = 0, length = conductor->ipc_publications.length; i < length; i++)
    {
        aeron_ipc_publication_close(&conductor->counters_manager, conductor->ipc_publications.array[i].publication);
    }
    aeron_free(conductor->ipc_publications.array);

    for (size_t i = 0, length = conductor->ipc_subscriptions.length; i < length; i++)
    {
        aeron_free(conductor->ipc_subscriptions.array[i].subscribeable_list.array);
    }
    aeron_free(conductor->ipc_subscriptions.array);

    aeron_system_counters_close(&conductor->system_counters);
    aeron_counters_manager_close(&conductor->counters_manager);
    aeron_distinct_error_log_close(&conductor->error_log);

    aeron_str_to_ptr_hash_map_delete(&conductor->send_channel_endpoint_by_channel_map);
    aeron_str_to_ptr_hash_map_delete(&conductor->receive_channel_endpoint_by_channel_map);
}

#define AERON_ERROR(c, ecode, desc, format, ...) \
do \
{ \
    snprintf(c->stack_error.buffer, sizeof(c->stack_error.buffer) - 1, format, __VA_ARGS__); \
    c->stack_error.code = ecode; \
    c->stack_error.description = desc; \
} while (0)

int aeron_driver_subscribeable_add_position(
    aeron_subscribeable_t *subscribeable, int64_t counter_id, int64_t *value_addr)
{
    int ensure_capacity_result = 0, result = -1;

    AERON_DRIVER_CONDUCTOR_ENSURE_CAPACITY(ensure_capacity_result, (*subscribeable), aeron_position_t);

    if (ensure_capacity_result >= 0)
    {
        aeron_position_t *entry = &subscribeable->array[subscribeable->length++];
        entry->counter_id = counter_id;
        entry->value_addr = value_addr;
        result = 0;
    }

    return result;
}

void aeron_driver_subscribeable_remove_position(aeron_subscribeable_t *subscribeable, int64_t counter_id)
{
    for (size_t i = 0, size = subscribeable->length, last_index = size - 1; i < size; i++)
    {
        if (counter_id == subscribeable->array[i].counter_id)
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *)subscribeable->array, sizeof(aeron_position_t), i, last_index);
            subscribeable->length--;
        }
    }
}

int aeron_driver_conductor_link_ipc_subscribeable(
    aeron_driver_conductor_t *conductor,
    aeron_subscription_link_t *link,
    aeron_ipc_publication_t *ipc_publication)
{
    int ensure_capacity_result = 0, result = -1;

    AERON_DRIVER_CONDUCTOR_ENSURE_CAPACITY(ensure_capacity_result, link->subscribeable_list, aeron_subscribeable_list_entry_t);

    if (ensure_capacity_result >= 0)
    {
        aeron_image_buffers_ready_subscriber_position_t position;
        int64_t joining_position = aeron_ipc_publication_joining_position(ipc_publication);
        int32_t counter_id = aeron_counter_subscription_position_allocate(
            &conductor->counters_manager,
            link->registration_id,
            ipc_publication->session_id,
            ipc_publication->stream_id,
            AERON_IPC_CHANNEL,
            joining_position);

        if (counter_id >= 0)
        {
            int64_t *position_addr = aeron_counter_addr(&conductor->counters_manager, counter_id);

            if (aeron_driver_subscribeable_add_position(
                &ipc_publication->conductor_fields.subscribeable, counter_id, position_addr) >= 0)
            {
                aeron_subscribeable_list_entry_t
                    *entry = &link->subscribeable_list.array[link->subscribeable_list.length++];

                aeron_counter_set_value(position_addr, joining_position);
                position.indicator_id = counter_id;
                position.registration_id = link->registration_id;

                entry->subscribeable = &ipc_publication->conductor_fields.subscribeable;
                entry->counter_id = counter_id;

                aeron_driver_conductor_on_available_image(
                    conductor,
                    ipc_publication->conductor_fields.managed_resource.registration_id,
                    ipc_publication->stream_id,
                    ipc_publication->session_id,
                    ipc_publication->log_file_name,
                    ipc_publication->log_file_name_length,
                    &position,
                    1,
                    AERON_IPC_CHANNEL,
                    strlen(AERON_IPC_CHANNEL));

                result = 0;
            }
        }
    }

    return result;
}

void aeron_driver_conductor_unlink_subscribeable(aeron_subscription_link_t *link, aeron_subscribeable_t *subscribeable)
{
    for (int last_index = link->subscribeable_list.length - 1, i = last_index; i >= 0; i--)
    {
        if (subscribeable == link->subscribeable_list.array[i].subscribeable)
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *)link->subscribeable_list.array, sizeof(aeron_subscribeable_list_entry_t), i, last_index);
            link->subscribeable_list.length--;
            last_index--;
        }
    }
}

void aeron_driver_conductor_unlink_all_subscribeable(aeron_driver_conductor_t *conductor, aeron_subscription_link_t *link)
{
    for (size_t i = 0; i < link->subscribeable_list.length; i++)
    {
        aeron_subscribeable_list_entry_t *entry = &link->subscribeable_list.array[i];

        aeron_driver_subscribeable_remove_position(entry->subscribeable, entry->counter_id);
        aeron_counters_manager_free(&conductor->counters_manager, (int32_t)entry->counter_id);
    }
}

inline static bool aeron_driver_conductor_is_subscribeable_linked(
    aeron_subscription_link_t *link, aeron_subscribeable_t *subscribeable)
{
    bool result = false;

    for (size_t i = 0; i < link->subscribeable_list.length; i++)
    {
        aeron_subscribeable_list_entry_t *entry = &link->subscribeable_list.array[i];

        if (subscribeable == entry->subscribeable)
        {
            result = true;
            break;
        }
    }

    return result;
}

int aeron_driver_conductor_on_add_ipc_publication(
    aeron_driver_conductor_t *conductor,
    aeron_publication_command_t *command,
    bool is_exclusive)
{
    aeron_client_t *client = NULL;
    aeron_ipc_publication_t *publication = NULL;

    if ((client = aeron_driver_conductor_get_or_add_client(conductor, command->correlated.client_id)) == NULL ||
        (publication = aeron_driver_conductor_get_or_add_ipc_publication(
            conductor, client, command->correlated.correlation_id, command->stream_id, is_exclusive)) == NULL)
    {
        return -1;
    }

    aeron_subscribeable_t *subscribeable = &publication->conductor_fields.subscribeable;

    /* TODO: pre-populate OOM in distinct_error_log so that it never needs to allocate if OOMed */

    aeron_driver_conductor_on_publication_ready(
        conductor,
        command->correlated.correlation_id,
        publication->stream_id,
        publication->session_id,
        (int32_t)publication->pub_lmt_position.counter_id,
        is_exclusive,
        publication->log_file_name,
        publication->log_file_name_length);

    for (size_t i = 0; i < conductor->ipc_subscriptions.length; i++)
    {
        aeron_subscription_link_t *subscription_link = &conductor->ipc_subscriptions.array[i];

        /* could be old pub, so have to check to see if already linked */
        /* TODO: add test for case */
        if (command->stream_id == subscription_link->stream_id &&
            !aeron_driver_conductor_is_subscribeable_linked(subscription_link, subscribeable))
        {
            if (aeron_driver_conductor_link_ipc_subscribeable(conductor, subscription_link, publication) < 0)
            {
                return -1;
            }
        }
    }

    return 0;
}

int aeron_driver_conductor_on_add_network_publication(
    aeron_driver_conductor_t *conductor,
    aeron_publication_command_t *command,
    bool is_exclusive)
{
    aeron_set_err(ENOTSUP, "%s", "network publications not currently supported");
    return -1;
}

int aeron_driver_conductor_on_remove_publication(
    aeron_driver_conductor_t *conductor,
    aeron_remove_command_t *command)
{
    int index;

    if ((index = aeron_driver_conductor_find_client(conductor, command->correlated.client_id)) >= 0)
    {
        aeron_client_t *client = &conductor->clients.array[index];

        for (size_t i = 0, size = client->publication_links.length, last_index = size - 1; i < size; i++)
        {
            aeron_driver_managed_resource_t *resource = client->publication_links.array[i].resource;

            if (command->registration_id == resource->registration_id)
            {
                resource->decref(resource->clientd);

                aeron_array_fast_unordered_remove(
                    (uint8_t *)client->publication_links.array, sizeof(aeron_publication_link_t), i, last_index);

                aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);
                return 0;
            }
        }
    }

    aeron_set_err(
        EINVAL,
        "unknown publication client_id=%" PRId64 ", registration_id=%" PRId64,
        command->correlated.client_id,
        command->registration_id);
    return -1;
}

int aeron_driver_conductor_on_add_ipc_subscription(
    aeron_driver_conductor_t *conductor,
    aeron_subscription_command_t *command)
{
    aeron_client_t *client = NULL;
    int ensure_capacity_result = 0;

    if ((client = aeron_driver_conductor_get_or_add_client(conductor, command->correlated.client_id)) == NULL)
    {
        return -1;
    }

    AERON_DRIVER_CONDUCTOR_ENSURE_CAPACITY(ensure_capacity_result, conductor->ipc_subscriptions, aeron_subscription_link_t);
    if (ensure_capacity_result >= 0)
    {
        aeron_subscription_link_t *link = &conductor->ipc_subscriptions.array[conductor->ipc_subscriptions.length++];

        link->stream_id = command->stream_id;
        link->client_id = command->correlated.client_id;
        link->registration_id = command->correlated.correlation_id;
        link->subscribeable_list.length = 0;
        link->subscribeable_list.capacity = 0;
        link->subscribeable_list.array = NULL;

        aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);

        for (size_t i = 0; i < conductor->ipc_publications.length; i++)
        {
            aeron_ipc_publication_entry_t *publication_entry = &conductor->ipc_publications.array[i];

            if (command->stream_id == publication_entry->publication->stream_id)
            {
                aeron_ipc_publication_t *publication = publication_entry->publication;

                if (aeron_driver_conductor_link_ipc_subscribeable(conductor, link, publication) < 0)
                {
                    return -1;
                }
            }
        }

        return 0;
    }

    return -1;
}

int aeron_driver_conductor_on_add_spy_subscription(
    aeron_driver_conductor_t *conductor,
    aeron_subscription_command_t *command)
{
    aeron_set_err(ENOTSUP, "%s", "spy subscriptions not currently supported");
    return -1;
}

int aeron_driver_conductor_on_add_network_subscription(
    aeron_driver_conductor_t *conductor,
    aeron_subscription_command_t *command)
{
    aeron_set_err(ENOTSUP, "%s", "network subscriptions not currently supported");
    return -1;
}

int aeron_driver_conductor_on_remove_subscription(
    aeron_driver_conductor_t *conductor,
    aeron_remove_command_t *command)
{

    for (size_t i = 0, size = conductor->ipc_subscriptions.length, last_index = size - 1; i < size; i++)
    {
        aeron_subscription_link_t *link = &conductor->ipc_subscriptions.array[i];

        if (command->registration_id == link->registration_id)
        {
            aeron_driver_conductor_unlink_all_subscribeable(conductor, link);

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->ipc_subscriptions.array, sizeof(aeron_subscription_link_t), i, last_index);
            conductor->ipc_subscriptions.length--;

            aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);
            return 0;
        }
    }

    /* TODO: search network subscriptions */

    /* TODO: search spy subscriptions */

    aeron_set_err(
        EINVAL,
        "unknown subscription client_id=%" PRId64 ", registration_id=%" PRId64,
        command->correlated.client_id,
        command->registration_id);
    return -1;
}

int aeron_driver_conductor_on_client_keepalive(
    aeron_driver_conductor_t *conductor,
    int64_t client_id)
{
    int index;

    aeron_counter_add_ordered(conductor->client_keep_alives_counter, 1);

    if ((index = aeron_driver_conductor_find_client(conductor, client_id)) >= 0)
    {
        aeron_client_t *client = &conductor->clients.array[index];

        client->time_of_last_keepalive = conductor->nano_clock();
    }
    return 0;
}

extern size_t aeron_driver_conductor_num_clients(aeron_driver_conductor_t *conductor);
extern size_t aeron_driver_conductor_num_ipc_publications(aeron_driver_conductor_t *conductor);
extern size_t aeron_driver_conductor_num_ipc_subscriptions(aeron_driver_conductor_t *conductor);
extern size_t aeron_driver_conductor_num_active_ipc_subscriptions(aeron_driver_conductor_t *conductor, int32_t stream_id);
extern aeron_ipc_publication_t *aeron_driver_conductor_find_ipc_publication(
    aeron_driver_conductor_t *conductor, int64_t id);
extern int64_t *aeron_driver_conductor_system_counter_addr(
    aeron_driver_conductor_t *conductor, aeron_system_counter_enum_t type);
