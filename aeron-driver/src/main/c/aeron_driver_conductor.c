/*
 * Copyright 2014-2019 Real Logic Ltd.
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
#include "media/aeron_receive_channel_endpoint.h"
#include "util/aeron_netutil.h"
#include "util/aeron_error.h"
#include "media/aeron_send_channel_endpoint.h"
#include "util/aeron_arrayutil.h"
#include "aeron_driver_conductor.h"
#include "aeron_position.h"
#include "aeron_driver_sender.h"
#include "aeron_driver_receiver.h"
#include "aeron_publication_image.h"
#include "concurrent/aeron_logbuffer_unblocker.h"

static void aeron_error_log_resource_linger(void *clientd, uint8_t *resource)
{
    aeron_driver_conductor_t *conductor = (aeron_driver_conductor_t *)clientd;
    aeron_driver_conductor_proxy_on_linger_buffer(conductor->context->conductor_proxy, resource);
}

static int64_t aeron_driver_conductor_null_epoch_clock()
{
    return 0;
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

    int64_t free_to_reuse_timeout_ms = 0;
    aeron_clock_func_t clock_func = aeron_driver_conductor_null_epoch_clock;

    if (context->counter_free_to_reuse_ns > 0)
    {
        free_to_reuse_timeout_ms = context->counter_free_to_reuse_ns / (1000 * 1000L);
        free_to_reuse_timeout_ms = (free_to_reuse_timeout_ms <= 0) ? 1 : free_to_reuse_timeout_ms;
        clock_func = aeron_epoch_clock;
    }

    if (aeron_counters_manager_init(
        &conductor->counters_manager,
        context->counters_metadata_buffer,
        context->counters_metadata_buffer_length,
        context->counters_values_buffer,
        context->counters_values_buffer_length,
        clock_func,
        free_to_reuse_timeout_ms) < 0)
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
        aeron_error_log_resource_linger,
        conductor) < 0)
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

    if (aeron_loss_reporter_init(&conductor->loss_reporter, context->loss_report.addr, context->loss_report.length) < 0)
    {
        return -1;
    }

    conductor->conductor_proxy.command_queue = &context->conductor_command_queue;
    conductor->conductor_proxy.fail_counter = aeron_counter_addr(
        &conductor->counters_manager, AERON_SYSTEM_COUNTER_CONDUCTOR_PROXY_FAILS);
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

    conductor->network_publications.array = NULL;
    conductor->network_publications.length = 0;
    conductor->network_publications.capacity = 0;
    conductor->network_publications.on_time_event = aeron_network_publication_entry_on_time_event;
    conductor->network_publications.has_reached_end_of_life = aeron_network_publication_entry_has_reached_end_of_life;
    conductor->network_publications.delete_func = aeron_network_publication_entry_delete;

    conductor->send_channel_endpoints.array = NULL;
    conductor->send_channel_endpoints.length = 0;
    conductor->send_channel_endpoints.capacity = 0;
    conductor->send_channel_endpoints.on_time_event = aeron_send_channel_endpoint_entry_on_time_event;
    conductor->send_channel_endpoints.has_reached_end_of_life = aeron_send_channel_endpoint_entry_has_reached_end_of_life;
    conductor->send_channel_endpoints.delete_func = aeron_send_channel_endpoint_entry_delete;

    conductor->receive_channel_endpoints.array = NULL;
    conductor->receive_channel_endpoints.length = 0;
    conductor->receive_channel_endpoints.capacity = 0;
    conductor->receive_channel_endpoints.on_time_event = aeron_receive_channel_endpoint_entry_on_time_event;
    conductor->receive_channel_endpoints.has_reached_end_of_life = aeron_receive_channel_endpoint_entry_has_reached_end_of_life;
    conductor->receive_channel_endpoints.delete_func = aeron_receive_channel_endpoint_entry_delete;

    conductor->publication_images.array = NULL;
    conductor->publication_images.length = 0;
    conductor->publication_images.capacity = 0;
    conductor->publication_images.on_time_event = aeron_publication_image_entry_on_time_event;
    conductor->publication_images.has_reached_end_of_life = aeron_publication_image_entry_has_reached_end_of_life;
    conductor->publication_images.delete_func = aeron_publication_image_entry_delete;

    conductor->lingering_resources.array = NULL;
    conductor->lingering_resources.length = 0;
    conductor->lingering_resources.capacity = 0;
    conductor->lingering_resources.on_time_event = aeron_linger_resource_entry_on_time_event;
    conductor->lingering_resources.has_reached_end_of_life = aeron_linger_resource_entry_has_reached_end_of_life;
    conductor->lingering_resources.delete_func = aeron_linger_resource_entry_delete;

    conductor->ipc_subscriptions.array = NULL;
    conductor->ipc_subscriptions.length = 0;
    conductor->ipc_subscriptions.capacity = 0;

    conductor->network_subscriptions.array = NULL;
    conductor->network_subscriptions.length = 0;
    conductor->network_subscriptions.capacity = 0;

    conductor->spy_subscriptions.array = NULL;
    conductor->spy_subscriptions.length = 0;
    conductor->spy_subscriptions.capacity = 0;

    conductor->errors_counter = aeron_counter_addr(&conductor->counters_manager, AERON_SYSTEM_COUNTER_ERRORS);
    conductor->unblocked_commands_counter = aeron_counter_addr(
        &conductor->counters_manager, AERON_SYSTEM_COUNTER_UNBLOCKED_COMMANDS);
    conductor->client_timeouts_counter = aeron_counter_addr(
        &conductor->counters_manager, AERON_SYSTEM_COUNTER_CLIENT_TIMEOUTS);

    int64_t now_ns = context->nano_clock();

    conductor->nano_clock = context->nano_clock;
    conductor->epoch_clock = context->epoch_clock;
    conductor->time_of_last_timeout_check_ns = now_ns;
    conductor->time_of_last_to_driver_position_change_ns = now_ns;
    conductor->next_session_id = aeron_randomised_int32();
    conductor->last_consumer_command_position = aeron_mpsc_rb_consumer_position(&conductor->to_driver_commands);

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

aeron_client_t *aeron_driver_conductor_get_or_add_client(aeron_driver_conductor_t *conductor, int64_t client_id)
{
    aeron_client_t *client = NULL;
    int index;

    if ((index = aeron_driver_conductor_find_client(conductor, client_id)) == -1)
    {
        int ensure_capacity_result = 0;
        AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, conductor->clients, aeron_client_t);

        if (ensure_capacity_result >= 0)
        {
            aeron_counter_t client_heartbeat;

            client_heartbeat.counter_id = aeron_counter_client_heartbeat_status_allocate(
                &conductor->counters_manager, client_id);
            client_heartbeat.value_addr = aeron_counter_addr(
                &conductor->counters_manager, (int32_t)client_heartbeat.counter_id);

            if (client_heartbeat.counter_id >= 0)
            {
                index = (int)conductor->clients.length;
                client = &conductor->clients.array[index];

                client->client_id = client_id;
                client->reached_end_of_life = false;
                client->time_of_last_keepalive_ms = conductor->context->epoch_clock();

                client->heartbeat_status.counter_id = client_heartbeat.counter_id;
                client->heartbeat_status.value_addr = client_heartbeat.value_addr;
                aeron_counter_set_ordered(client->heartbeat_status.value_addr, client->time_of_last_keepalive_ms);

                client->client_liveness_timeout_ms = conductor->context->client_liveness_timeout_ns < 1000000 ?
                    1 : conductor->context->client_liveness_timeout_ns / 1000000;
                client->publication_links.array = NULL;
                client->publication_links.length = 0;
                client->publication_links.capacity = 0;
                client->counter_links.array = NULL;
                client->counter_links.length = 0;
                client->counter_links.capacity = 0;
                conductor->clients.length++;
            }
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
    if (now_ms > (client->time_of_last_keepalive_ms + client->client_liveness_timeout_ms))
    {
        client->reached_end_of_life = true;
        aeron_counter_ordered_increment(conductor->client_timeouts_counter, 1);
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

    for (size_t i = 0; i < client->counter_links.length; i++)
    {
        aeron_counter_link_t *link = &client->counter_links.array[i];
        aeron_counters_manager_free(&conductor->counters_manager, link->counter_id);
    }

    for (int last_index = (int)conductor->ipc_subscriptions.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_subscription_link_t *link = &conductor->ipc_subscriptions.array[i];

        if (client->client_id == link->client_id)
        {
            aeron_driver_conductor_unlink_all_subscribable(conductor, link);

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->ipc_subscriptions.array, sizeof(aeron_subscription_link_t), i, last_index);
            conductor->ipc_subscriptions.length--;
            last_index--;
        }
    }

    for (int last_index = (int)conductor->network_subscriptions.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];

        if (client->client_id == link->client_id)
        {
            aeron_receive_channel_endpoint_t *endpoint = link->endpoint;

            link->endpoint = NULL;
            aeron_receive_channel_endpoint_decref_to_stream(endpoint, link->stream_id);
            if (AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_CLOSING == endpoint->conductor_fields.status)
            {
                aeron_udp_channel_t *udp_channel = endpoint->conductor_fields.udp_channel;

                aeron_str_to_ptr_hash_map_remove(
                    &conductor->receive_channel_endpoint_by_channel_map,
                    udp_channel->canonical_form,
                    udp_channel->canonical_length);
            }

            aeron_driver_conductor_unlink_all_subscribable(conductor, link);

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->network_subscriptions.array, sizeof(aeron_subscription_link_t), i, last_index);
            conductor->network_subscriptions.length--;
            last_index--;
        }
    }

    for (int last_index = (int)conductor->spy_subscriptions.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_subscription_link_t *link = &conductor->spy_subscriptions.array[i];

        if (client->client_id == link->client_id)
        {
            aeron_udp_channel_delete(link->spy_channel);
            link->spy_channel = NULL;
            aeron_driver_conductor_unlink_all_subscribable(conductor, link);

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->spy_subscriptions.array, sizeof(aeron_subscription_link_t), i, last_index);
            conductor->spy_subscriptions.length--;
            last_index--;
        }
    }

    aeron_counters_manager_free(&conductor->counters_manager, (int32_t)client->heartbeat_status.counter_id);

    aeron_free(client->publication_links.array);
    client->publication_links.array = NULL;
    client->publication_links.length = 0;
    client->publication_links.capacity = 0;

    aeron_free(client->counter_links.array);
    client->counter_links.array = NULL;
    client->counter_links.length = 0;
    client->counter_links.capacity = 0;

    client->client_id = -1;
    client->heartbeat_status.counter_id = -1;
    client->heartbeat_status.value_addr = NULL;
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

                for (size_t i = 0, size = conductor->ipc_subscriptions.length; i < size; i++)
                {
                    aeron_subscription_link_t *link = &conductor->ipc_subscriptions.array[i];

                    if (aeron_driver_conductor_is_subscribable_linked(link, &publication->conductor_fields.subscribable))
                    {
                        aeron_driver_conductor_on_unavailable_image(
                            conductor,
                            publication->conductor_fields.managed_resource.registration_id,
                            link->registration_id,
                            publication->stream_id,
                            AERON_IPC_CHANNEL,
                            AERON_IPC_CHANNEL_LEN);
                    }
                }
            }
            else if (aeron_logbuffer_unblocker_unblock(
                publication->mapped_raw_log.term_buffers,
                publication->log_meta_data,
                publication->conductor_fields.consumer_position))
            {
                aeron_counter_ordered_increment(publication->unblocked_publications_counter, 1);
            }
            break;

        case AERON_IPC_PUBLICATION_STATUS_LINGER:
            if (now_ns >
                (publication->conductor_fields.managed_resource.time_of_last_status_change +
                publication->linger_timeout_ns))
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
        aeron_driver_conductor_unlink_subscribable(link, &entry->publication->conductor_fields.subscribable);
    }

    aeron_ipc_publication_close(&conductor->counters_manager, entry->publication);
    entry->publication = NULL;
}

void aeron_network_publication_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_network_publication_entry_t *entry, int64_t now_ns, int64_t now_ms)
{
    aeron_network_publication_on_time_event(conductor, entry->publication, now_ns, now_ms);
}

bool aeron_network_publication_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_network_publication_entry_t *entry)
{
    return aeron_network_publication_has_sender_released(entry->publication);
}

void aeron_network_publication_entry_delete(
    aeron_driver_conductor_t *conductor, aeron_network_publication_entry_t *entry)
{
    aeron_send_channel_endpoint_t *endpoint = entry->publication->endpoint;

    for (size_t i = 0, size = conductor->spy_subscriptions.length; i < size; i++)
    {
        aeron_subscription_link_t *link = &conductor->spy_subscriptions.array[i];
        aeron_driver_conductor_unlink_subscribable(link, &entry->publication->conductor_fields.subscribable);
    }

    aeron_network_publication_close(&conductor->counters_manager, entry->publication);
    entry->publication = NULL;

    endpoint->conductor_fields.managed_resource.decref(endpoint->conductor_fields.managed_resource.clientd);

    if (AERON_SEND_CHANNEL_ENDPOINT_STATUS_CLOSING == endpoint->conductor_fields.status)
    {
        aeron_str_to_ptr_hash_map_remove(
            &conductor->send_channel_endpoint_by_channel_map,
            endpoint->conductor_fields.udp_channel->canonical_form,
            endpoint->conductor_fields.udp_channel->canonical_length);
    }
}

void aeron_driver_conductor_cleanup_spies(aeron_driver_conductor_t *conductor, aeron_network_publication_t *publication)
{
    for (size_t i = 0, size = conductor->spy_subscriptions.length; i < size; i++)
    {
        aeron_subscription_link_t *link = &conductor->spy_subscriptions.array[i];

        if (aeron_driver_conductor_is_subscribable_linked(link, &publication->conductor_fields.subscribable))
        {
            aeron_driver_conductor_on_unavailable_image(
                conductor,
                publication->conductor_fields.managed_resource.registration_id,
                link->registration_id,
                link->stream_id,
                link->channel,
                (size_t)link->channel_length);
        }

        aeron_driver_conductor_unlink_subscribable(link, &publication->conductor_fields.subscribable);
    }
}

void aeron_driver_conductor_cleanup_network_publication(
    aeron_driver_conductor_t *conductor, aeron_network_publication_t *publication)
{
    aeron_driver_sender_proxy_on_remove_publication(conductor->context->sender_proxy, publication);
}

void aeron_send_channel_endpoint_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_send_channel_endpoint_entry_t *entry, int64_t now_ns, int64_t now_ms)
{
    /* nothing done here. Could linger if needed. */
}

bool aeron_send_channel_endpoint_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_send_channel_endpoint_entry_t *entry)
{
    return aeron_send_channel_endpoint_has_sender_released(entry->endpoint);
}

void aeron_send_channel_endpoint_entry_delete(
    aeron_driver_conductor_t *conductor, aeron_send_channel_endpoint_entry_t *entry)
{
    aeron_send_channel_endpoint_delete(&conductor->counters_manager, entry->endpoint);
}

void aeron_receive_channel_endpoint_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_receive_channel_endpoint_entry_t *entry, int64_t now_ns, int64_t now_ms)
{
    /* nothing done here. could linger if needed. */
}

bool aeron_receive_channel_endpoint_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_receive_channel_endpoint_entry_t *entry)
{
    return aeron_receive_channel_endpoint_has_receiver_released(entry->endpoint);
}

void aeron_receive_channel_endpoint_entry_delete(
    aeron_driver_conductor_t *conductor, aeron_receive_channel_endpoint_entry_t *entry)
{
    for (size_t i = 0, size = conductor->publication_images.length; i < size; i++)
    {
        aeron_publication_image_t *image = conductor->publication_images.array[i].image;

        if (entry->endpoint == image->endpoint)
        {
            aeron_publication_image_disconnect_endpoint(image);
        }
    }

    aeron_receive_channel_endpoint_delete(&conductor->counters_manager, entry->endpoint);
}

void aeron_publication_image_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_publication_image_entry_t *entry, int64_t now_ns, int64_t now_ms)
{
    aeron_publication_image_on_time_event(conductor, entry->image, now_ns, now_ms);
}

bool aeron_publication_image_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_publication_image_entry_t *entry)
{
    return AERON_PUBLICATION_IMAGE_STATUS_DONE == entry->image->conductor_fields.status;
}

void aeron_publication_image_entry_delete(
    aeron_driver_conductor_t *conductor, aeron_publication_image_entry_t *entry)
{
    for (size_t i = 0, size = conductor->network_subscriptions.length; i < size; i++)
    {
        aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];
        aeron_driver_conductor_unlink_subscribable(link, &entry->image->conductor_fields.subscribable);
    }

    aeron_publication_image_close(&conductor->counters_manager, entry->image);
    entry->image = NULL;
}

void aeron_linger_resource_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_linger_resource_entry_t *entry, int64_t now_ns, int64_t now_ms)
{
    if (now_ns > entry->timeout)
    {
        entry->has_reached_end_of_life = true;
    }
}

bool aeron_linger_resource_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_linger_resource_entry_t *entry)
{
    return entry->has_reached_end_of_life;
}

void aeron_linger_resource_entry_delete(aeron_driver_conductor_t *conductor, aeron_linger_resource_entry_t *entry)
{
    aeron_free(entry->buffer);
}

void aeron_driver_conductor_image_transition_to_linger(
    aeron_driver_conductor_t *conductor, aeron_publication_image_t *image)
{
    if (NULL != image->endpoint)
    {
        for (size_t i = 0, size = conductor->network_subscriptions.length; i < size; i++)
        {
            aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];

            if (aeron_driver_conductor_is_subscribable_linked(link, &image->conductor_fields.subscribable))
            {
                aeron_driver_conductor_on_unavailable_image(
                    conductor,
                    image->conductor_fields.managed_resource.registration_id,
                    link->registration_id,
                    image->stream_id,
                    link->channel,
                    (size_t)link->channel_length);
            }
        }

        aeron_driver_receiver_proxy_on_remove_cool_down(
            conductor->context->receiver_proxy, image->endpoint, image->session_id, image->stream_id);
    }
}

#define AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(c, l, t, now_ns, now_ms) \
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
    AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(
        conductor, conductor->network_publications, aeron_network_publication_entry_t, now_ns, now_ms);
    AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(
        conductor, conductor->send_channel_endpoints, aeron_send_channel_endpoint_entry_t, now_ns, now_ms);
    AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(
        conductor, conductor->receive_channel_endpoints, aeron_receive_channel_endpoint_entry_t, now_ns, now_ms);
    AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(
        conductor, conductor->publication_images, aeron_publication_image_entry_t, now_ns, now_ms);
    AERON_DRIVER_CONDUCTOR_CHECK_MANAGED_RESOURCE(
        conductor, conductor->lingering_resources, aeron_linger_resource_entry_t, now_ns, now_ms);
}

aeron_ipc_publication_t *aeron_driver_conductor_get_or_add_ipc_publication(
    aeron_driver_conductor_t *conductor,
    aeron_client_t *client,
    aeron_uri_publication_params_t *params,
    int64_t registration_id,
    int32_t stream_id,
    int32_t channel_length,
    const char *channel,
    bool is_exclusive)
{
    aeron_ipc_publication_t *publication = NULL;

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

    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, client->publication_links, aeron_publication_link_t);

    if (ensure_capacity_result >= 0)
    {
        if (NULL == publication)
        {
            AERON_ARRAY_ENSURE_CAPACITY(
                ensure_capacity_result, conductor->ipc_publications, aeron_ipc_publication_entry_t);

            if (ensure_capacity_result >= 0)
            {
                int32_t session_id = conductor->next_session_id++;
                int32_t initial_term_id = aeron_randomised_int32();
                aeron_position_t pub_lmt_position;
                aeron_position_t pub_pos_position;

                pub_lmt_position.counter_id = aeron_counter_publisher_limit_allocate(
                    &conductor->counters_manager, registration_id, session_id, stream_id, channel_length, channel);
                pub_lmt_position.value_addr = aeron_counter_addr(
                    &conductor->counters_manager, (int32_t)pub_lmt_position.counter_id);

                pub_pos_position.counter_id = aeron_counter_publisher_position_allocate(
                    &conductor->counters_manager, registration_id, session_id, stream_id, channel_length, channel);
                pub_pos_position.value_addr = aeron_counter_addr(
                    &conductor->counters_manager, (int32_t)pub_pos_position.counter_id);

                if (pub_lmt_position.counter_id >= 0 &&
                    pub_pos_position.counter_id >= 0 &&
                    aeron_ipc_publication_create(
                        &publication,
                        conductor->context,
                        session_id,
                        stream_id,
                        registration_id,
                        &pub_lmt_position,
                        &pub_pos_position,
                        initial_term_id,
                        params->term_length,
                        params->mtu_length,
                        params->is_sparse,
                        is_exclusive,
                        &conductor->system_counters) >= 0)
                {
                    aeron_publication_link_t *link = &client->publication_links.array[client->publication_links.length];

                    link->resource = &publication->conductor_fields.managed_resource;
                    link->registration_id = registration_id;
                    client->publication_links.length++;

                    conductor->ipc_publications.array[conductor->ipc_publications.length++].publication = publication;
                    publication->conductor_fields.managed_resource.time_of_last_status_change = conductor->nano_clock();
                }
            }
        }
        else
        {
            aeron_publication_link_t *link = &client->publication_links.array[client->publication_links.length];

            link->resource = &publication->conductor_fields.managed_resource;
            link->registration_id = registration_id;
            client->publication_links.length++;

            publication->conductor_fields.managed_resource.incref(
                publication->conductor_fields.managed_resource.clientd);
        }
    }

    return ensure_capacity_result >= 0 ? publication : NULL;
}

aeron_network_publication_t *aeron_driver_conductor_get_or_add_network_publication(
    aeron_driver_conductor_t *conductor,
    aeron_client_t *client,
    aeron_send_channel_endpoint_t *endpoint,
    int32_t uri_length,
    const char *uri,
    aeron_uri_publication_params_t *params,
    int64_t registration_id,
    int32_t stream_id,
    bool is_exclusive)
{
    aeron_network_publication_t *publication = NULL;
    aeron_udp_channel_t *udp_channel = endpoint->conductor_fields.udp_channel;

    if (!is_exclusive)
    {
        for (size_t i = 0; i < conductor->network_publications.length; i++)
        {
            aeron_network_publication_t *pub_entry = conductor->network_publications.array[i].publication;

            if (endpoint == pub_entry->endpoint &&
                stream_id == pub_entry->stream_id &&
                !pub_entry->is_exclusive &&
                pub_entry->conductor_fields.status == AERON_NETWORK_PUBLICATION_STATUS_ACTIVE)
            {
                publication = pub_entry;
                break;
            }
        }
    }

    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, client->publication_links, aeron_publication_link_t);

    if (ensure_capacity_result >= 0)
    {
        if (NULL == publication)
        {
            AERON_ARRAY_ENSURE_CAPACITY(
                ensure_capacity_result, conductor->network_publications, aeron_network_publication_entry_t);

            if (ensure_capacity_result >= 0)
            {
                int32_t session_id = conductor->next_session_id++;
                int32_t initial_term_id = aeron_randomised_int32();
                aeron_position_t pub_lmt_position;
                aeron_position_t pub_pos_position;
                aeron_position_t snd_pos_position;
                aeron_position_t snd_lmt_position;

                pub_lmt_position.counter_id = aeron_counter_publisher_limit_allocate(
                    &conductor->counters_manager, registration_id, session_id, stream_id, uri_length, uri);
                pub_pos_position.counter_id = aeron_counter_publisher_position_allocate(
                    &conductor->counters_manager, registration_id, session_id, stream_id, uri_length, uri);
                snd_pos_position.counter_id = aeron_counter_sender_position_allocate(
                    &conductor->counters_manager, registration_id, session_id, stream_id, uri_length, uri);
                snd_lmt_position.counter_id = aeron_counter_sender_limit_allocate(
                    &conductor->counters_manager, registration_id, session_id, stream_id, uri_length, uri);

                if (pub_lmt_position.counter_id < 0 ||
                    pub_pos_position.counter_id < 0 ||
                    snd_pos_position.counter_id < 0 ||
                    snd_lmt_position.counter_id < 0)
                {
                    return NULL;
                }

                pub_lmt_position.value_addr = aeron_counter_addr(
                    &conductor->counters_manager, (int32_t)pub_lmt_position.counter_id);
                pub_pos_position.value_addr = aeron_counter_addr(
                    &conductor->counters_manager, (int32_t)pub_pos_position.counter_id);
                snd_pos_position.value_addr = aeron_counter_addr(
                    &conductor->counters_manager, (int32_t)snd_pos_position.counter_id);
                snd_lmt_position.value_addr = aeron_counter_addr(
                    &conductor->counters_manager, (int32_t)snd_lmt_position.counter_id);

                aeron_flow_control_strategy_supplier_func_t flow_control_strategy_supplier_func =
                    udp_channel->explicit_control || udp_channel->multicast ?
                        conductor->context->multicast_flow_control_supplier_func :
                        conductor->context->unicast_flow_control_supplier_func;
                aeron_flow_control_strategy_t *flow_control_strategy;

                if (flow_control_strategy_supplier_func(
                    &flow_control_strategy,
                    uri_length,
                    uri,
                    stream_id,
                    registration_id,
                    initial_term_id,
                    params->term_length) < 0)
                {
                    return NULL;
                }

                if (pub_lmt_position.counter_id >= 0 &&
                    aeron_network_publication_create(
                        &publication,
                        endpoint,
                        conductor->context,
                        registration_id,
                        session_id,
                        stream_id,
                        initial_term_id,
                        params->mtu_length,
                        &pub_lmt_position,
                        &pub_pos_position,
                        &snd_pos_position,
                        &snd_lmt_position,
                        flow_control_strategy,
                        params->linger_timeout_ns,
                        params->term_length,
                        params->is_sparse,
                        is_exclusive,
                        conductor->context->spies_simulate_connection,
                        &conductor->system_counters) >= 0)
                {
                    endpoint->conductor_fields.managed_resource.incref(endpoint->conductor_fields.managed_resource.clientd);
                    aeron_driver_sender_proxy_on_add_publication(conductor->context->sender_proxy, publication);

                    aeron_publication_link_t *link = &client->publication_links.array[client->publication_links.length];

                    link->resource = &publication->conductor_fields.managed_resource;
                    link->registration_id = registration_id;
                    client->publication_links.length++;

                    conductor->network_publications.array[conductor->network_publications.length++].publication = publication;
                    publication->conductor_fields.managed_resource.time_of_last_status_change = conductor->nano_clock();
                }
            }
        }
        else
        {
            aeron_publication_link_t *link = &client->publication_links.array[client->publication_links.length];

            link->resource = &publication->conductor_fields.managed_resource;
            link->registration_id = registration_id;
            client->publication_links.length++;

            publication->conductor_fields.managed_resource.incref(publication->conductor_fields.managed_resource.clientd);
        }
    }

    return ensure_capacity_result >= 0 ? publication : NULL;
}

aeron_send_channel_endpoint_t *aeron_driver_conductor_get_or_add_send_channel_endpoint(
    aeron_driver_conductor_t *conductor, aeron_udp_channel_t *channel)
{
    aeron_send_channel_endpoint_t *endpoint = aeron_str_to_ptr_hash_map_get(
        &conductor->send_channel_endpoint_by_channel_map, channel->canonical_form, channel->canonical_length);

    if (NULL == endpoint)
    {
        aeron_counter_t status_indicator;
        int ensure_capacity_result = 0;

        AERON_ARRAY_ENSURE_CAPACITY(
            ensure_capacity_result, conductor->send_channel_endpoints, aeron_send_channel_endpoint_entry_t);

        if (ensure_capacity_result < 0)
        {
            return NULL;
        }

        status_indicator.counter_id = aeron_counter_send_channel_status_allocate(
            &conductor->counters_manager, (int32_t)channel->uri_length, channel->original_uri);

        status_indicator.value_addr = aeron_counter_addr(
            &conductor->counters_manager, (int32_t)status_indicator.counter_id);

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

        aeron_driver_sender_proxy_on_add_endpoint(conductor->context->sender_proxy, endpoint);
        conductor->send_channel_endpoints.array[conductor->send_channel_endpoints.length++].endpoint = endpoint;
        *status_indicator.value_addr = AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ACTIVE;
    }

    return endpoint;
}

aeron_receive_channel_endpoint_t *aeron_driver_conductor_get_or_add_receive_channel_endpoint(
    aeron_driver_conductor_t *conductor, aeron_udp_channel_t *channel)
{
    aeron_receive_channel_endpoint_t *endpoint = aeron_str_to_ptr_hash_map_get(
        &conductor->receive_channel_endpoint_by_channel_map, channel->canonical_form, channel->canonical_length);

    if (NULL == endpoint)
    {
        aeron_counter_t status_indicator;
        int ensure_capacity_result = 0;

        AERON_ARRAY_ENSURE_CAPACITY(
            ensure_capacity_result, conductor->receive_channel_endpoints, aeron_receive_channel_endpoint_entry_t);

        if (ensure_capacity_result < 0)
        {
            return NULL;
        }

        status_indicator.counter_id = aeron_counter_receive_channel_status_allocate(
            &conductor->counters_manager, (int32_t)channel->uri_length, channel->original_uri);

        status_indicator.value_addr = aeron_counter_addr(
            &conductor->counters_manager, (int32_t)status_indicator.counter_id);

        if (status_indicator.counter_id < 0 ||
            aeron_receive_channel_endpoint_create(
                &endpoint,
                channel,
                &status_indicator,
                &conductor->system_counters,
                conductor->context) < 0)
        {
            return NULL;
        }

        if (aeron_str_to_ptr_hash_map_put(
            &conductor->receive_channel_endpoint_by_channel_map,
            channel->canonical_form,
            channel->canonical_length,
            endpoint) < 0)
        {
            aeron_receive_channel_endpoint_delete(&conductor->counters_manager, endpoint);
            return NULL;
        }

        conductor->receive_channel_endpoints.array[conductor->receive_channel_endpoints.length++].endpoint = endpoint;
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
    conductor->context->to_client_interceptor_func(conductor, msg_type_id, msg, length);
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
    int64_t original_registration_id,
    int32_t stream_id,
    int32_t session_id,
    int32_t position_limit_counter_id,
    int32_t channel_status_indicator_id,
    bool is_exclusive,
    const char *log_file_name,
    size_t log_file_name_length)
{
    char response_buffer[sizeof(aeron_publication_buffers_ready_t) + AERON_MAX_PATH];
    aeron_publication_buffers_ready_t *response = (aeron_publication_buffers_ready_t *)response_buffer;

    response->correlation_id = registration_id;
    response->registration_id = original_registration_id;
    response->stream_id = stream_id;
    response->session_id = session_id;
    response->position_limit_counter_id = position_limit_counter_id;
    response->channel_status_indicator_id = channel_status_indicator_id;
    response->log_file_length = (int32_t)log_file_name_length;
    memcpy(response_buffer + sizeof(aeron_publication_buffers_ready_t), log_file_name, log_file_name_length);

    aeron_driver_conductor_client_transmit(
        conductor,
        is_exclusive ? AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY : AERON_RESPONSE_ON_PUBLICATION_READY,
        response,
        sizeof(aeron_publication_buffers_ready_t) + log_file_name_length);
}

void aeron_driver_conductor_on_subscription_ready(
    aeron_driver_conductor_t *conductor,
    int64_t registration_id,
    int32_t channel_status_indicator_id)
{
    char response_buffer[sizeof(aeron_correlated_command_t)];
    aeron_subscription_ready_t *response = (aeron_subscription_ready_t *)response_buffer;

    response->correlation_id = registration_id;
    response->channel_status_indicator_id = channel_status_indicator_id;

    aeron_driver_conductor_client_transmit(
        conductor, AERON_RESPONSE_ON_SUBSCRIPTION_READY, response, sizeof(aeron_subscription_ready_t));
}

void aeron_driver_conductor_on_counter_ready(
    aeron_driver_conductor_t *conductor,
    int64_t registration_id,
    int32_t counter_id)
{
    char response_buffer[sizeof(aeron_counter_update_t)];
    aeron_counter_update_t *response = (aeron_counter_update_t *)response_buffer;

    response->correlation_id = registration_id;
    response->counter_id = counter_id;

    aeron_driver_conductor_client_transmit(
        conductor, AERON_RESPONSE_ON_COUNTER_READY, response, sizeof(aeron_counter_update_t));
}

void aeron_driver_conductor_on_unavailable_counter(
    aeron_driver_conductor_t *conductor,
    int64_t registration_id,
    int32_t counter_id)
{
    char response_buffer[sizeof(aeron_counter_update_t)];
    aeron_counter_update_t *response = (aeron_counter_update_t *)response_buffer;

    response->correlation_id = registration_id;
    response->counter_id = counter_id;

    aeron_driver_conductor_client_transmit(
        conductor, AERON_RESPONSE_ON_UNAVAILABLE_COUNTER, response, sizeof(aeron_counter_update_t));
}

void aeron_driver_conductor_on_operation_succeeded(
    aeron_driver_conductor_t *conductor,
    int64_t correlation_id)
{
    char response_buffer[sizeof(aeron_correlated_command_t)];
    aeron_operation_succeeded_t *response = (aeron_operation_succeeded_t *)response_buffer;

    response->correlation_id = correlation_id;

    aeron_driver_conductor_client_transmit(
        conductor, AERON_RESPONSE_ON_OPERATION_SUCCESS, response, sizeof(aeron_operation_succeeded_t));
}

void aeron_driver_conductor_on_available_image(
    aeron_driver_conductor_t *conductor,
    int64_t correlation_id,
    int32_t stream_id,
    int32_t session_id,
    const char *log_file_name,
    size_t log_file_name_length,
    int32_t subscriber_position_id,
    int64_t subscriber_registration_id,
    const char *source_identity,
    size_t source_identity_length)
{
    char response_buffer[sizeof(aeron_image_buffers_ready_t) + (2 * AERON_MAX_PATH)];
    char *ptr = response_buffer;
    aeron_image_buffers_ready_t *response;
    size_t response_length =
        sizeof(aeron_image_buffers_ready_t) +
        AERON_ALIGN(log_file_name_length, sizeof(int32_t)) +
        source_identity_length +
        (2 * sizeof(int32_t));

    response = (aeron_image_buffers_ready_t *)ptr;

    response->correlation_id = correlation_id;
    response->stream_id = stream_id;
    response->session_id = session_id;
    response->subscriber_position_id = subscriber_position_id;
    response->subscriber_registration_id = subscriber_registration_id;
    ptr += sizeof(aeron_image_buffers_ready_t);

    int32_t length_field;

    length_field = (int32_t)log_file_name_length;
    memcpy(ptr, &length_field, sizeof(length_field));
    ptr += sizeof(int32_t);
    memcpy(ptr, log_file_name, log_file_name_length);
    ptr += AERON_ALIGN(log_file_name_length, sizeof(int32_t));

    length_field = (int32_t)source_identity_length;
    memcpy(ptr, &length_field, sizeof(length_field));
    ptr += sizeof(int32_t);
    memcpy(ptr, source_identity, source_identity_length);
    /* ptr += source_identity_length; */

    aeron_driver_conductor_client_transmit(
        conductor, AERON_RESPONSE_ON_AVAILABLE_IMAGE, response, response_length);
}

void aeron_driver_conductor_on_unavailable_image(
    aeron_driver_conductor_t *conductor,
    int64_t correlation_id,
    int64_t subscription_registration_id,
    int32_t stream_id,
    const char *channel,
    size_t channel_length)
{
    char response_buffer[sizeof(aeron_image_message_t) + AERON_MAX_PATH];
    aeron_image_message_t *response = (aeron_image_message_t *)response_buffer;

    response->correlation_id = correlation_id;
    response->subscription_registration_id = subscription_registration_id;
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
    aeron_set_err(0, "%s", "no error");
}

void aeron_driver_conductor_on_command(int32_t msg_type_id, const void *message, size_t length, void *clientd)
{
    aeron_driver_conductor_t *conductor = (aeron_driver_conductor_t *)clientd;
    int64_t correlation_id = 0;
    int result = 0;

    conductor->context->to_driver_interceptor_func(msg_type_id, message, length, clientd);

    char error_message[AERON_MAX_PATH] = "\0";

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
            const char *channel = (const char *)message + sizeof(aeron_publication_command_t);

            if (strncmp(channel, AERON_IPC_CHANNEL, AERON_IPC_CHANNEL_LEN) == 0)
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
            const char *channel = (const char *)message + sizeof(aeron_publication_command_t);

            if (strncmp(channel, AERON_IPC_CHANNEL, AERON_IPC_CHANNEL_LEN) == 0)
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
            const char *channel = (const char *)message + sizeof(aeron_subscription_command_t);

            if (strncmp(channel, AERON_IPC_CHANNEL, AERON_IPC_CHANNEL_LEN) == 0)
            {
                result = aeron_driver_conductor_on_add_ipc_subscription(conductor, command);
            }
            else if (strncmp(channel, AERON_SPY_PREFIX, AERON_IPC_CHANNEL_LEN) == 0)
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

        case AERON_COMMAND_ADD_DESTINATION:
        {
            aeron_destination_command_t *command = (aeron_destination_command_t *)message;

            if (length < sizeof(aeron_destination_command_t) ||
                length < (sizeof(aeron_destination_command_t) + command->channel_length))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;

            result = aeron_driver_conductor_on_add_destination(conductor, command);
            break;
        }

        case AERON_COMMAND_REMOVE_DESTINATION:
        {
            aeron_destination_command_t *command = (aeron_destination_command_t *)message;

            if (length < sizeof(aeron_destination_command_t) ||
                length < (sizeof(aeron_destination_command_t) + command->channel_length))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;

            result = aeron_driver_conductor_on_remove_destination(conductor, command);
            break;
        }

        case AERON_COMMAND_ADD_COUNTER:
        {
            aeron_counter_command_t *command = (aeron_counter_command_t *)message;

            if (length < sizeof(aeron_counter_command_t))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;

            result = aeron_driver_conductor_on_add_counter(conductor, command);
            break;
        }

        case AERON_COMMAND_REMOVE_COUNTER:
        {
            aeron_remove_command_t *command = (aeron_remove_command_t *)message;

            if (length < sizeof(aeron_remove_command_t))
            {
                goto malformed_command;
            }

            correlation_id = command->correlated.correlation_id;

            result = aeron_driver_conductor_on_remove_counter(conductor, command);
            break;
        }

        case AERON_COMMAND_CLIENT_CLOSE:
        {
            aeron_correlated_command_t *command = (aeron_correlated_command_t *)message;

            if (length < sizeof(aeron_correlated_command_t))
            {
                goto malformed_command;
            }

            result = aeron_driver_conductor_on_client_close(conductor, command);
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
        char *error_description = strerror(os_errno);

        AERON_FORMAT_BUFFER(error_message, "(%d) %s: %s", os_errno, error_description, aeron_errmsg());
        aeron_driver_conductor_on_error(conductor, code, error_message, strlen(error_message), correlation_id);
        aeron_driver_conductor_error(conductor, code, error_description, error_message);
    }

    return;

    malformed_command:
    AERON_FORMAT_BUFFER(error_message, "command=%d too short: length=%lu", msg_type_id, length);
    aeron_driver_conductor_error(conductor, AERON_ERROR_CODE_MALFORMED_COMMAND, "command too short", error_message);
}

void aeron_driver_conductor_on_command_queue(void *clientd, volatile void *item)
{
    aeron_command_base_t *cmd = (aeron_command_base_t *)item;
    cmd->func(clientd, cmd);
}

void aeron_driver_conductor_on_check_for_blocked_driver_commands(aeron_driver_conductor_t *conductor, int64_t now_ns)
{
    int64_t consumer_position = aeron_mpsc_rb_consumer_position(&conductor->to_driver_commands);

    if (consumer_position == conductor->last_consumer_command_position)
    {
        if (aeron_mpsc_rb_producer_position(&conductor->to_driver_commands) > consumer_position &&
            now_ns > (conductor->time_of_last_to_driver_position_change_ns +
                (int64_t)conductor->context->client_liveness_timeout_ns))
        {
            if (aeron_mpsc_rb_unblock(&conductor->to_driver_commands))
            {
                aeron_counter_ordered_increment(conductor->unblocked_commands_counter, 1);
            }
        }
    }
    else
    {
        conductor->time_of_last_to_driver_position_change_ns = now_ns;
        conductor->last_consumer_command_position = consumer_position;
    }
}

int aeron_driver_conductor_do_work(void *clientd)
{
    aeron_driver_conductor_t *conductor = (aeron_driver_conductor_t *)clientd;
    int work_count = 0;
    int64_t now_ns = conductor->nano_clock();

    work_count += (int)aeron_mpsc_rb_read(
        &conductor->to_driver_commands, aeron_driver_conductor_on_command, conductor, 10);
    work_count += aeron_mpsc_concurrent_array_queue_drain(
        conductor->conductor_proxy.command_queue, aeron_driver_conductor_on_command_queue, conductor, 10);

    if (now_ns > (conductor->time_of_last_timeout_check_ns + (int64_t)conductor->context->timer_interval_ns))
    {
        int64_t now_ms = conductor->epoch_clock();

        aeron_mpsc_rb_consumer_heartbeat_time(&conductor->to_driver_commands, now_ms);
        aeron_driver_conductor_on_check_managed_resources(conductor, now_ns, now_ms);
        aeron_driver_conductor_on_check_for_blocked_driver_commands(conductor, now_ns);
        conductor->time_of_last_timeout_check_ns = now_ns;
        work_count++;
    }

    for (size_t i = 0, length = conductor->ipc_publications.length; i < length; i++)
    {
        work_count += aeron_ipc_publication_update_pub_lmt(conductor->ipc_publications.array[i].publication);
    }

    for (size_t i = 0, length = conductor->network_publications.length; i < length; i++)
    {
        work_count += aeron_network_publication_update_pub_lmt(conductor->network_publications.array[i].publication);
    }

    for (size_t i = 0, length = conductor->publication_images.length; i < length; i++)
    {
        aeron_publication_image_track_rebuild(
            conductor->publication_images.array[i].image, now_ns, conductor->context->status_message_timeout_ns);
    }

    return work_count;
}

void aeron_driver_conductor_on_close(void *clientd)
{
    aeron_driver_conductor_t *conductor = (aeron_driver_conductor_t *)clientd;

    for (size_t i = 0, length = conductor->clients.length; i < length; i++)
    {
        aeron_free(conductor->clients.array[i].publication_links.array);
        aeron_free(conductor->clients.array[i].counter_links.array);
    }
    aeron_free(conductor->clients.array);

    for (size_t i = 0, length = conductor->ipc_publications.length; i < length; i++)
    {
        aeron_ipc_publication_close(&conductor->counters_manager, conductor->ipc_publications.array[i].publication);
    }
    aeron_free(conductor->ipc_publications.array);

    for (size_t i = 0, length = conductor->network_publications.length; i < length; i++)
    {
        aeron_network_publication_close(
            &conductor->counters_manager, conductor->network_publications.array[i].publication);
    }
    aeron_free(conductor->network_publications.array);

    for (size_t i = 0, length = conductor->ipc_subscriptions.length; i < length; i++)
    {
        aeron_free(conductor->ipc_subscriptions.array[i].subscribable_list.array);
    }
    aeron_free(conductor->ipc_subscriptions.array);

    for (size_t i = 0, length = conductor->network_subscriptions.length; i < length; i++)
    {
        aeron_free(conductor->network_subscriptions.array[i].subscribable_list.array);
    }
    aeron_free(conductor->network_subscriptions.array);

    for (size_t i = 0, length = conductor->spy_subscriptions.length; i < length; i++)
    {
        aeron_udp_channel_delete(conductor->spy_subscriptions.array[i].spy_channel);
        aeron_free(conductor->spy_subscriptions.array[i].subscribable_list.array);
    }
    aeron_free(conductor->spy_subscriptions.array);

    for (size_t i = 0, length = conductor->send_channel_endpoints.length; i < length; i++)
    {
        aeron_send_channel_endpoint_delete(
            &conductor->counters_manager, conductor->send_channel_endpoints.array[i].endpoint);
    }
    aeron_free(conductor->send_channel_endpoints.array);

    for (size_t i = 0, length = conductor->receive_channel_endpoints.length; i < length; i++)
    {
        aeron_receive_channel_endpoint_delete(
            &conductor->counters_manager, conductor->receive_channel_endpoints.array[i].endpoint);
    }
    aeron_free(conductor->receive_channel_endpoints.array);

    for (size_t i = 0, length = conductor->publication_images.length; i < length; i++)
    {
        aeron_publication_image_close(&conductor->counters_manager, conductor->publication_images.array[i].image);
    }
    aeron_free(conductor->publication_images.array);

    aeron_system_counters_close(&conductor->system_counters);
    aeron_counters_manager_close(&conductor->counters_manager);
    aeron_distinct_error_log_close(&conductor->error_log);

    aeron_str_to_ptr_hash_map_delete(&conductor->send_channel_endpoint_by_channel_map);
    aeron_str_to_ptr_hash_map_delete(&conductor->receive_channel_endpoint_by_channel_map);
}

int aeron_driver_subscribable_add_position(
    aeron_subscribable_t *subscribable, int64_t counter_id, int64_t *value_addr)
{
    int ensure_capacity_result = 0, result = -1;

    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, (*subscribable), aeron_position_t);

    if (ensure_capacity_result >= 0)
    {
        aeron_position_t *entry = &subscribable->array[subscribable->length];
        entry->counter_id = counter_id;
        entry->value_addr = value_addr;
        subscribable->add_position_hook_func(subscribable->clientd, value_addr);
        subscribable->length++;
        result = 0;
    }

    return result;
}

void aeron_driver_subscribable_remove_position(aeron_subscribable_t *subscribable, int64_t counter_id)
{
    for (size_t i = 0, size = subscribable->length, last_index = size - 1; i < size; i++)
    {
        if (counter_id == subscribable->array[i].counter_id)
        {
            subscribable->remove_position_hook_func(subscribable->clientd, subscribable->array[i].value_addr);
            aeron_array_fast_unordered_remove((uint8_t *)subscribable->array, sizeof(aeron_position_t), i, last_index);
            subscribable->length--;
            break;
        }
    }
}

int aeron_driver_conductor_link_subscribable(
    aeron_driver_conductor_t *conductor,
    aeron_subscription_link_t *link,
    aeron_subscribable_t *subscribable,
    int64_t original_registration_id,
    int32_t session_id,
    int32_t stream_id,
    int64_t join_position,
    int32_t uri_length,
    const char *original_uri,
    const char *source_identity,
    const char *log_file_name,
    size_t log_file_name_length)
{
    int ensure_capacity_result = 0, result = -1;

    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, link->subscribable_list, aeron_subscribable_list_entry_t);

    if (ensure_capacity_result >= 0)
    {
        int64_t joining_position = join_position;
        int32_t counter_id = aeron_counter_subscription_position_allocate(
            &conductor->counters_manager,
            link->registration_id,
            session_id,
            stream_id,
            uri_length,
            original_uri,
            joining_position);

        if (counter_id >= 0)
        {
            int64_t *position_addr = aeron_counter_addr(&conductor->counters_manager, counter_id);

            if (aeron_driver_subscribable_add_position(subscribable, counter_id, position_addr) >= 0)
            {
                aeron_subscribable_list_entry_t *entry =
                    &link->subscribable_list.array[link->subscribable_list.length++];

                aeron_counter_set_ordered(position_addr, joining_position);

                entry->subscribable = subscribable;
                entry->counter_id = counter_id;

                aeron_driver_conductor_on_available_image(
                    conductor,
                    original_registration_id,
                    stream_id,
                    session_id,
                    log_file_name,
                    log_file_name_length,
                    counter_id,
                    link->registration_id,
                    source_identity,
                    strlen(source_identity));

                result = 0;
            }
        }
    }

    return result;
}

void aeron_driver_conductor_unlink_subscribable(aeron_subscription_link_t *link, aeron_subscribable_t *subscribable)
{
    for (int last_index = link->subscribable_list.length - 1, i = last_index; i >= 0; i--)
    {
        if (subscribable == link->subscribable_list.array[i].subscribable)
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *)link->subscribable_list.array, sizeof(aeron_subscribable_list_entry_t), i, last_index);
            link->subscribable_list.length--;
            last_index--;
        }
    }
}

void aeron_driver_conductor_unlink_all_subscribable(
    aeron_driver_conductor_t *conductor, aeron_subscription_link_t *link)
{
    for (size_t i = 0; i < link->subscribable_list.length; i++)
    {
        aeron_subscribable_list_entry_t *entry = &link->subscribable_list.array[i];

        aeron_driver_subscribable_remove_position(entry->subscribable, entry->counter_id);
        aeron_counters_manager_free(&conductor->counters_manager, (int32_t)entry->counter_id);
    }

    aeron_free(link->subscribable_list.array);
    link->subscribable_list.array = NULL;
    link->subscribable_list.length = 0;
    link->subscribable_list.capacity = 0;
}

int aeron_driver_conductor_on_add_ipc_publication(
    aeron_driver_conductor_t *conductor,
    aeron_publication_command_t *command,
    bool is_exclusive)
{
    int64_t correlation_id = command->correlated.correlation_id;
    aeron_client_t *client = NULL;
    aeron_ipc_publication_t *publication = NULL;
    const char *uri = (const char *)command + sizeof(aeron_publication_command_t);
    int32_t uri_length = command->channel_length;
    aeron_uri_t aeron_uri_params;
    aeron_uri_publication_params_t params;

    if (aeron_uri_parse(uri, &aeron_uri_params) < 0 ||
        aeron_uri_publication_params(&aeron_uri_params, &params, conductor->context, is_exclusive) < 0)
    {
        goto error_cleanup;
    }

    if ((client = aeron_driver_conductor_get_or_add_client(conductor, command->correlated.client_id)) == NULL ||
        (publication = aeron_driver_conductor_get_or_add_ipc_publication(
            conductor, client, &params, correlation_id, command->stream_id, uri_length, uri, is_exclusive)) == NULL)
    {
        goto error_cleanup;
    }

    aeron_subscribable_t *subscribable = &publication->conductor_fields.subscribable;

    aeron_driver_conductor_on_publication_ready(
        conductor,
        command->correlated.correlation_id,
        publication->conductor_fields.managed_resource.registration_id,
        publication->stream_id,
        publication->session_id,
        (int32_t)publication->pub_lmt_position.counter_id,
        AERON_CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED,
        is_exclusive,
        publication->log_file_name,
        publication->log_file_name_length);

    for (size_t i = 0; i < conductor->ipc_subscriptions.length; i++)
    {
        aeron_subscription_link_t *subscription_link = &conductor->ipc_subscriptions.array[i];

        if (command->stream_id == subscription_link->stream_id &&
            !aeron_driver_conductor_is_subscribable_linked(subscription_link, subscribable))
        {
            if (aeron_driver_conductor_link_subscribable(
                conductor,
                subscription_link,
                subscribable,
                publication->conductor_fields.managed_resource.registration_id,
                publication->session_id,
                publication->stream_id,
                aeron_ipc_publication_joining_position(publication),
                subscription_link->channel_length,
                subscription_link->channel,
                AERON_IPC_CHANNEL,
                publication->log_file_name,
                publication->log_file_name_length) < 0)
            {
                goto error_cleanup;
            }
        }
    }

    aeron_uri_close(&aeron_uri_params);
    return 0;

    error_cleanup:
    aeron_uri_close(&aeron_uri_params);
    return -1;
}

int aeron_driver_conductor_on_add_network_publication(
    aeron_driver_conductor_t *conductor,
    aeron_publication_command_t *command,
    bool is_exclusive)
{
    int64_t correlation_id = command->correlated.correlation_id;
    aeron_client_t *client = NULL;
    aeron_udp_channel_t *udp_channel = NULL;
    aeron_send_channel_endpoint_t *endpoint = NULL;
    aeron_network_publication_t *publication = NULL;
    const char *uri = (const char *)command + sizeof(aeron_publication_command_t);
    int32_t uri_length = command->channel_length;
    aeron_uri_publication_params_t params;

    if (aeron_udp_channel_parse(uri, (size_t)uri_length, &udp_channel) < 0 ||
        aeron_uri_publication_params(&udp_channel->uri, &params, conductor->context, is_exclusive) < 0)
    {
        return -1;
    }

    if ((client = aeron_driver_conductor_get_or_add_client(conductor, command->correlated.client_id)) == NULL)
    {
        return -1;
    }

    if ((endpoint = aeron_driver_conductor_get_or_add_send_channel_endpoint(conductor, udp_channel)) == NULL)
    {
        return -1;
    }

    if (AERON_SEND_CHANNEL_ENDPOINT_STATUS_CLOSING == endpoint->conductor_fields.status)
    {
        aeron_set_err(EINVAL, "%s", "send_channel_endpoint found in CLOSING state");
        return -1;
    }

    if ((publication = aeron_driver_conductor_get_or_add_network_publication(
        conductor, client, endpoint, uri_length, uri, &params, correlation_id, command->stream_id, is_exclusive)) == NULL)
    {
        return -1;
    }

    aeron_subscribable_t *subscribable = &publication->conductor_fields.subscribable;

    aeron_driver_conductor_on_publication_ready(
        conductor,
        correlation_id,
        publication->conductor_fields.managed_resource.registration_id,
        publication->stream_id,
        publication->session_id,
        (int32_t)publication->pub_lmt_position.counter_id,
        (int32_t)endpoint->channel_status.counter_id,
        is_exclusive,
        publication->log_file_name,
        publication->log_file_name_length);

    for (size_t i = 0; i < conductor->spy_subscriptions.length; i++)
    {
        aeron_subscription_link_t *subscription_link = &conductor->spy_subscriptions.array[i];

        if (command->stream_id == subscription_link->stream_id &&
            0 == strncmp(
                subscription_link->spy_channel->canonical_form,
                udp_channel->canonical_form,
                subscription_link->spy_channel->canonical_length) &&
            !aeron_driver_conductor_is_subscribable_linked(subscription_link, subscribable))
        {
            if (aeron_driver_conductor_link_subscribable(
                conductor,
                subscription_link,
                &publication->conductor_fields.subscribable,
                publication->conductor_fields.managed_resource.registration_id,
                publication->session_id,
                publication->stream_id,
                aeron_network_publication_consumer_position(publication),
                subscription_link->channel_length,
                subscription_link->channel,
                AERON_IPC_CHANNEL,
                publication->log_file_name,
                publication->log_file_name_length) < 0)
            {
                return -1;
            }
        }
    }

    if (endpoint->conductor_fields.udp_channel != udp_channel)
    {
        aeron_udp_channel_delete(udp_channel);
    }

    return 0;
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

            if (command->registration_id == client->publication_links.array[i].registration_id)
            {
                resource->decref(resource->clientd);

                aeron_array_fast_unordered_remove(
                    (uint8_t *)client->publication_links.array, sizeof(aeron_publication_link_t), i, last_index);
                client->publication_links.length--;

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
    const char *uri = (const char *)command + sizeof(aeron_subscription_command_t);
    aeron_uri_t aeron_uri_params;
    aeron_uri_subscription_params_t params;

    if (aeron_uri_parse(uri, &aeron_uri_params) < 0 ||
        aeron_uri_subscription_params(&aeron_uri_params, &params, conductor->context) < 0)
    {
        goto error_cleanup;
    }

    if (aeron_driver_conductor_get_or_add_client(conductor, command->correlated.client_id) == NULL)
    {
        goto error_cleanup;
    }

    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, conductor->ipc_subscriptions, aeron_subscription_link_t);
    if (ensure_capacity_result < 0)
    {
        goto error_cleanup;
    }

    aeron_subscription_link_t *link = &conductor->ipc_subscriptions.array[conductor->ipc_subscriptions.length++];
    aeron_driver_init_subscription_channel(command->channel_length, uri, link);
    link->endpoint = NULL;
    link->spy_channel = NULL;
    link->stream_id = command->stream_id;
    link->client_id = command->correlated.client_id;
    link->registration_id = command->correlated.correlation_id;
    link->is_reliable = true;
    link->subscribable_list.length = 0;
    link->subscribable_list.capacity = 0;
    link->subscribable_list.array = NULL;

    aeron_driver_conductor_on_subscription_ready(
        conductor, command->correlated.correlation_id, AERON_CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED);

    for (size_t i = 0; i < conductor->ipc_publications.length; i++)
    {
        aeron_ipc_publication_entry_t *publication_entry = &conductor->ipc_publications.array[i];

        if (command->stream_id == publication_entry->publication->stream_id)
        {
            aeron_ipc_publication_t *publication = publication_entry->publication;

            if (aeron_driver_conductor_link_subscribable(
                conductor,
                link,
                &publication->conductor_fields.subscribable,
                publication->conductor_fields.managed_resource.registration_id,
                publication->session_id,
                publication->stream_id,
                aeron_ipc_publication_joining_position(publication),
                link->channel_length,
                link->channel,
                AERON_IPC_CHANNEL,
                publication->log_file_name,
                publication->log_file_name_length) < 0)
            {
                goto error_cleanup;
            }
        }
    }

    aeron_uri_close(&aeron_uri_params);
    return 0;

    error_cleanup:
    aeron_uri_close(&aeron_uri_params);
    return -1;
}

int aeron_driver_conductor_on_add_spy_subscription(
    aeron_driver_conductor_t *conductor,
    aeron_subscription_command_t *command)
{
    aeron_udp_channel_t *udp_channel = NULL;
    aeron_send_channel_endpoint_t *endpoint = NULL;
    const char *uri = (const char *)command + sizeof(aeron_subscription_command_t) + strlen(AERON_SPY_PREFIX);
    aeron_uri_subscription_params_t params;

    if (aeron_udp_channel_parse(uri, (size_t)command->channel_length - strlen(AERON_SPY_PREFIX), &udp_channel) < 0 ||
        aeron_uri_subscription_params(&udp_channel->uri, &params, conductor->context) < 0)
    {
        return -1;
    }

    if (aeron_driver_conductor_get_or_add_client(conductor, command->correlated.client_id) == NULL)
    {
        return -1;
    }

    endpoint = aeron_str_to_ptr_hash_map_get(
        &conductor->send_channel_endpoint_by_channel_map, udp_channel->canonical_form, udp_channel->canonical_length);

    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, conductor->spy_subscriptions, aeron_subscription_link_t);
    if (ensure_capacity_result < 0)
    {
        return -1;
    }

    aeron_subscription_link_t *link = &conductor->spy_subscriptions.array[conductor->spy_subscriptions.length++];
    aeron_driver_init_subscription_channel(command->channel_length, uri, link);
    link->endpoint = NULL;
    link->spy_channel = udp_channel;
    link->stream_id = command->stream_id;
    link->client_id = command->correlated.client_id;
    link->registration_id = command->correlated.correlation_id;
    link->is_reliable = params.is_reliable;
    link->is_sparse = params.is_sparse;
    link->subscribable_list.length = 0;
    link->subscribable_list.capacity = 0;
    link->subscribable_list.array = NULL;

    aeron_driver_conductor_on_subscription_ready(
        conductor, command->correlated.correlation_id, AERON_CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED);

    for (size_t i = 0, length = conductor->network_publications.length; i < length; i++)
    {
        aeron_network_publication_t *publication = conductor->network_publications.array[i].publication;

        if (command->stream_id == publication->stream_id && endpoint == publication->endpoint &&
            AERON_NETWORK_PUBLICATION_STATUS_ACTIVE == publication->conductor_fields.status)
        {
            if (aeron_driver_conductor_link_subscribable(
                conductor,
                link,
                &publication->conductor_fields.subscribable,
                publication->conductor_fields.managed_resource.registration_id,
                publication->session_id,
                publication->stream_id,
                aeron_network_publication_consumer_position(publication),
                link->channel_length,
                link->channel,
                AERON_IPC_CHANNEL,
                publication->log_file_name,
                publication->log_file_name_length) < 0)
            {
                return -1;
            }
        }
    }

    return 0;
}

int aeron_driver_conductor_on_add_network_subscription(
    aeron_driver_conductor_t *conductor,
    aeron_subscription_command_t *command)
{
    aeron_udp_channel_t *udp_channel = NULL;
    aeron_receive_channel_endpoint_t *endpoint = NULL;
    const char *uri = (const char *)command + sizeof(aeron_subscription_command_t);
    aeron_uri_subscription_params_t params;

    if (aeron_udp_channel_parse(uri, (size_t)command->channel_length, &udp_channel) < 0 ||
        aeron_uri_subscription_params(&udp_channel->uri, &params, conductor->context) < 0)
    {
        return -1;
    }

    bool is_reliable = params.is_reliable;
    if (aeron_driver_conductor_has_clashing_subscription(conductor, endpoint, command->stream_id, is_reliable))
    {
        aeron_set_err(
            EINVAL, "option conflicts with existing subscriptions: reliable=%s", is_reliable ? "true" : "false");
        return -1;
    }

    if (aeron_driver_conductor_get_or_add_client(conductor, command->correlated.client_id) == NULL)
    {
        return -1;
    }

    if ((endpoint = aeron_driver_conductor_get_or_add_receive_channel_endpoint(conductor, udp_channel)) == NULL)
    {
        return -1;
    }

    if (AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_CLOSING == endpoint->conductor_fields.status)
    {
        aeron_set_err(EINVAL, "%s", "receive_channel_endpoint found in CLOSING state");
        return -1;
    }

    if (aeron_receive_channel_endpoint_incref_to_stream(endpoint, command->stream_id) < 0)
    {
        return -1;
    }

    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, conductor->network_subscriptions, aeron_subscription_link_t);
    if (ensure_capacity_result >= 0)
    {
        aeron_subscription_link_t *link =
            &conductor->network_subscriptions.array[conductor->network_subscriptions.length++];

        aeron_driver_init_subscription_channel(command->channel_length, uri, link);
        link->endpoint = endpoint;
        link->spy_channel = NULL;
        link->stream_id = command->stream_id;
        link->client_id = command->correlated.client_id;
        link->registration_id = command->correlated.correlation_id;
        link->is_reliable = params.is_reliable;
        link->is_sparse = params.is_sparse;
        link->subscribable_list.length = 0;
        link->subscribable_list.capacity = 0;
        link->subscribable_list.array = NULL;

        aeron_driver_conductor_on_subscription_ready(
            conductor, command->correlated.correlation_id, (int32_t)endpoint->channel_status.counter_id);

        for (size_t i = 0, length = conductor->publication_images.length; i < length; i++)
        {
            aeron_publication_image_t *image = conductor->publication_images.array[i].image;

            if (endpoint == image->endpoint && command->stream_id == image->stream_id &&
                aeron_publication_image_is_accepting_subscriptions(image))
            {
                char source_identity[AERON_MAX_PATH];
                aeron_format_source_identity(source_identity, sizeof(source_identity), &image->source_address);

                if (aeron_driver_conductor_link_subscribable(
                    conductor,
                    link,
                    &image->conductor_fields.subscribable,
                    image->conductor_fields.managed_resource.registration_id,
                    image->session_id,
                    image->stream_id,
                    aeron_counter_get(image->rcv_pos_position.value_addr),
                    link->channel_length,
                    link->channel,
                    source_identity,
                    image->log_file_name,
                    image->log_file_name_length) < 0)
                {
                    return -1;
                }
            }
        }

        if (endpoint->conductor_fields.udp_channel != udp_channel)
        {
            aeron_udp_channel_delete(udp_channel);
        }

        return 0;
    }

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
            aeron_driver_conductor_unlink_all_subscribable(conductor, link);

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->ipc_subscriptions.array, sizeof(aeron_subscription_link_t), i, last_index);
            conductor->ipc_subscriptions.length--;

            aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);
            return 0;
        }
    }

    for (size_t i = 0, size = conductor->network_subscriptions.length, last_index = size - 1; i < size; i++)
    {
        aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];

        if (command->registration_id == link->registration_id)
        {
            aeron_receive_channel_endpoint_t *endpoint = link->endpoint;

            link->endpoint = NULL;
            aeron_receive_channel_endpoint_decref_to_stream(endpoint, link->stream_id);
            if (AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_CLOSING == endpoint->conductor_fields.status)
            {
                aeron_udp_channel_t *udp_channel = endpoint->conductor_fields.udp_channel;

                aeron_str_to_ptr_hash_map_remove(
                    &conductor->receive_channel_endpoint_by_channel_map,
                    udp_channel->canonical_form,
                    udp_channel->canonical_length);
            }

            aeron_driver_conductor_unlink_all_subscribable(conductor, link);

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->network_subscriptions.array, sizeof(aeron_subscription_link_t), i, last_index);
            conductor->network_subscriptions.length--;

            aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);
            return 0;
        }
    }

    for (size_t i = 0, size = conductor->spy_subscriptions.length, last_index = size - 1; i < size; i++)
    {
        aeron_subscription_link_t *link = &conductor->spy_subscriptions.array[i];

        if (command->registration_id == link->registration_id)
        {
            aeron_driver_conductor_unlink_all_subscribable(conductor, link);

            aeron_udp_channel_delete(link->spy_channel);
            link->spy_channel = NULL;
            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->spy_subscriptions.array, sizeof(aeron_subscription_link_t), i, last_index);
            conductor->spy_subscriptions.length--;

            aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);
            return 0;
        }
    }

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

    if ((index = aeron_driver_conductor_find_client(conductor, client_id)) >= 0)
    {
        aeron_client_t *client = &conductor->clients.array[index];

        client->time_of_last_keepalive_ms = conductor->epoch_clock();
        aeron_counter_set_ordered(client->heartbeat_status.value_addr, client->time_of_last_keepalive_ms);
    }

    return 0;
}

int aeron_driver_conductor_on_add_destination(
    aeron_driver_conductor_t *conductor,
    aeron_destination_command_t *command)
{
    aeron_send_channel_endpoint_t *endpoint = NULL;
    const char *command_uri = (const char *)command + sizeof(aeron_destination_command_t);

    for (size_t i = 0, length = conductor->network_publications.length; i < length; i++)
    {
        aeron_network_publication_t *publication = conductor->network_publications.array[i].publication;

        if (command->registration_id == publication->conductor_fields.managed_resource.registration_id)
        {
            endpoint = publication->endpoint;
            break;
        }
    }

    if (NULL != endpoint)
    {
        char buffer[AERON_MAX_PATH];
        aeron_uri_t uri_params;
        struct sockaddr_storage destination_addr;

        if (NULL != endpoint->destination_tracker || !endpoint->destination_tracker->is_manual_control_mode)
        {
            aeron_set_err(EINVAL, "channel does not allow manual control of destinations: %s", buffer);
            return -1;
        }

        strncpy(buffer, command_uri, (size_t)command->channel_length);
        if (aeron_uri_parse(buffer, &uri_params) < 0)
        {
            return -1;
        }

        if (uri_params.type != AERON_URI_UDP || NULL == uri_params.params.udp.endpoint_key)
        {
            aeron_set_err(EINVAL, "incorrect URI format for destination: %s", buffer);
            return -1;
        }

        if (aeron_host_and_port_parse_and_resolve(uri_params.params.udp.endpoint_key, &destination_addr) < 0)
        {
            aeron_set_err(
                aeron_errcode(),
                "could not resolve destination address=(%s): %s",
                uri_params.params.udp.endpoint_key,
                aeron_errmsg());
            return -1;
        }

        aeron_driver_sender_proxy_on_add_destination(conductor->context->sender_proxy, endpoint, &destination_addr);
        aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);

        return 0;
    }

    aeron_set_err(
        EINVAL,
        "unknown add destination registration_id=%" PRId64,
        command->correlated.client_id,
        command->registration_id);

    return -1;
}

int aeron_driver_conductor_on_remove_destination(
    aeron_driver_conductor_t *conductor,
    aeron_destination_command_t *command)
{
    aeron_send_channel_endpoint_t *endpoint = NULL;
    const char *command_uri = (const char *)command + sizeof(aeron_destination_command_t);

    for (size_t i = 0, length = conductor->network_publications.length; i < length; i++)
    {
        aeron_network_publication_t *publication = conductor->network_publications.array[i].publication;

        if (command->registration_id == publication->conductor_fields.managed_resource.registration_id)
        {
            endpoint = publication->endpoint;
            break;
        }
    }

    if (NULL != endpoint)
    {
        char buffer[AERON_MAX_PATH];
        aeron_uri_t uri_params;
        struct sockaddr_storage destination_addr;

        if (NULL != endpoint->destination_tracker || !endpoint->destination_tracker->is_manual_control_mode)
        {
            aeron_set_err(EINVAL, "channel does not allow manual control of destinations: %s", buffer);
            return -1;
        }

        strncpy(buffer, command_uri, (size_t)command->channel_length);
        if (aeron_uri_parse(buffer, &uri_params) < 0)
        {
            return -1;
        }

        if (uri_params.type != AERON_URI_UDP || NULL == uri_params.params.udp.endpoint_key)
        {
            aeron_set_err(EINVAL, "incorrect URI format for destination: %s", buffer);
            return -1;
        }

        if (aeron_host_and_port_parse_and_resolve(uri_params.params.udp.endpoint_key, &destination_addr) < 0)
        {
            aeron_set_err(
                aeron_errcode(),
                "could not resolve destination address=(%s): %s",
                uri_params.params.udp.endpoint_key,
                aeron_errmsg());
            return -1;
        }

        aeron_driver_sender_proxy_on_remove_destination(conductor->context->sender_proxy, endpoint, &destination_addr);
        aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);

        return 0;
    }

    aeron_set_err(
        EINVAL,
        "unknown remove destination registration_id=%" PRId64,
        command->correlated.client_id,
        command->registration_id);

    return -1;
}

int aeron_driver_conductor_on_add_counter(
    aeron_driver_conductor_t *conductor,
    aeron_counter_command_t *command)
{
    aeron_client_t *client = NULL;

    if ((client = aeron_driver_conductor_get_or_add_client(conductor, command->correlated.client_id)) == NULL)
    {
        return -1;
    }

    const uint8_t *cursor = (const uint8_t *)command + sizeof(aeron_counter_command_t);
    int32_t key_length;

    memcpy(&key_length, cursor, sizeof(key_length));
    const uint8_t *key = cursor + sizeof(int32_t);

    cursor = key + AERON_ALIGN(key_length, sizeof(int32_t));
    int32_t label_length;

    memcpy(&label_length, cursor, sizeof(label_length));
    const char *label = (const char *)cursor + sizeof(int32_t);

    const int32_t counter_id = aeron_counters_manager_allocate(
        &conductor->counters_manager, command->type_id, key, (size_t)key_length, label, (size_t)label_length);

    if (counter_id >= 0)
    {
        int ensure_capacity_result = 0;

        AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, client->counter_links, aeron_counter_link_t);
        if (ensure_capacity_result >= 0)
        {
            aeron_counter_link_t *link = &client->counter_links.array[client->counter_links.length++];

            link->registration_id = command->correlated.correlation_id;
            link->counter_id = counter_id;

            aeron_driver_conductor_on_counter_ready(conductor, command->correlated.correlation_id, counter_id);
            return 0;
        }
    }

    return -1;
}

int aeron_driver_conductor_on_remove_counter(
    aeron_driver_conductor_t *conductor,
    aeron_remove_command_t *command)
{
    int index;

    if ((index = aeron_driver_conductor_find_client(conductor, command->correlated.client_id)) >= 0)
    {
        aeron_client_t *client = &conductor->clients.array[index];

        for (size_t i = 0, size = client->counter_links.length, last_index = size - 1; i < size; i++)
        {
            aeron_counter_link_t *link = &client->counter_links.array[i];

            if (command->registration_id == link->registration_id)
            {
                aeron_driver_conductor_on_operation_succeeded(conductor, command->correlated.correlation_id);
                aeron_driver_conductor_on_unavailable_counter(conductor, link->registration_id, link->counter_id);

                aeron_counters_manager_free(&conductor->counters_manager, link->counter_id);

                aeron_array_fast_unordered_remove(
                    (uint8_t *)client->counter_links.array, sizeof(aeron_counter_link_t), i, last_index);
                client->counter_links.length--;

                return 0;
            }
        }
    }

    aeron_set_err(
        EINVAL,
        "unknown counter client_id=%" PRId64 ", registration_id=%" PRId64,
        command->correlated.client_id,
        command->registration_id);

    return -1;
}

int aeron_driver_conductor_on_client_close(
    aeron_driver_conductor_t *conductor,
    aeron_correlated_command_t *command)
{
    int index;

    if ((index = aeron_driver_conductor_find_client(conductor, command->client_id)) >= 0)
    {
        aeron_client_t *client = &conductor->clients.array[index];

        client->time_of_last_keepalive_ms = 0;
        aeron_counter_set_ordered(client->heartbeat_status.value_addr, client->time_of_last_keepalive_ms);
    }

    return 0;
}

void aeron_driver_conductor_on_create_publication_image(void *clientd, void *item)
{
    aeron_driver_conductor_t *conductor = (aeron_driver_conductor_t *)clientd;
    aeron_command_create_publication_image_t *command = (aeron_command_create_publication_image_t *)item;
    aeron_receive_channel_endpoint_t *endpoint = command->endpoint;

    if (aeron_receiver_channel_endpoint_validate_sender_mtu_length(
        endpoint, (size_t)command->mtu_length, conductor->context->initial_window_length) < 0)
    {
        aeron_driver_conductor_error(conductor, aeron_errcode(), aeron_errmsg(), aeron_errmsg());
        return;
    }

    if (!aeron_driver_conductor_has_network_subscription_interest(conductor, endpoint, command->stream_id))
    {
        return;
    }

    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, conductor->publication_images, aeron_publication_image_entry_t);
    if (ensure_capacity_result < 0)
    {
        return;
    }

    const int64_t registration_id = aeron_mpsc_rb_next_correlation_id(&conductor->to_driver_commands);
    const int64_t join_position = aeron_logbuffer_compute_position(command->active_term_id, command->term_offset,
        (size_t)aeron_number_of_trailing_zeroes(command->term_length), command->initial_term_id);

    const char *uri = endpoint->conductor_fields.udp_channel->original_uri;
    int32_t uri_length = (int32_t)endpoint->conductor_fields.udp_channel->uri_length;

    aeron_congestion_control_strategy_t *congestion_control = NULL;
    if (conductor->context->congestion_control_supplier_func(
        &congestion_control,
        uri_length,
        uri,
        command->stream_id,
        command->session_id,
        registration_id,
        command->term_length,
        command->mtu_length,
        conductor->context,
        &conductor->counters_manager) < 0)
    {
        return;
    }

    aeron_position_t rcv_hwm_position;
    aeron_position_t rcv_pos_position;

    rcv_hwm_position.counter_id = aeron_counter_receiver_hwm_allocate(
        &conductor->counters_manager, registration_id, command->session_id, command->stream_id, uri_length, uri);
    rcv_pos_position.counter_id = aeron_counter_receiver_position_allocate(
        &conductor->counters_manager, registration_id, command->session_id, command->stream_id, uri_length, uri);

    if (rcv_hwm_position.counter_id < 0 || rcv_pos_position.counter_id < 0)
    {
        return;
    }

    rcv_hwm_position.value_addr = aeron_counter_addr(&conductor->counters_manager, (int32_t)rcv_hwm_position.counter_id);
    rcv_pos_position.value_addr = aeron_counter_addr(&conductor->counters_manager, (int32_t)rcv_pos_position.counter_id);

    bool is_reliable = conductor->network_subscriptions.array[0].is_reliable;
    aeron_publication_image_t *image = NULL;
    if (aeron_publication_image_create(
        &image,
        endpoint,
        conductor->context,
        registration_id,
        command->session_id,
        command->stream_id,
        command->initial_term_id,
        command->active_term_id,
        command->term_offset,
        &rcv_hwm_position,
        &rcv_pos_position,
        congestion_control,
        &command->control_address,
        &command->src_address,
        command->term_length,
        command->mtu_length,
        &conductor->loss_reporter,
        is_reliable,
        aeron_driver_conductor_is_oldest_subscription_sparse(conductor, endpoint, command->stream_id, registration_id),
        &conductor->system_counters) < 0)
    {
        return;
    }

    conductor->publication_images.array[conductor->publication_images.length++].image = image;

    for (size_t i = 0, length = conductor->network_subscriptions.length; i < length; i++)
    {
        char source_identity[AERON_MAX_PATH];
        aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];

        if (endpoint != link->endpoint || command->stream_id != link->stream_id)
        {
            continue;
        }

        aeron_format_source_identity(source_identity, sizeof(source_identity), &command->src_address);

        if (aeron_driver_conductor_link_subscribable(
            conductor,
            link,
            &image->conductor_fields.subscribable,
            registration_id,
            command->session_id,
            command->stream_id,
            join_position,
            link->channel_length,
            link->channel,
            source_identity,
            image->log_file_name,
            image->log_file_name_length) < 0)
        {
            return;
        }
    }

    aeron_driver_receiver_proxy_on_add_publication_image(conductor->context->receiver_proxy, endpoint, image);
    aeron_driver_receiver_proxy_on_delete_create_publication_image_cmd(conductor->context->receiver_proxy, item);
}

void aeron_driver_conductor_on_linger_buffer(void *clientd, void *item)
{
    aeron_driver_conductor_t *conductor = (aeron_driver_conductor_t *)clientd;
    aeron_command_base_t *command = (aeron_command_base_t *)item;
    int ensure_capacity_result = 0;

    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, conductor->lingering_resources, aeron_linger_resource_entry_t);
    if (ensure_capacity_result >= 0)
    {
        aeron_linger_resource_entry_t *entry =
            &conductor->lingering_resources.array[conductor->lingering_resources.length++];

        entry->buffer = command->item;
        entry->has_reached_end_of_life = false;
        entry->timeout = conductor->nano_clock() + AERON_DRIVER_CONDUCTOR_LINGER_RESOURCE_TIMEOUT_NS;
    }

    if (conductor->context->threading_mode != AERON_THREADING_MODE_SHARED)
    {
        aeron_free(command);
        /* do not know where it came from originally, so just free command on the conductor duty cycle */
    }
}

extern void aeron_driver_subscribable_null_hook(void *clientd, int64_t *value_addr);

extern bool aeron_driver_conductor_is_subscribable_linked(
    aeron_subscription_link_t *link, aeron_subscribable_t *subscribable);

extern bool aeron_driver_conductor_has_network_subscription_interest(
    aeron_driver_conductor_t *conductor, const aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id);

extern bool aeron_driver_conductor_has_clashing_subscription(
    aeron_driver_conductor_t *conductor,
    const aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    bool is_reliable);

extern bool aeron_driver_conductor_is_oldest_subscription_sparse(
    aeron_driver_conductor_t *conductor,
    const aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    int64_t highest_id);

extern size_t aeron_driver_conductor_num_clients(aeron_driver_conductor_t *conductor);

extern size_t aeron_driver_conductor_num_ipc_publications(aeron_driver_conductor_t *conductor);

extern size_t aeron_driver_conductor_num_ipc_subscriptions(aeron_driver_conductor_t *conductor);

extern size_t aeron_driver_conductor_num_network_publications(aeron_driver_conductor_t *conductor);

extern size_t aeron_driver_conductor_num_network_subscriptions(aeron_driver_conductor_t *conductor);

extern size_t aeron_driver_conductor_num_spy_subscriptions(aeron_driver_conductor_t *conductor);

extern size_t aeron_driver_conductor_num_send_channel_endpoints(aeron_driver_conductor_t *conductor);

extern size_t aeron_driver_conductor_num_receive_channel_endpoints(aeron_driver_conductor_t *conductor);

extern size_t aeron_driver_conductor_num_active_ipc_subscriptions(
    aeron_driver_conductor_t *conductor, int32_t stream_id);

extern size_t aeron_driver_conductor_num_active_network_subscriptions(
    aeron_driver_conductor_t *conductor, const char *original_uri, int32_t stream_id);

extern size_t aeron_driver_conductor_num_active_spy_subscriptions(
    aeron_driver_conductor_t *conductor, const char *original_uri, int32_t stream_id);

extern size_t aeron_driver_conductor_num_images(aeron_driver_conductor_t *conductor);

extern aeron_ipc_publication_t *aeron_driver_conductor_find_ipc_publication(
    aeron_driver_conductor_t *conductor, int64_t id);

extern aeron_network_publication_t *aeron_driver_conductor_find_network_publication(
    aeron_driver_conductor_t *conductor, int64_t id);

extern aeron_publication_image_t *aeron_driver_conductor_find_publication_image(
    aeron_driver_conductor_t *conductor, aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id);

extern void aeron_driver_init_subscription_channel(
    int32_t uri_length, const char *uri, aeron_subscription_link_t *link);

extern int64_t *aeron_driver_conductor_system_counter_addr(
    aeron_driver_conductor_t *conductor, aeron_system_counter_enum_t type);
