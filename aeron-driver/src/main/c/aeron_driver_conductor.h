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
#ifndef AERON_DRIVER_CONDUCTOR_H
#define AERON_DRIVER_CONDUCTOR_H

#include "aeron_driver_common.h"
#include "aeron_driver_context.h"
#include "uri/aeron_uri.h"
#include "concurrent/aeron_mpsc_rb.h"
#include "concurrent/aeron_broadcast_transmitter.h"
#include "concurrent/aeron_distinct_error_log.h"
#include "concurrent/aeron_counters_manager.h"
#include "command/aeron_control_protocol.h"
#include "aeron_system_counters.h"
#include "aeron_ipc_publication.h"
#include "collections/aeron_str_to_ptr_hash_map.h"
#include "media/aeron_send_channel_endpoint.h"
#include "media/aeron_receive_channel_endpoint.h"
#include "aeron_driver_conductor_proxy.h"
#include "aeron_publication_image.h"
#include "reports/aeron_loss_reporter.h"

#define AERON_DRIVER_CONDUCTOR_LINGER_RESOURCE_TIMEOUT_NS (5 * 1000 * 1000 * 1000L)

typedef struct aeron_publication_link_stct
{
    aeron_driver_managed_resource_t *resource;
    int64_t registration_id;
}
aeron_publication_link_t;

typedef struct aeron_counter_link_stct
{
    int64_t registration_id;
    int32_t counter_id;
}
aeron_counter_link_t;

typedef struct aeron_client_stct
{
    int64_t client_id;
    int64_t client_liveness_timeout_ms;
    int64_t time_of_last_keepalive_ms;
    bool reached_end_of_life;

    aeron_counter_t heartbeat_status;

    struct publication_link_stct
    {
        aeron_publication_link_t *array;
        size_t length;
        size_t capacity;
    }
    publication_links;

    struct counter_link_stct
    {
        aeron_counter_link_t *array;
        size_t length;
        size_t capacity;
    }
    counter_links;
}
aeron_client_t;

typedef struct aeron_subscribable_list_entry_stct
{
    aeron_subscribable_t *subscribable;
    int64_t counter_id;
}
aeron_subscribable_list_entry_t;

typedef struct aeron_subscription_link_stct
{
    char channel[AERON_MAX_PATH];
    int64_t client_id;
    int64_t registration_id;
    int32_t stream_id;
    int32_t channel_length;
    bool is_reliable;
    bool is_sparse;

    aeron_receive_channel_endpoint_t *endpoint;
    aeron_udp_channel_t *spy_channel;

    struct subscribable_list_stct
    {
        aeron_subscribable_list_entry_t *array;
        size_t length;
        size_t capacity;
    }
    subscribable_list;
}
aeron_subscription_link_t;

typedef struct aeron_ipc_publication_entry_stct
{
    aeron_ipc_publication_t *publication;
}
aeron_ipc_publication_entry_t;

typedef struct aeron_network_publication_entry_stct
{
    aeron_network_publication_t *publication;
}
aeron_network_publication_entry_t;

typedef struct aeron_send_channel_endpoint_entry_stct
{
    aeron_send_channel_endpoint_t *endpoint;
}
aeron_send_channel_endpoint_entry_t;

typedef struct aeron_receive_channel_endpoint_entry_stct
{
    aeron_receive_channel_endpoint_t *endpoint;
}
aeron_receive_channel_endpoint_entry_t;

typedef struct aeron_publication_image_entry_stct
{
    aeron_publication_image_t *image;
}
aeron_publication_image_entry_t;

typedef struct aeron_linger_resource_entry_stct
{
    uint8_t *buffer;
    int64_t timeout;
    bool has_reached_end_of_life;
}
aeron_linger_resource_entry_t;

typedef struct aeron_driver_conductor_stct aeron_driver_conductor_t;

typedef struct aeron_driver_conductor_stct
{
    aeron_driver_context_t *context;
    aeron_mpsc_rb_t to_driver_commands;
    aeron_broadcast_transmitter_t to_clients;
    aeron_distinct_error_log_t error_log;
    aeron_counters_manager_t counters_manager;
    aeron_system_counters_t system_counters;
    aeron_driver_conductor_proxy_t conductor_proxy;
    aeron_loss_reporter_t loss_reporter;

    aeron_str_to_ptr_hash_map_t send_channel_endpoint_by_channel_map;
    aeron_str_to_ptr_hash_map_t receive_channel_endpoint_by_channel_map;

    struct client_stct
    {
        aeron_client_t *array;
        size_t length;
        size_t capacity;
        void (*on_time_event)(aeron_driver_conductor_t *, aeron_client_t *, int64_t, int64_t);
        bool (*has_reached_end_of_life)(aeron_driver_conductor_t *, aeron_client_t *);
        void (*delete_func)(aeron_driver_conductor_t *, aeron_client_t *);
    }
    clients;

    struct ipc_subscriptions_stct
    {
        aeron_subscription_link_t *array;
        size_t length;
        size_t capacity;
    }
    ipc_subscriptions;

    struct ipc_publication_stct
    {
        aeron_ipc_publication_entry_t *array;
        size_t length;
        size_t capacity;
        void (*on_time_event)(aeron_driver_conductor_t *, aeron_ipc_publication_entry_t *, int64_t, int64_t);
        bool (*has_reached_end_of_life)(aeron_driver_conductor_t *, aeron_ipc_publication_entry_t *);
        void (*delete_func)(aeron_driver_conductor_t *, aeron_ipc_publication_entry_t *);
    }
    ipc_publications;

    struct network_subscriptions_stct
    {
        aeron_subscription_link_t *array;
        size_t length;
        size_t capacity;
    }
    network_subscriptions;

    struct spy_subscriptions_stct
    {
        aeron_subscription_link_t *array;
        size_t length;
        size_t capacity;
    }
    spy_subscriptions;

    struct network_publication_stct
    {
        aeron_network_publication_entry_t *array;
        size_t length;
        size_t capacity;
        void (*on_time_event)(aeron_driver_conductor_t *, aeron_network_publication_entry_t *, int64_t, int64_t);
        bool (*has_reached_end_of_life)(aeron_driver_conductor_t *, aeron_network_publication_entry_t *);
        void (*delete_func)(aeron_driver_conductor_t *, aeron_network_publication_entry_t *);
    }
    network_publications;

    struct send_channel_endpoint_stct
    {
        aeron_send_channel_endpoint_entry_t *array;
        size_t length;
        size_t capacity;
        void (*on_time_event)(aeron_driver_conductor_t *, aeron_send_channel_endpoint_entry_t *, int64_t, int64_t);
        bool (*has_reached_end_of_life)(aeron_driver_conductor_t *, aeron_send_channel_endpoint_entry_t *);
        void (*delete_func)(aeron_driver_conductor_t *, aeron_send_channel_endpoint_entry_t *);
    }
    send_channel_endpoints;

    struct receive_channel_endpoint_stct
    {
        aeron_receive_channel_endpoint_entry_t *array;
        size_t length;
        size_t capacity;
        void (*on_time_event)(aeron_driver_conductor_t *, aeron_receive_channel_endpoint_entry_t *, int64_t, int64_t);
        bool (*has_reached_end_of_life)(aeron_driver_conductor_t *, aeron_receive_channel_endpoint_entry_t *);
        void (*delete_func)(aeron_driver_conductor_t *, aeron_receive_channel_endpoint_entry_t *);
    }
    receive_channel_endpoints;

    struct publication_image_stct
    {
        aeron_publication_image_entry_t *array;
        size_t length;
        size_t capacity;
        void (*on_time_event)(aeron_driver_conductor_t *, aeron_publication_image_entry_t *, int64_t, int64_t);
        bool (*has_reached_end_of_life)(aeron_driver_conductor_t *, aeron_publication_image_entry_t *);
        void (*delete_func)(aeron_driver_conductor_t *, aeron_publication_image_entry_t *);
    }
    publication_images;

    struct aeron_driver_conductor_lingering_resources_stct
    {
        aeron_linger_resource_entry_t *array;
        size_t length;
        size_t capacity;
        void (*on_time_event)(aeron_driver_conductor_t *, aeron_linger_resource_entry_t *, int64_t, int64_t);
        bool (*has_reached_end_of_life)(aeron_driver_conductor_t *, aeron_linger_resource_entry_t *);
        void (*delete_func)(aeron_driver_conductor_t *, aeron_linger_resource_entry_t *);
    }
    lingering_resources;

    int64_t *errors_counter;
    int64_t *unblocked_commands_counter;
    int64_t *client_timeouts_counter;

    aeron_clock_func_t nano_clock;
    aeron_clock_func_t epoch_clock;

    int64_t time_of_last_timeout_check_ns;
    int64_t time_of_last_to_driver_position_change_ns;
    int64_t last_consumer_command_position;
    int32_t next_session_id;
}
aeron_driver_conductor_t;

#define AERON_FORMAT_BUFFER(buffer, format, ...) snprintf(buffer, sizeof(buffer) - 1, format, __VA_ARGS__)

void aeron_client_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_client_t *client, int64_t now_ns, int64_t now_ms);

bool aeron_client_has_reached_end_of_life(aeron_driver_conductor_t *conductor, aeron_client_t *client);

void aeron_client_delete(aeron_driver_conductor_t *conductor, aeron_client_t *);

void aeron_ipc_publication_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_ipc_publication_entry_t *entry, int64_t now_ns, int64_t now_ms);

bool aeron_ipc_publication_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_ipc_publication_entry_t *entry);

void aeron_ipc_publication_entry_delete(aeron_driver_conductor_t *conductor, aeron_ipc_publication_entry_t *);

void aeron_network_publication_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_network_publication_entry_t *entry, int64_t now_ns, int64_t now_ms);

bool aeron_network_publication_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_network_publication_entry_t *entry);

void aeron_network_publication_entry_delete(aeron_driver_conductor_t *conductor, aeron_network_publication_entry_t *);

void aeron_send_channel_endpoint_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_send_channel_endpoint_entry_t *entry, int64_t now_ns, int64_t now_ms);

bool aeron_send_channel_endpoint_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_send_channel_endpoint_entry_t *entry);

void aeron_send_channel_endpoint_entry_delete(
    aeron_driver_conductor_t *conductor, aeron_send_channel_endpoint_entry_t *);

void aeron_receive_channel_endpoint_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_receive_channel_endpoint_entry_t *entry, int64_t now_ns, int64_t now_ms);

bool aeron_receive_channel_endpoint_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_receive_channel_endpoint_entry_t *entry);

void aeron_receive_channel_endpoint_entry_delete(
    aeron_driver_conductor_t *conductor, aeron_receive_channel_endpoint_entry_t *);

void aeron_publication_image_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_publication_image_entry_t *entry, int64_t now_ns, int64_t now_ms);

bool aeron_publication_image_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_publication_image_entry_t *entry);

void aeron_publication_image_entry_delete(aeron_driver_conductor_t *conductor, aeron_publication_image_entry_t *);

void aeron_linger_resource_entry_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_linger_resource_entry_t *entry, int64_t now_ns, int64_t now_ms);

bool aeron_linger_resource_entry_has_reached_end_of_life(
    aeron_driver_conductor_t *conductor, aeron_linger_resource_entry_t *entry);

void aeron_linger_resource_entry_delete(aeron_driver_conductor_t *conductor, aeron_linger_resource_entry_t *);

void aeron_driver_conductor_image_transition_to_linger(
    aeron_driver_conductor_t *conductor, aeron_publication_image_t *image);

int aeron_driver_conductor_init(aeron_driver_conductor_t *conductor, aeron_driver_context_t *context);

void aeron_driver_conductor_client_transmit(
    aeron_driver_conductor_t *conductor,
    int32_t msg_type_id,
    const void *message,
    size_t length);

void aeron_driver_conductor_on_unavailable_image(
    aeron_driver_conductor_t *conductor,
    int64_t correlation_id,
    int64_t subscription_registration_id,
    int32_t stream_id,
    const char *channel,
    size_t channel_length);

void aeron_driver_conductor_on_client_timeout(
    aeron_driver_conductor_t *conductor,
    int64_t correlation_id);

void aeron_driver_conductor_cleanup_spies(
    aeron_driver_conductor_t *conductor, aeron_network_publication_t *publication);

void aeron_driver_conductor_cleanup_network_publication(
    aeron_driver_conductor_t *conductor, aeron_network_publication_t *publication);

void aeron_driver_conductor_on_command(int32_t msg_type_id, const void *message, size_t length, void *clientd);

int aeron_driver_conductor_do_work(void *clientd);

void aeron_driver_conductor_on_close(void *clientd);

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
    size_t log_file_name_length);

void aeron_driver_conductor_unlink_subscribable(aeron_subscription_link_t *link, aeron_subscribable_t *subscribable);

void aeron_driver_conductor_unlink_all_subscribable(
    aeron_driver_conductor_t *conductor, aeron_subscription_link_t *link);

int aeron_driver_conductor_on_add_ipc_publication(
    aeron_driver_conductor_t *conductor, aeron_publication_command_t *command, bool is_exclusive);

int aeron_driver_conductor_on_add_network_publication(
    aeron_driver_conductor_t *conductor, aeron_publication_command_t *command, bool is_exclusive);

int aeron_driver_conductor_on_remove_publication(aeron_driver_conductor_t *conductor, aeron_remove_command_t *command);

int aeron_driver_conductor_on_add_ipc_subscription(
    aeron_driver_conductor_t *conductor, aeron_subscription_command_t *command);

int aeron_driver_conductor_on_add_spy_subscription(
    aeron_driver_conductor_t *conductor, aeron_subscription_command_t *command);

int aeron_driver_conductor_on_add_network_subscription(
    aeron_driver_conductor_t *conductor, aeron_subscription_command_t *command);

int aeron_driver_conductor_on_remove_subscription(aeron_driver_conductor_t *conductor, aeron_remove_command_t *command);

int aeron_driver_conductor_on_client_keepalive(aeron_driver_conductor_t *conductor, int64_t client_id);

int aeron_driver_conductor_on_add_destination(
    aeron_driver_conductor_t *conductor, aeron_destination_command_t *command);

int aeron_driver_conductor_on_remove_destination(
    aeron_driver_conductor_t *conductor, aeron_destination_command_t *command);

int aeron_driver_conductor_on_add_counter(aeron_driver_conductor_t *conductor, aeron_counter_command_t *command);

int aeron_driver_conductor_on_remove_counter(aeron_driver_conductor_t *conductor, aeron_remove_command_t *command);

int aeron_driver_conductor_on_client_close(aeron_driver_conductor_t *conductor, aeron_correlated_command_t *command);

int aeron_driver_conductor_on_terminate_driver(
    aeron_driver_conductor_t *conductor, aeron_terminate_driver_command_t *command);

void aeron_driver_conductor_on_create_publication_image(void *clientd, void *item);

void aeron_driver_conductor_on_linger_buffer(void *clientd, void *item);

inline bool aeron_driver_conductor_is_subscribable_linked(
    aeron_subscription_link_t *link, aeron_subscribable_t *subscribable)
{
    bool result = false;

    for (size_t i = 0; i < link->subscribable_list.length; i++)
    {
        aeron_subscribable_list_entry_t *entry = &link->subscribable_list.array[i];

        if (subscribable == entry->subscribable)
        {
            result = true;
            break;
        }
    }

    return result;
}

inline bool aeron_driver_conductor_has_network_subscription_interest(
    aeron_driver_conductor_t *conductor, const aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id)
{
    for (size_t i = 0, length = conductor->network_subscriptions.length; i < length; i++)
    {
        aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];

        if (endpoint == link->endpoint && stream_id == link->stream_id)
        {
            return true;
        }
    }

    return false;
}

inline bool aeron_driver_conductor_has_clashing_subscription(
    aeron_driver_conductor_t *conductor,
    const aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    bool is_reliable)
{
    for (size_t i = 0, length = conductor->network_subscriptions.length; i < length; i++)
    {
        aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];

        if (endpoint == link->endpoint && stream_id == link->stream_id && link->is_reliable != is_reliable)
        {
            return true;
        }
    }

    return false;
}

inline bool aeron_driver_conductor_is_oldest_subscription_sparse(
    aeron_driver_conductor_t *conductor,
    const aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    int64_t highest_id)
{
    int64_t registration_id = highest_id;
    bool is_sparse = conductor->context->term_buffer_sparse_file;

    for (size_t i = 0, length = conductor->network_subscriptions.length; i < length; i++)
    {
        aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];

        if (endpoint == link->endpoint && stream_id == link->stream_id && link->registration_id < registration_id)
        {
            registration_id = link->registration_id;
            is_sparse = link->is_sparse;
        }
    }

    return is_sparse;
}

inline size_t aeron_driver_conductor_num_clients(aeron_driver_conductor_t *conductor)
{
    return conductor->clients.length;
}

inline size_t aeron_driver_conductor_num_ipc_publications(aeron_driver_conductor_t *conductor)
{
    return conductor->ipc_publications.length;
}

inline size_t aeron_driver_conductor_num_ipc_subscriptions(aeron_driver_conductor_t *conductor)
{
    return conductor->ipc_subscriptions.length;
}

inline size_t aeron_driver_conductor_num_network_publications(aeron_driver_conductor_t *conductor)
{
    return conductor->network_publications.length;
}

inline size_t aeron_driver_conductor_num_network_subscriptions(aeron_driver_conductor_t *conductor)
{
    return conductor->network_subscriptions.length;
}

inline size_t aeron_driver_conductor_num_spy_subscriptions(aeron_driver_conductor_t *conductor)
{
    return conductor->spy_subscriptions.length;
}

inline size_t aeron_driver_conductor_num_send_channel_endpoints(aeron_driver_conductor_t *conductor)
{
    return conductor->send_channel_endpoints.length;
}

inline size_t aeron_driver_conductor_num_receive_channel_endpoints(aeron_driver_conductor_t *conductor)
{
    return conductor->receive_channel_endpoints.length;
}

inline size_t aeron_driver_conductor_num_images(aeron_driver_conductor_t *conductor)
{
    return conductor->publication_images.length;
}

inline size_t aeron_driver_conductor_num_active_ipc_subscriptions(
    aeron_driver_conductor_t *conductor, int32_t stream_id)
{
    size_t num = 0;

    for (size_t i = 0, length = conductor->ipc_subscriptions.length; i < length; i++)
    {
        aeron_subscription_link_t *link = &conductor->ipc_subscriptions.array[i];

        if (stream_id == link->stream_id)
        {
            num += link->subscribable_list.length;
        }
    }

    return num;
}

inline size_t aeron_driver_conductor_num_active_network_subscriptions(
    aeron_driver_conductor_t *conductor, const char *original_uri, int32_t stream_id)
{
    size_t num = 0;

    for (size_t i = 0, length = conductor->network_subscriptions.length; i < length; i++)
    {
        aeron_subscription_link_t *link = &conductor->network_subscriptions.array[i];
        aeron_udp_channel_t *udp_channel = link->endpoint->conductor_fields.udp_channel;

        if (stream_id == link->stream_id &&
            strncmp(original_uri, udp_channel->original_uri, udp_channel->uri_length) == 0)
        {
            num += link->subscribable_list.length;
        }
    }

    return num;
}

inline size_t aeron_driver_conductor_num_active_spy_subscriptions(
    aeron_driver_conductor_t *conductor, const char *original_uri, int32_t stream_id)
{
    size_t num = 0;

    for (size_t i = 0, length = conductor->spy_subscriptions.length; i < length; i++)
    {
        aeron_subscription_link_t *link = &conductor->spy_subscriptions.array[i];
        aeron_udp_channel_t *udp_channel = link->spy_channel;

        if (stream_id == link->stream_id &&
            strncmp(original_uri, udp_channel->original_uri, udp_channel->uri_length) == 0)
        {
            num += link->subscribable_list.length;
        }
    }

    return num;
}

inline aeron_send_channel_endpoint_t * aeron_driver_conductor_find_send_channel_endpoint(
    aeron_driver_conductor_t *conductor, const char *original_uri)
{
    for (size_t i = 0, length = conductor->send_channel_endpoints.length; i < length; i++)
    {
        aeron_send_channel_endpoint_t *endpoint = conductor->send_channel_endpoints.array[i].endpoint;
        aeron_udp_channel_t *udp_channel = endpoint->conductor_fields.udp_channel;

        if (strncmp(original_uri, udp_channel->original_uri, udp_channel->uri_length) == 0)
        {
            return endpoint;
        }
    }

    return NULL;
}

inline aeron_receive_channel_endpoint_t * aeron_driver_conductor_find_receive_channel_endpoint(
    aeron_driver_conductor_t *conductor, const char *original_uri)
{
    for (size_t i = 0, length = conductor->receive_channel_endpoints.length; i < length; i++)
    {
        aeron_receive_channel_endpoint_t *endpoint = conductor->receive_channel_endpoints.array[i].endpoint;
        aeron_udp_channel_t *udp_channel = endpoint->conductor_fields.udp_channel;

        if (strncmp(original_uri, udp_channel->original_uri, udp_channel->uri_length) == 0)
        {
            return endpoint;
        }
    }

    return NULL;
}

inline aeron_ipc_publication_t * aeron_driver_conductor_find_ipc_publication(
    aeron_driver_conductor_t *conductor, int64_t id)
{
    for (size_t i = 0, length = conductor->ipc_publications.length; i < length; i++)
    {
        aeron_ipc_publication_t *publication = conductor->ipc_publications.array[i].publication;

        if (id == publication->conductor_fields.managed_resource.registration_id)
        {
            return publication;
        }
    }

    return NULL;
}

inline aeron_network_publication_t * aeron_driver_conductor_find_network_publication(
    aeron_driver_conductor_t *conductor, int64_t id)
{
    for (size_t i = 0, length = conductor->network_publications.length; i < length; i++)
    {
        aeron_network_publication_t *publication = conductor->network_publications.array[i].publication;

        if (id == publication->conductor_fields.managed_resource.registration_id)
        {
            return publication;
        }
    }

    return NULL;
}

inline aeron_publication_image_t * aeron_driver_conductor_find_publication_image(
    aeron_driver_conductor_t *conductor, aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id)
{
    for (size_t i = 0, length = conductor->publication_images.length; i < length; i++)
    {
        aeron_publication_image_t *image = conductor->publication_images.array[i].image;

        if (endpoint == image->endpoint && stream_id == image->stream_id)
        {
            return image;
        }
    }

    return NULL;
}

inline void aeron_driver_init_subscription_channel(
    int32_t uri_length, const char *uri, aeron_subscription_link_t *link)
{
    size_t copy_length = sizeof(link->channel) - 1;
    copy_length = (size_t)uri_length < copy_length ? (size_t)uri_length : copy_length;

    strncpy(link->channel, uri, copy_length);
    link->channel[copy_length] = '\0';
    link->channel_length = (int32_t)copy_length;
}

#endif //AERON_DRIVER_CONDUCTOR_H
