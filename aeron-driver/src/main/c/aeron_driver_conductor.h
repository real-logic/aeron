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

#ifndef AERON_AERON_DRIVER_CONDUCTOR_H
#define AERON_AERON_DRIVER_CONDUCTOR_H

#include "aeron_driver_common.h"
#include "aeron_driver_context.h"
#include "concurrent/aeron_mpsc_rb.h"
#include "concurrent/aeron_broadcast_transmitter.h"
#include "concurrent/aeron_distinct_error_log.h"
#include "concurrent/aeron_counters_manager.h"
#include "command/aeron_control_protocol.h"
#include "aeron_system_counters.h"
#include "aeron_ipc_publication.h"

#define AERON_DRIVER_CONDUCTOR_TIMEOUT_CHECK_NS (1 * 1000 * 1000 * 1000)

typedef struct aeron_publication_link_stct
{
    aeron_driver_managed_resource_t *resource;
}
aeron_publication_link_t;

typedef struct aeron_client_stct
{
    int64_t client_id;
    int64_t client_liveness_timeout_ns;
    int64_t time_of_last_keepalive;
    bool reached_end_of_life;

    struct publication_link_stct
    {
        aeron_publication_link_t *array;
        size_t length;
        size_t capacity;
    }
    publication_links;
}
aeron_client_t;

typedef struct aeron_subscribeable_list_entry_stct
{
    aeron_subscribeable_t *subscribeable;
    int64_t counter_id;
}
aeron_subscribeable_list_entry_t;

typedef struct aeron_subscription_link_stct
{
    /* TODO: channel */
    int32_t stream_id;
    int64_t client_id;
    int64_t registration_id;

    struct subscribeable_list_stct
    {
        aeron_subscribeable_list_entry_t *array;
        size_t length;
        size_t capacity;
    }
    subscribeable_list;
}
aeron_subscription_link_t;

typedef struct aeron_ipc_publication_entry_stct
{
    aeron_ipc_publication_t *publication;
}
aeron_ipc_publication_entry_t;

typedef struct aeron_driver_conductor_stct aeron_driver_conductor_t;

typedef struct aeron_driver_conductor_stct
{
    aeron_driver_context_t *context;
    aeron_mpsc_rb_t to_driver_commands;
    aeron_broadcast_transmitter_t to_clients;
    aeron_distinct_error_log_t error_log;
    aeron_counters_manager_t counters_manager;
    aeron_system_counters_t system_counters;

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

    int64_t *errors_counter;
    int64_t *client_keep_alives_counter;

    aeron_clock_func_t nano_clock;
    aeron_clock_func_t epoch_clock;

    int64_t time_of_last_timeout_check_ns;
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

int aeron_driver_conductor_init(aeron_driver_conductor_t *conductor, aeron_driver_context_t *context);

void aeron_driver_conductor_client_transmit(
    aeron_driver_conductor_t *conductor,
    int32_t msg_type_id,
    const void *message,
    size_t length);

void aeron_driver_conductor_on_unavailable_image(
    aeron_driver_conductor_t *conductor,
    int64_t correlation_id,
    int32_t stream_id,
    const char *channel,
    size_t channel_length);

void aeron_driver_conductor_on_command(int32_t msg_type_id, const void *message, size_t length, void *clientd);

int aeron_driver_conductor_do_work(void *clientd);
void aeron_driver_conductor_on_close(void *clientd);

void aeron_driver_conductor_unlink_subscribeable(aeron_subscription_link_t *link, aeron_subscribeable_t *subscribeable);
void aeron_driver_conductor_unlink_all_subscribeable(aeron_driver_conductor_t *conductor, aeron_subscription_link_t *link);

int aeron_driver_conductor_on_add_ipc_publication(
    aeron_driver_conductor_t *conductor,
    aeron_publication_command_t *command,
    bool is_exclusive);

int aeron_driver_conductor_on_add_network_publication(
    aeron_driver_conductor_t *conductor,
    aeron_publication_command_t *command,
    bool is_exclusive);

int aeron_driver_conductor_on_remove_publication(
    aeron_driver_conductor_t *conductor,
    aeron_remove_command_t *command);

int aeron_driver_conductor_on_add_ipc_subscription(
    aeron_driver_conductor_t *conductor,
    aeron_subscription_command_t *command);

int aeron_driver_conductor_on_add_spy_subscription(
    aeron_driver_conductor_t *conductor,
    aeron_subscription_command_t *command);

int aeron_driver_conductor_on_add_network_subscription(
    aeron_driver_conductor_t *conductor,
    aeron_subscription_command_t *command);

int aeron_driver_conductor_on_remove_subscription(
    aeron_driver_conductor_t *conductor,
    aeron_remove_command_t *command);

int aeron_driver_conductor_on_client_keepalive(
    aeron_driver_conductor_t *conductor,
    int64_t client_id);

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

inline size_t aeron_driver_conductor_num_active_ipc_subscriptions(aeron_driver_conductor_t *conductor, int32_t stream_id)
{
    size_t num = 0;

    for (size_t i = 0, length = conductor->ipc_subscriptions.length; i < length; i++)
    {
        aeron_subscription_link_t *link = &conductor->ipc_subscriptions.array[i];

        if (stream_id == link->stream_id)
        {
            num += link->subscribeable_list.length;
        }
    }

    return num;
}

inline aeron_ipc_publication_t *aeron_driver_conductor_find_ipc_publication(
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

#endif //AERON_AERON_DRIVER_CONDUCTOR_H
