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

#include "aeron_driver_context.h"
#include "concurrent/aeron_mpsc_rb.h"
#include "concurrent/aeron_broadcast_transmitter.h"
#include "concurrent/aeron_distinct_error_log.h"
#include "concurrent/aeron_counters_manager.h"
#include "aeron_system_counters.h"

typedef struct aeron_client_stct
{
    int64_t client_id;
    int64_t client_liveness_timeout_ns;
    int64_t time_of_last_keepalive;
    bool reached_end_of_life;
}
aeron_client_t;

typedef struct aeron_publication_link_stct
{
    void *publication;
    int64_t client_id;
    int64_t registration_id;
    int cached_client_index;
    bool reached_end_of_life;
}
aeron_publication_link_t;

typedef void (*aeron_managed_resource_on_time_event_func_t)(void *clientd, int64_t, int64_t);
typedef bool (*aeron_managed_resource_has_reached_end_of_life_func_t)(void *clientd);
typedef void (*aeron_managed_resource_delete_func_t)(void *clientd);

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
        aeron_managed_resource_on_time_event_func_t on_time_event;
        aeron_managed_resource_has_reached_end_of_life_func_t has_reached_end_of_life;
        aeron_managed_resource_delete_func_t delete;
    }
    clients;

    struct publication_links_stct
    {
        aeron_publication_link_t *array;
        size_t length;
        size_t capacity;
        aeron_managed_resource_on_time_event_func_t on_time_event;
        aeron_managed_resource_has_reached_end_of_life_func_t has_reached_end_of_life;
        aeron_managed_resource_delete_func_t delete;
    }
    publication_links;
}
aeron_driver_conductor_t;

int aeron_driver_conductor_init(aeron_driver_conductor_t *conductor, aeron_driver_context_t *context);

void aeron_driver_conductor_client_transmit(
    aeron_driver_conductor_t *conductor,
    int32_t msg_type_id,
    const void *message,
    size_t length);

void aeron_driver_conductor_on_command(int32_t msg_type_id, const void *message, size_t length, void *clientd);

int aeron_driver_conductor_do_work(void *clientd);
void aeron_driver_conductor_on_close(void *clientd);

#endif //AERON_AERON_DRIVER_CONDUCTOR_H
