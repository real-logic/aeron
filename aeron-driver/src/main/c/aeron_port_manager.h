/*
 * Copyright 2014-2025 Real Logic Limited.
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

#ifndef AERON_PORT_MANAGER_H
#define AERON_PORT_MANAGER_H

#include "aeron_driver_common.h"
#include "collections/aeron_int64_counter_map.h"

struct sockaddr_storage;
typedef struct aeron_udp_channel_stct aeron_udp_channel_t;

typedef int (*aeron_port_manager_get_managed_port_func_t)(
    void *state,
    struct sockaddr_storage *bind_addr_out,
    aeron_udp_channel_t *udp_channel,
    struct sockaddr_storage *bind_addr);
typedef void (*aeron_port_manager_free_managed_port_func_t)(void *state, struct sockaddr_storage *bind_addr);

typedef struct aeron_port_manager_stct
{
    aeron_port_manager_get_managed_port_func_t get_managed_port;
    aeron_port_manager_free_managed_port_func_t free_managed_port;
    void *state;
}
aeron_port_manager_t;

typedef struct aeron_wildcard_port_manager_stct {
    aeron_port_manager_t port_manager;
    aeron_int64_counter_map_t port_table;
    uint16_t low_port;
    uint16_t high_port;
    uint16_t next_port;
    bool is_sender;
    bool is_os_wildcard;
}
aeron_wildcard_port_manager_t;

int aeron_wildcard_port_manager_get_managed_port(
    void *state,
    struct sockaddr_storage *bind_addr_out,
    aeron_udp_channel_t *udp_channel,
    struct sockaddr_storage *bind_addr);
void aeron_wildcard_port_manager_free_managed_port(void *state, struct sockaddr_storage *bind_addr);

int aeron_wildcard_port_manager_init(
    aeron_wildcard_port_manager_t *port_manager, bool is_sender);

void aeron_wildcard_port_manager_set_range(
    aeron_wildcard_port_manager_t *port_manager, uint16_t low_port, uint16_t high_port);

void aeron_wildcard_port_manager_delete(aeron_wildcard_port_manager_t *port_manager);
int aeron_parse_port_range(const char *range_str, uint16_t *low_port, uint16_t *high_port);

#endif // AERON_PORT_MANAGER_H
