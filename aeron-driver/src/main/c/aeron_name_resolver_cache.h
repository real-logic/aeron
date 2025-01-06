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

#ifndef AERON_AERON_NAME_RESOLVER_CACHE_H
#define AERON_AERON_NAME_RESOLVER_CACHE_H

#include "protocol/aeron_udp_protocol.h"

typedef struct aeron_name_resolver_cache_addr_stct
{
    uint8_t address[AERON_RES_HEADER_ADDRESS_LENGTH_IP6];
    uint16_t port;
    int8_t res_type;
}
aeron_name_resolver_cache_addr_t;

typedef struct aeron_name_resolver_cache_entry_stct
{
    aeron_name_resolver_cache_addr_t cache_addr;
    int64_t deadline_ms;
    int64_t time_of_last_activity_ms;
    size_t name_length;
    const char *name;
}
aeron_name_resolver_cache_entry_t;

typedef struct aeron_name_resolver_cache_stct
{
    int64_t timeout_ms;
    struct entry_stct
    {
        size_t length;
        size_t capacity;
        aeron_name_resolver_cache_entry_t *array;
    }
    entries;
}
aeron_name_resolver_cache_t;

int aeron_name_resolver_cache_init(aeron_name_resolver_cache_t *cache, int64_t timeout_ms);

int aeron_name_resolver_cache_add_or_update(
    aeron_name_resolver_cache_t *cache,
    const char *name,
    size_t name_length,
    aeron_name_resolver_cache_addr_t *cache_addr,
    int64_t time_of_last_activity_ms,
    volatile int64_t *cache_entries_counter);

int aeron_name_resolver_cache_lookup_by_name(
    aeron_name_resolver_cache_t *cache,
    const char *name,
    size_t name_length,
    int8_t res_type,
    aeron_name_resolver_cache_entry_t **entry);

int aeron_name_resolver_cache_close(aeron_name_resolver_cache_t *cache);

int aeron_name_resolver_cache_timeout_old_entries(
    aeron_name_resolver_cache_t *cache, int64_t now_ms, volatile int64_t *cache_entries_counter);

#endif //AERON_AERON_NAME_RESOLVER_CACHE_H
