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

#include "util/aeron_platform.h"

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#if defined(AERON_COMPILER_MSVC)
#include <winsock2.h>
#include <inaddr.h>
#define in_addr_t ULONG
#else
#include <unistd.h>
#endif
#include "protocol/aeron_udp_protocol.h"
#include "util/aeron_error.h"
#include "util/aeron_arrayutil.h"
#include "aeron_name_resolver.h"
#include "aeron_driver_context.h"
#include "media/aeron_udp_channel_transport_bindings.h"
#include "media/aeron_udp_channel_transport.h"
#include "media/aeron_udp_transport_poller.h"
#include "util/aeron_netutil.h"
#include "aeron_name_resolver_cache.h"
#include "aeron_driver_name_resolver.h"

#if !defined(HAVE_STRUCT_MMSGHDR)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif

// Cater for windows.
#define AERON_MAX_HOSTNAME_LEN (256)
#define AERON_NAME_RESOLVER_DRIVER_DUTY_CYCLE_MS (10)
#define AERON_NAME_RESOLVER_DRIVER_NUM_RECV_BUFFERS (1)

typedef struct aeron_driver_name_resolver_neighbor_stct
{
    aeron_name_resolver_cache_addr_t cache_addr;
    int64_t time_of_last_activity_ms;
    struct sockaddr_storage socket_addr;
}
aeron_driver_name_resolver_neighbor_t;

typedef struct aeron_driver_name_resolver_stct
{
    const char *name;
    size_t name_length;
    struct sockaddr_storage local_socket_addr;
    aeron_name_resolver_cache_addr_t local_cache_addr;
    const char *bootstrap_neighbor;
    struct sockaddr_storage bootstrap_neighbor_addr;
    unsigned int interface_index;
    aeron_udp_channel_transport_bindings_t *transport_bindings;
    aeron_name_resolver_t bootstrap_resolver;
    aeron_udp_channel_data_paths_t data_paths;
    aeron_udp_channel_transport_t transport;
    aeron_udp_transport_poller_t poller;
    aeron_name_resolver_cache_t cache;
    struct neighbours_stct
    {
        size_t length;
        size_t capacity;
        aeron_driver_name_resolver_neighbor_t *array;
    }
    neighbors;

    int64_t self_resolution_interval_ms;
    int64_t neighbor_resolution_interval_ms;
    int64_t neighbor_timeout_ms;

    int64_t time_of_last_work_ms;
    int64_t time_of_last_bootstrap_neighbor_resolve_ms;
    int64_t dead_line_self_resolutions_ms;
    int64_t dead_line_neighbor_resolutions_ms;

    int64_t now_ms;

    int64_t *invalid_packets_counter;
    int64_t *short_sends_counter;
    int64_t* error_counter;

    aeron_position_t neighbor_counter;
    aeron_position_t cache_size_counter;
    aeron_distinct_error_log_t *error_log;

    uint8_t buffer[AERON_MAX_UDP_PAYLOAD_BUFFER_LENGTH];

    aeron_udp_channel_recv_buffers_t recv_buffers;
}
aeron_driver_name_resolver_t;

void aeron_driver_name_resolver_receive(
    aeron_udp_channel_data_paths_t *data_paths,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

static int aeron_driver_name_resolver_from_sockaddr(
    struct sockaddr_storage *addr,
    aeron_name_resolver_cache_addr_t *cache_addr);

int aeron_driver_name_resolver_init(
    aeron_driver_name_resolver_t **driver_resolver,
    aeron_driver_context_t *context,
    const char *name,
    const char *interface_name,
    const char *bootstrap_neighbor)
{
    aeron_driver_name_resolver_t *_driver_resolver = NULL;
    char *local_hostname = NULL;

    if (aeron_alloc((void **)&_driver_resolver, sizeof(aeron_driver_name_resolver_t)) < 0)
    {
        aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
        goto error_cleanup;
    }

    aeron_udp_channel_recv_buffers_init(&(_driver_resolver->recv_buffers), AERON_DRIVER_MAX_UDP_PACKET_LENGTH);
    _driver_resolver->recv_buffers.count = AERON_NAME_RESOLVER_DRIVER_NUM_RECV_BUFFERS;

    _driver_resolver->name = name;
    if (NULL == _driver_resolver->name)
    {
        if (aeron_alloc((void **)&local_hostname, AERON_MAX_HOSTNAME_LEN) < 0)
        {
            goto error_cleanup;
        }

        if (gethostname(local_hostname, AERON_MAX_HOSTNAME_LEN) < 0)
        {
            aeron_set_err(errno, "Failed to lookup: %s", local_hostname);
            goto error_cleanup;
        }

        _driver_resolver->name = local_hostname;
    }
    _driver_resolver->name_length = strlen(_driver_resolver->name);

    if (aeron_find_unicast_interface(
        AF_INET, interface_name, &_driver_resolver->local_socket_addr, &_driver_resolver->interface_index) < 0)
    {
        goto error_cleanup;
    }

    if (aeron_driver_name_resolver_from_sockaddr(
        &_driver_resolver->local_socket_addr, &_driver_resolver->local_cache_addr) < 0)
    {
        goto error_cleanup;
    }

    // TODO: Make fallback configurable...
    if (aeron_default_name_resolver_supplier(&_driver_resolver->bootstrap_resolver, NULL, context) < 0)
    {
        goto error_cleanup;
    }

    _driver_resolver->bootstrap_neighbor = bootstrap_neighbor;
    if (NULL != _driver_resolver->bootstrap_neighbor)
    {
        if (aeron_name_resolver_resolve_host_and_port(
            &_driver_resolver->bootstrap_resolver,
            _driver_resolver->bootstrap_neighbor,
            "bootstrap_neighbor",
            false,
            &_driver_resolver->bootstrap_neighbor_addr) < 0)
        {
            goto error_cleanup;
        }
    }

    _driver_resolver->transport_bindings = context->udp_channel_transport_bindings;
    if (aeron_udp_channel_data_paths_init(
        &_driver_resolver->data_paths,
        context->udp_channel_outgoing_interceptor_bindings,
        context->udp_channel_incoming_interceptor_bindings,
        _driver_resolver->transport_bindings,
        aeron_driver_name_resolver_receive,
        AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_CONDUCTOR) < 0)
    {
        goto error_cleanup;
    }

    if (_driver_resolver->transport_bindings->init_func(
        &_driver_resolver->transport,
        &_driver_resolver->local_socket_addr,
        NULL, // Unicast only.
        _driver_resolver->interface_index,
        0,
        context->socket_rcvbuf,
        context->socket_sndbuf,
        context,
        AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_CONDUCTOR) < 0)
    {
        goto error_cleanup;
    }

    _driver_resolver->transport.data_paths = &_driver_resolver->data_paths;

    if (_driver_resolver->transport_bindings->poller_init_func(
        &_driver_resolver->poller, context, AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_CONDUCTOR) < 0)
    {
        goto error_cleanup;
    }

    if (_driver_resolver->transport_bindings->poller_add_func(
        &_driver_resolver->poller, &_driver_resolver->transport) < 0)
    {
        goto error_cleanup;
    }

    aeron_name_resolver_cache_init(&_driver_resolver->cache, AERON_NAME_RESOLVER_DRIVER_TIMEOUT_MS);

    int64_t now_ms = aeron_clock_cached_epoch_time(context->cached_clock);
    _driver_resolver->neighbor_timeout_ms = AERON_NAME_RESOLVER_DRIVER_TIMEOUT_MS;
    _driver_resolver->self_resolution_interval_ms = AERON_NAME_RESOLVER_DRIVER_SELF_RESOLUTION_INTERVAL_MS;
    _driver_resolver->dead_line_self_resolutions_ms = 0;
    _driver_resolver->neighbor_resolution_interval_ms = AERON_NAME_RESOLVER_DRIVER_NEIGHBOUR_RESOLUTION_INTERVAL_MS;
    _driver_resolver->dead_line_neighbor_resolutions_ms = now_ms + _driver_resolver->neighbor_resolution_interval_ms;
    _driver_resolver->time_of_last_bootstrap_neighbor_resolve_ms = now_ms;
    _driver_resolver->time_of_last_work_ms = 0;

    _driver_resolver->neighbor_counter.counter_id = aeron_counters_manager_allocate(
        context->counters_manager,
        AERON_COUNTER_NAME_RESOLVER_NEIGHBORS_COUNTER_TYPE_ID,
        NULL, 0,
        "Resolver neighbors", strlen("Resolver neighbors"));
    if (_driver_resolver->neighbor_counter.counter_id < 0)
    {
        goto error_cleanup;
    }

    _driver_resolver->neighbor_counter.value_addr = aeron_counters_manager_addr(
    context->counters_manager, _driver_resolver->neighbor_counter.counter_id);
    char cache_entries_label[512];
    snprintf(
        cache_entries_label, sizeof(cache_entries_label), "Resolver cache entries: name %s", _driver_resolver->name);
    _driver_resolver->cache_size_counter.counter_id = aeron_counters_manager_allocate(
        context->counters_manager,
        AERON_COUNTER_NAME_RESOLVER_CACHE_ENTRIES_COUNTER_TYPE_ID,
        NULL, 0,
        cache_entries_label, strlen(cache_entries_label));
    if (_driver_resolver->cache_size_counter.counter_id < 0)
    {
        goto error_cleanup;
    }

    _driver_resolver->cache_size_counter.value_addr = aeron_counters_manager_addr(
    context->counters_manager, _driver_resolver->cache_size_counter.counter_id);

    _driver_resolver->short_sends_counter = aeron_system_counter_addr(
        context->system_counters, AERON_SYSTEM_COUNTER_SHORT_SENDS);
    _driver_resolver->invalid_packets_counter = aeron_system_counter_addr(
        context->system_counters, AERON_SYSTEM_COUNTER_INVALID_PACKETS);
    _driver_resolver->error_counter = aeron_system_counter_addr(
        context->system_counters, AERON_SYSTEM_COUNTER_ERRORS);

    _driver_resolver->error_log = context->error_log;

    *driver_resolver = _driver_resolver;
    return 0;

error_cleanup:
    context->udp_channel_transport_bindings->poller_close_func(&_driver_resolver->poller);
    context->udp_channel_transport_bindings->close_func(&_driver_resolver->transport);
    aeron_udp_channel_data_paths_delete(&_driver_resolver->data_paths);
    aeron_name_resolver_cache_close(&_driver_resolver->cache);
    aeron_free((void *)local_hostname);
    aeron_free((void *)_driver_resolver);
    return -1;
}

int aeron_driver_name_resolver_close(aeron_name_resolver_t *resolver)
{
    aeron_driver_name_resolver_t *driver_resolver = (aeron_driver_name_resolver_t *)resolver->state;
    driver_resolver->transport_bindings->poller_close_func(&driver_resolver->poller);
    driver_resolver->transport_bindings->close_func(&driver_resolver->transport);
    aeron_udp_channel_data_paths_delete(&driver_resolver->data_paths);
    aeron_name_resolver_cache_close(&driver_resolver->cache);
    aeron_free(driver_resolver->neighbors.array);
    aeron_free(driver_resolver);
    return 0;
}

static int aeron_driver_name_resolver_to_sockaddr(
    aeron_name_resolver_cache_addr_t *cache_addr,
    struct sockaddr_storage *addr)
{
    int result = -1;
    if (cache_addr->res_type == AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD)
    {
        struct sockaddr_in6 *addr_in6 = (struct sockaddr_in6 *)addr;
        addr_in6->sin6_family = AF_INET6;
        addr_in6->sin6_port = htons(cache_addr->port);
        memcpy(&addr_in6->sin6_addr, cache_addr->address, sizeof(addr_in6->sin6_addr));
        result = 0;
    }
    else if (cache_addr->res_type == AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD)
    {
        struct sockaddr_in *addr_in = (struct sockaddr_in *)addr;
        addr_in->sin_family = AF_INET;
        addr_in->sin_port = htons(cache_addr->port);
        memcpy(&addr_in->sin_addr, cache_addr->address, sizeof(addr_in->sin_addr));
        result = 0;
    }
    else
    {
        aeron_set_err(EINVAL, "Invalid res_type: %d", cache_addr->res_type);
    }

    return result;
}

static int aeron_driver_name_resolver_from_sockaddr(
    struct sockaddr_storage *addr,
    aeron_name_resolver_cache_addr_t *cache_addr)
{
    int result = -1;
    if (addr->ss_family == AF_INET6)
    {
        struct sockaddr_in6 *addr_in6 = (struct sockaddr_in6 *)addr;
        cache_addr->res_type = AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD;
        cache_addr->port = ntohs(addr_in6->sin6_port);
        memcpy(cache_addr->address, &addr_in6->sin6_addr, sizeof(addr_in6->sin6_addr));
        result = 0;
    }
    else if (addr->ss_family == AF_INET)
    {
        struct sockaddr_in *addr_in = (struct sockaddr_in *)addr;
        cache_addr->res_type = AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD;
        cache_addr->port = ntohs(addr_in->sin_port);
        memcpy(cache_addr->address, &addr_in->sin_addr, sizeof(addr_in->sin_addr));
        result = 0;
    }
    else
    {
        aeron_set_err(EINVAL, "Invalid address family: %d", addr->ss_family);
    }

    return result;
}

static uint16_t aeron_driver_name_resolver_get_port(aeron_driver_name_resolver_t *resolver)
{
    uint16_t port = resolver->local_socket_addr.ss_family == AF_INET6 ?
        ((struct sockaddr_in6 *)&resolver->local_socket_addr)->sin6_port :
        ((struct sockaddr_in *)&resolver->local_socket_addr)->sin_port;
    return ntohs(port);
}

static int aeron_driver_name_resolver_find_neighbor_by_addr(
    aeron_driver_name_resolver_t *resolver,
    aeron_name_resolver_cache_addr_t *cache_addr)
{
    for (size_t i = 0; i < resolver->neighbors.length; i++)
    {
        aeron_driver_name_resolver_neighbor_t *neighbor = &resolver->neighbors.array[i];

        if (cache_addr->res_type == neighbor->cache_addr.res_type &&
            cache_addr->port == neighbor->cache_addr.port &&
            0 == memcmp(
                cache_addr->address,
                neighbor->cache_addr.address,
                aeron_res_header_address_length(cache_addr->res_type)))
        {
            return (int)i;
        }
    }

    return -1;
}

static int aeron_driver_name_resolver_add_neighbor(
    aeron_driver_name_resolver_t *resolver,
    aeron_name_resolver_cache_addr_t *cache_addr,
    bool is_self,
    int64_t time_of_last_activity)
{
    const int neighbor_index = aeron_driver_name_resolver_find_neighbor_by_addr(resolver, cache_addr);
    if (neighbor_index < 0)
    {
        int ensure_capacity_result = 0;
        AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, resolver->neighbors, aeron_driver_name_resolver_neighbor_t)
        if (ensure_capacity_result < 0)
        {
            aeron_set_err_from_last_err_code(
                "Failed to allocate rows for neighbors table (%" PRIu32 ",%" PRIu32 ") - %s:%d",
                (uint32_t)resolver->neighbors.length, (uint32_t)resolver->neighbors.capacity, __FILE__, __LINE__);

            return ensure_capacity_result;
        }

        aeron_driver_name_resolver_neighbor_t *new_neighbor = &resolver->neighbors.array[resolver->neighbors.length];
        if (aeron_driver_name_resolver_to_sockaddr(cache_addr, &new_neighbor->socket_addr) < 0)
        {
            return -1;
        }

        memcpy(&new_neighbor->cache_addr, cache_addr, sizeof(new_neighbor->cache_addr));
        new_neighbor->time_of_last_activity_ms = time_of_last_activity;
        resolver->neighbors.length++;
        aeron_counter_set_ordered(resolver->neighbor_counter.value_addr, (int64_t)resolver->neighbors.length);

        return 1;
    }
    else if (is_self)
    {
        resolver->neighbors.array[neighbor_index].time_of_last_activity_ms = time_of_last_activity;

        return 2;
    }

    return 0;
}

static int aeron_driver_name_resolver_on_resolution_entry(
    aeron_driver_name_resolver_t *resolver,
    const aeron_resolution_header_t *resolution_header,
    const char *name,
    size_t name_length,
    aeron_name_resolver_cache_addr_t *cache_addr,
    bool is_self,
    int64_t now_ms)
{
    // Ignore own records that match me...
    if (cache_addr->port == aeron_driver_name_resolver_get_port(resolver) &&
        name_length && resolver->name_length &&
        0 == strncmp(resolver->name, name, name_length))
    {
        return 0;
    }

    int64_t time_of_last_activity = now_ms - resolution_header->age_in_ms;

    if (aeron_name_resolver_cache_add_or_update(
        &resolver->cache,
        name,
        name_length,
        cache_addr,
        time_of_last_activity,
        resolver->cache_size_counter.value_addr) < 0)
    {
        return -1;
    }

    if (aeron_driver_name_resolver_add_neighbor(resolver, cache_addr, is_self, time_of_last_activity) < 0)
    {
        return -1;
    }

    return 1;
}

static bool aeron_driver_name_resolver_is_wildcard(int8_t res_type, uint8_t *address)
{
    in_addr_t ipv4_wildcard = INADDR_ANY;
    return
        (res_type == AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD && 0 == memcmp(address, &in6addr_any, sizeof(in6addr_any))) ||
        0 == memcmp(address, &ipv4_wildcard, sizeof(ipv4_wildcard));
}

static void aeron_name_resolver_log_and_clear_error(aeron_driver_name_resolver_t *resolver)
{
    aeron_distinct_error_log_record(resolver->error_log, AERON_ERROR_CODE_GENERIC_ERROR, aeron_errmsg(), "");
    aeron_counter_increment(resolver->error_counter, 1);
    aeron_set_err(0, "%s", "no error");
}

void aeron_driver_name_resolver_receive(
    aeron_udp_channel_data_paths_t *data_paths,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
    aeron_driver_name_resolver_t *resolver = receiver_clientd;
    aeron_frame_header_t *frame_header = (aeron_frame_header_t *)buffer;
    size_t remaining = length;

    if ((remaining < sizeof(aeron_frame_header_t)) || (frame_header->version != AERON_FRAME_HEADER_VERSION))
    {
        aeron_counter_increment(resolver->invalid_packets_counter, 1);
        return;
    }

    remaining -= sizeof(aeron_frame_header_t);

    while (0 < remaining)
    {
        const size_t offset = length - remaining;
        if (AERON_HDR_TYPE_RES != frame_header->type || remaining < sizeof(aeron_resolution_header_t))
        {
            aeron_counter_increment(resolver->invalid_packets_counter, 1);
            return;
        }

        aeron_resolution_header_t *resolution_header = (aeron_resolution_header_t *)&buffer[offset];
        aeron_name_resolver_cache_addr_t cache_addr;
        const char *name;
        size_t name_length;
        size_t entry_length;

        cache_addr.res_type = resolution_header->res_type;
        cache_addr.port = resolution_header->udp_port;

        if (AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD == resolution_header->res_type)
        {
            aeron_resolution_header_ipv4_t *ip4_hdr = (aeron_resolution_header_ipv4_t *)resolution_header;
            if (length < sizeof(aeron_resolution_header_ipv4_t) ||
                length < (entry_length = aeron_res_header_entry_length_ipv4(ip4_hdr)))
            {
                aeron_counter_increment(resolver->invalid_packets_counter, 1);
                return;
            }

            memcpy(cache_addr.address, ip4_hdr->addr, sizeof(ip4_hdr->addr));
            name_length = (size_t)ip4_hdr->name_length;
            name = (const char *)(ip4_hdr + 1);
        }
        else if (AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD == resolution_header->res_type)
        {
            aeron_resolution_header_ipv6_t *ip6_hdr = (aeron_resolution_header_ipv6_t *)resolution_header;
            if (length < sizeof(aeron_resolution_header_ipv6_t) ||
                length < (entry_length = aeron_res_header_entry_length_ipv6(ip6_hdr)))
            {
                aeron_counter_increment(resolver->invalid_packets_counter, 1);
                return;
            }

            memcpy(cache_addr.address, ip6_hdr->addr, sizeof(ip6_hdr->addr));
            name_length = (size_t)ip6_hdr->name_length;
            name = (const char *)(ip6_hdr + 1);
        }
        else
        {
            aeron_set_err(-1, "Invalid res type on entry: %d", resolution_header->res_type);
            aeron_name_resolver_log_and_clear_error(resolver);
            return;
        }

        const bool is_self = AERON_RES_HEADER_SELF_FLAG == (resolution_header->res_flags & AERON_RES_HEADER_SELF_FLAG);

        if (is_self && aeron_driver_name_resolver_is_wildcard(cache_addr.res_type, cache_addr.address))
        {
            if (aeron_driver_name_resolver_from_sockaddr(addr, &cache_addr) < 0)
            {
                aeron_set_err(
                    -1, "Failed to replace wildcard with source addr: %d, %s", addr->ss_family, aeron_errmsg());
                aeron_name_resolver_log_and_clear_error(resolver);

                return;
            }
        }

        if (aeron_driver_name_resolver_on_resolution_entry(
            resolver, resolution_header, name, name_length, &cache_addr, is_self, resolver->now_ms) < 0)
        {
            aeron_set_err(-1, "Failed to handle resolution entry: %s", aeron_errmsg());
            aeron_name_resolver_log_and_clear_error(resolver);
        }

        remaining -= entry_length;
    }
}

static int aeron_driver_name_resolver_poll(aeron_driver_name_resolver_t *resolver)
{
    int64_t bytes_received = 0;
    int poll_result = resolver->transport_bindings->poller_poll_func(
        &resolver->poller,
        &resolver->recv_buffers,
        &bytes_received,
        resolver->data_paths.recv_func,
        resolver->transport_bindings->recvmmsg_func,
        resolver);

    if (poll_result < 0)
    {
        aeron_set_err(poll_result, "Failed to poll in driver name resolver: %s", aeron_errmsg());
        aeron_name_resolver_log_and_clear_error(resolver);
    }

    return bytes_received > 0 ? (int)bytes_received : 0;
}

static bool aeron_driver_name_resolver_sockaddr_equals(struct sockaddr_storage *a, struct sockaddr_storage *b)
{
    if (a->ss_family != b->ss_family)
    {
        return false;
    }

    if (AF_INET == a->ss_family)
    {
        struct sockaddr_in *a_in = (struct sockaddr_in*) a;
        struct sockaddr_in *b_in = (struct sockaddr_in*) b;

        return a_in->sin_addr.s_addr == b_in->sin_addr.s_addr && a_in->sin_port == b_in->sin_port;
    }
    else if (AF_INET6 == a->ss_family)
    {
        struct sockaddr_in6 *a_in = (struct sockaddr_in6*) a;
        struct sockaddr_in6 *b_in = (struct sockaddr_in6*) b;

        return 0 == memcmp(&a_in->sin6_addr, &b_in->sin6_addr, sizeof(a_in->sin6_addr)) &&
            a_in->sin6_port == b_in->sin6_port;
    }

    return false;
}

static int aeron_driver_name_resolver_do_send(
    aeron_driver_name_resolver_t *resolver,
    aeron_frame_header_t *frame_header,
    ssize_t length,
    struct sockaddr_storage *neighbor_address)
{
    struct iovec iov[1];
    struct msghdr msghdr;

    iov[0].iov_base = frame_header;
    iov[0].iov_len = (uint32_t)length;
    msghdr.msg_iov = iov;
    msghdr.msg_iovlen = 1;
    msghdr.msg_flags = 0;
    msghdr.msg_name = neighbor_address;
    msghdr.msg_namelen = AERON_ADDR_LEN(neighbor_address);
    msghdr.msg_control = NULL;
    msghdr.msg_controllen = 0;

    int send_result = resolver->data_paths.sendmsg_func(&resolver->data_paths, &resolver->transport, &msghdr);
    if (send_result >= 0)
    {
        if ((size_t)send_result != iov[0].iov_len)
        {
            aeron_counter_increment(resolver->short_sends_counter, 1);
        }
    }
    else
    {
        aeron_set_err(errno, "Failed to send resolution frames: %s", aeron_errmsg());
    }

    return send_result;
}

static int aeron_driver_name_resolver_send_self_resolutions(aeron_driver_name_resolver_t *resolver, int64_t now_ms)
{
    uint8_t *aligned_buffer = (uint8_t *)AERON_ALIGN((uintptr_t)resolver->buffer, AERON_CACHE_LINE_LENGTH);

    if (NULL == resolver->bootstrap_neighbor && 0 == resolver->neighbors.length)
    {
        return 0;
    }

    const size_t entry_offset = sizeof(aeron_frame_header_t);

    aeron_frame_header_t *frame_header = (aeron_frame_header_t *)&aligned_buffer[0];
    aeron_resolution_header_t *resolution_header = (aeron_resolution_header_t *)&aligned_buffer[entry_offset];

    const size_t name_length = resolver->name_length; // TODO: cache name length

    int entry_length = aeron_driver_name_resolver_set_resolution_header(
        resolution_header,
        AERON_MAX_UDP_PAYLOAD_LENGTH - entry_offset,
        AERON_RES_HEADER_SELF_FLAG,
        &resolver->local_cache_addr,
        resolver->name,
        name_length);

    assert(0 <= entry_length || "local_cache_addr should of been correctly constructed during init");

    size_t frame_length = sizeof(aeron_frame_header_t) + (size_t)entry_length;

    frame_header->type = AERON_HDR_TYPE_RES;
    frame_header->flags = UINT8_C(0);
    frame_header->version = AERON_FRAME_HEADER_VERSION;
    frame_header->frame_length = (int32_t)frame_length;
    resolution_header->age_in_ms = 0;

    struct sockaddr_storage neighbor_sock_addr;

    bool send_to_bootstrap = NULL != resolver->bootstrap_neighbor;
    int send_work = 0;
    // TODO: Optimise with sendmmsg
    for (size_t k = 0; k < resolver->neighbors.length; k++)
    {
        aeron_driver_name_resolver_neighbor_t *neighbor = &resolver->neighbors.array[k];

        aeron_driver_name_resolver_to_sockaddr(&neighbor->cache_addr, &neighbor_sock_addr);

        if (aeron_driver_name_resolver_do_send(resolver, frame_header, frame_length, &neighbor_sock_addr) < 0)
        {
            aeron_set_err_from_last_err_code("Self resolution to neighbor: %s", aeron_errmsg());
            aeron_name_resolver_log_and_clear_error(resolver);
        }
        else
        {
            send_work++;
        }

        if (send_to_bootstrap &&
            aeron_driver_name_resolver_sockaddr_equals(&resolver->bootstrap_neighbor_addr, &neighbor_sock_addr))
        {
            send_to_bootstrap = false;
        }
    }

    if (send_to_bootstrap)
    {
        if (resolver->time_of_last_bootstrap_neighbor_resolve_ms + AERON_NAME_RESOLVER_DRIVER_TIMEOUT_MS <= now_ms)
        {
            if (aeron_name_resolver_resolve_host_and_port(
                &resolver->bootstrap_resolver, resolver->bootstrap_neighbor, "bootstrap_neighbor", false,
                &resolver->bootstrap_neighbor_addr) < 0)
            {
                aeron_set_err(
                    -1, "failed to resolve bootstrap neighbor (%s), %s", resolver->bootstrap_neighbor, aeron_errmsg());
                return send_work;
            }

            resolver->time_of_last_bootstrap_neighbor_resolve_ms = now_ms;
        }

        if (aeron_driver_name_resolver_do_send(resolver, frame_header, frame_length, &resolver->bootstrap_neighbor_addr) < 0)
        {
            aeron_set_err_from_last_err_code("Self resolution to bootstrap: %s", aeron_errmsg());
            aeron_name_resolver_log_and_clear_error(resolver);
        }
        else
        {
            send_work++;
        }
    }

    return send_work;
}

static int aeron_driver_name_resolver_send_neighbor_resolutions(aeron_driver_name_resolver_t *resolver, int64_t now_ms)
{
    uint8_t *aligned_buffer = (uint8_t *)AERON_ALIGN((uintptr_t)resolver->buffer, AERON_CACHE_LINE_LENGTH);

    aeron_frame_header_t *frame_header = (aeron_frame_header_t *) aligned_buffer;
    frame_header->type = AERON_HDR_TYPE_RES;
    frame_header->flags = UINT8_C(0);
    frame_header->version = AERON_FRAME_HEADER_VERSION;

    size_t i;
    size_t j;
    int work_count = 0;
    for (i = 0; i < resolver->cache.entries.length;)
    {
        size_t entry_offset = sizeof(aeron_frame_header_t);

        for (j = i; j < resolver->cache.entries.length;)
        {
            aeron_resolution_header_t *resolution_header = (aeron_resolution_header_t *)&aligned_buffer[entry_offset];
            aeron_name_resolver_cache_entry_t *cache_entry = &resolver->cache.entries.array[j];

            int entry_length = aeron_driver_name_resolver_set_resolution_header(
                resolution_header,
                AERON_MAX_UDP_PAYLOAD_LENGTH - entry_offset,
                0,
                &cache_entry->cache_addr,
                cache_entry->name,
                cache_entry->name_length);

            assert(-1 != entry_length && "Invalid res_type crept in from somewhere");

            resolution_header->age_in_ms = (int32_t)(now_ms - cache_entry->time_of_last_activity_ms);

            if (0 == entry_length)
            {
                break;
            }

            entry_offset += entry_length;
            j++;
        }

        frame_header->frame_length = (int32_t)entry_offset;

        // TODO: Optimise with sendmmsg
        for (size_t k = 0; k < resolver->neighbors.length; k++)
        {
            aeron_driver_name_resolver_neighbor_t *neighbor = &resolver->neighbors.array[k];

            if (aeron_driver_name_resolver_do_send(resolver, frame_header, entry_offset, &neighbor->socket_addr) < 0)
            {
                aeron_set_err_from_last_err_code("Neighbor resolutions: %s", aeron_errmsg());
                aeron_distinct_error_log_record(
                    resolver->error_log, AERON_ERROR_CODE_GENERIC_ERROR, aeron_errmsg(), "");
            }
            else
            {
                work_count++;
            }
        }

        i = j;
    }

    return work_count;
}

static int aeron_driver_name_resolver_timeout_neighbors(aeron_driver_name_resolver_t *resolver, int64_t now_ms)
{
    int num_removed = 0;
    for (int last_index = (int)resolver->neighbors.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_driver_name_resolver_neighbor_t *entry = &resolver->neighbors.array[i];

        if ((entry->time_of_last_activity_ms + resolver->neighbor_timeout_ms) <= now_ms)
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *)resolver->neighbors.array, sizeof(aeron_name_resolver_cache_entry_t), i, last_index);
            resolver->neighbors.length--;
            last_index--;
            num_removed++;
        }
    }

    if (0 != num_removed)
    {
        aeron_counter_set_ordered(resolver->neighbor_counter.value_addr, resolver->neighbors.length);
    }

    return num_removed;
}

int aeron_driver_name_resolver_set_resolution_header_from_sockaddr(
    aeron_resolution_header_t *resolution_header,
    size_t capacity,
    uint8_t flags,
    struct sockaddr_storage *addr,
    const char *name,
    size_t name_length)
{
    aeron_name_resolver_cache_addr_t cache_addr;

    if (aeron_driver_name_resolver_from_sockaddr(addr, &cache_addr) < 0)
    {
        return -1;
    }

    return aeron_driver_name_resolver_set_resolution_header(
        resolution_header,
        capacity,
        flags,
        &cache_addr,
        name,
        name_length);
}

int aeron_driver_name_resolver_set_resolution_header(
    aeron_resolution_header_t *resolution_header,
    size_t capacity,
    uint8_t flags,
    aeron_name_resolver_cache_addr_t *cache_addr,
    const char *name,
    size_t name_length)
{
    size_t name_offset;
    size_t entry_length;

    switch (cache_addr->res_type)
    {
        case AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD:
            entry_length = AERON_ALIGN(sizeof(aeron_resolution_header_ipv4_t) + name_length, sizeof(int64_t));
            if (capacity < entry_length)
            {
                return 0;
            }

            aeron_resolution_header_ipv4_t *hdr_ipv4 = (aeron_resolution_header_ipv4_t *) resolution_header;
            memcpy(&hdr_ipv4->addr, cache_addr->address, sizeof(hdr_ipv4->addr));
            hdr_ipv4->name_length = (int16_t)name_length;
            name_offset = sizeof(aeron_resolution_header_ipv4_t);

            break;

        case AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD:
            entry_length = AERON_ALIGN(sizeof(aeron_resolution_header_ipv6_t) + name_length, sizeof(int64_t));
            if (capacity < entry_length)
            {
                return 0;
            }

            aeron_resolution_header_ipv6_t *hdr_ipv6 = (aeron_resolution_header_ipv6_t *) resolution_header;

            memcpy(&hdr_ipv6->addr, cache_addr->address, sizeof(hdr_ipv6->addr));
            hdr_ipv6->name_length = (int16_t)name_length;
            name_offset = sizeof(aeron_resolution_header_ipv6_t);

            break;

        default:
            return -1;
    }

    resolution_header->res_type = cache_addr->res_type;
    resolution_header->udp_port = cache_addr->port;
    resolution_header->res_flags = flags;

    uint8_t *buffer = (uint8_t *)resolution_header;
    memcpy(&buffer[name_offset], name, name_length);

    return (int)entry_length;
}

int aeron_driver_name_resolver_resolve(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    struct sockaddr_storage *sock_addr)
{
    aeron_driver_name_resolver_t *driver_resolver = resolver->state;

    const int8_t res_type = sock_addr->ss_family == AF_INET6 ?
        AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD : AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD;

    aeron_name_resolver_cache_entry_t *cache_entry;
    if (aeron_name_resolver_cache_lookup_by_name(
        &driver_resolver->cache, name, strlen(name), res_type, &cache_entry) < 0)
    {
        if (0 == strncmp(name, driver_resolver->name, driver_resolver->name_length + 1))
        {
            memcpy(sock_addr, &driver_resolver->local_socket_addr, sizeof(struct sockaddr_storage));
            return 0;
        }
        else
        {
            return driver_resolver->bootstrap_resolver.resolve_func(
                &driver_resolver->bootstrap_resolver, name, uri_param_name, is_re_resolution, sock_addr);
        }
    }

    return aeron_driver_name_resolver_to_sockaddr(&cache_entry->cache_addr, sock_addr);
}

int aeron_driver_name_resolver_do_work(aeron_name_resolver_t *resolver, int64_t now_ms)
{
    aeron_driver_name_resolver_t *driver_resolver = resolver->state;
    driver_resolver->now_ms = now_ms;
    int work_count = 0;

    if ((driver_resolver->time_of_last_work_ms + AERON_NAME_RESOLVER_DRIVER_DUTY_CYCLE_MS) <= now_ms)
    {
        work_count += aeron_driver_name_resolver_poll(driver_resolver);
        work_count += aeron_name_resolver_cache_timeout_old_entries(
            &driver_resolver->cache, now_ms, driver_resolver->cache_size_counter.value_addr);
        work_count += aeron_driver_name_resolver_timeout_neighbors(driver_resolver, now_ms);

        if (driver_resolver->dead_line_self_resolutions_ms <= now_ms)
        {
            work_count += aeron_driver_name_resolver_send_self_resolutions(driver_resolver, now_ms);

            driver_resolver->dead_line_self_resolutions_ms = now_ms + driver_resolver->self_resolution_interval_ms;
        }

        if (driver_resolver->dead_line_neighbor_resolutions_ms <= now_ms)
        {
            work_count += aeron_driver_name_resolver_send_neighbor_resolutions(driver_resolver, now_ms);

            driver_resolver->dead_line_neighbor_resolutions_ms = now_ms + driver_resolver->neighbor_resolution_interval_ms;
        }

        driver_resolver->time_of_last_work_ms = now_ms;
    }

    return work_count;
}

int aeron_driver_name_resolver_supplier(
    aeron_name_resolver_t *resolver,
    const char *args,
    aeron_driver_context_t *context)
{
    aeron_driver_name_resolver_t *name_resolver = NULL;

    resolver->state = NULL;
    if (aeron_driver_name_resolver_init(
        &name_resolver, context,
        context->resolver_name,
        context->resolver_interface,
        context->resolver_bootstrap_neighbor) < 0)
    {
        return -1;
    }

    resolver->lookup_func = aeron_default_name_resolver_lookup;
    resolver->resolve_func = aeron_driver_name_resolver_resolve;
    resolver->do_work_func = aeron_driver_name_resolver_do_work;
    resolver->close_func = aeron_driver_name_resolver_close;

    resolver->state = name_resolver;

    return 0;
}
