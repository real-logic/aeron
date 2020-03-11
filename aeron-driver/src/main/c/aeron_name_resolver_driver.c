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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "protocol/aeron_udp_protocol.h"
#include "util/aeron_error.h"
#include "util/aeron_strutil.h"
#include "util/aeron_arrayutil.h"
#include "aeron_name_resolver.h"
#include "aeron_driver_context.h"
#include "media/aeron_udp_channel_transport_bindings.h"
#include "media/aeron_udp_channel_transport.h"
#include "media/aeron_udp_transport_poller.h"
#include "util/aeron_netutil.h"
#include "aeron_name_resolver_driver_cache.h"
#include "aeron_name_resolver_driver.h"


// Cater for windows.
#define AERON_MAX_HOSTNAME_LEN (256)
#define AERON_NAME_RESOLVER_DRIVER_DUTY_CYCLE_MS (10)
#define AERON_NAME_RESOLVER_DRIVER_NUM_RECV_BUFFERS (1)

typedef struct aeron_name_resolver_driver_stct
{
    const char *name;
    struct sockaddr_storage local_socket_addr;
    const char *bootstrap_neighbor;
    struct sockaddr_storage bootstrap_neighbor_addr;
    unsigned int interface_index;
    aeron_udp_channel_transport_bindings_t *transport_bindings;
    aeron_name_resolver_t bootstrap_resolver;
    aeron_udp_channel_data_paths_t data_paths;
    aeron_udp_channel_transport_t transport;
    aeron_udp_transport_poller_t poller;
    aeron_name_resolver_driver_cache_t cache;

    int64_t time_of_last_work_ms;
    int64_t dead_line_self_resolutions;
    uint8_t buffer[AERON_MAX_UDP_PAYLOAD_LENGTH];  // TODO: Cache alignment??
}
aeron_name_resolver_driver_t;

void aeron_name_resolver_driver_receive(
    aeron_udp_channel_data_paths_t *data_paths,
    void *receiver_clientd,
    void *endpoint_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

int aeron_name_resolver_driver_init(
    aeron_name_resolver_driver_t **driver_resolver,
    aeron_driver_context_t *context,
    const char *name,
    const char *interface_name,
    const char *bootstrap_neighbor)
{
    aeron_name_resolver_driver_t *_driver_resolver = NULL;
    char *local_hostname = NULL;

    if (aeron_alloc((void **)&_driver_resolver, sizeof(aeron_name_resolver_driver_t)) < 0)
    {
        aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
        goto error_cleanup;
    }

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

    if (aeron_find_unicast_interface(
        AF_INET, interface_name, &_driver_resolver->local_socket_addr, &_driver_resolver->interface_index) < 0)
    {
        goto error_cleanup;
    }

    char local_host_name[INET6_ADDRSTRLEN];
    char local_port[NI_MAXSERV];

    getnameinfo(
        (const struct sockaddr *)&_driver_resolver->local_socket_addr, sizeof(struct sockaddr_storage),
        local_host_name, INET6_ADDRSTRLEN,
        local_port, NI_MAXSERV,
        NI_NUMERICHOST | NI_NUMERICSERV);

    printf("Local socket address: %s, port: %s\n", local_host_name, local_port);

    if (aeron_name_resolver_default_supplier(context, &_driver_resolver->bootstrap_resolver, NULL) < 0)
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

    _driver_resolver->time_of_last_work_ms = 0;

    _driver_resolver->transport_bindings = context->udp_channel_transport_bindings;
    if (aeron_udp_channel_data_paths_init(
        &_driver_resolver->data_paths,
        context->udp_channel_outgoing_interceptor_bindings,
        context->udp_channel_incoming_interceptor_bindings,
        _driver_resolver->transport_bindings,
        aeron_name_resolver_driver_receive,
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

    aeron_name_resolver_driver_cache_init(&_driver_resolver->cache);

    *driver_resolver = _driver_resolver;
    return 0;

error_cleanup:
    aeron_free((void *)local_hostname);
    aeron_free((void *)_driver_resolver);
    return -1;
}

int aeron_name_resolver_driver_resolve(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    struct sockaddr_storage *address)
{
    aeron_name_resolver_driver_t *driver_resolver = resolver->state;

    bool is_ipv6 = address->ss_family == AF_INET6;
    int8_t res_type = is_ipv6 ?
        AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD : AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD;
    
    aeron_name_resolver_driver_cache_entry_t *cache_entry;
    if (aeron_name_resolver_driver_cache_lookup(
        &driver_resolver->cache, name, strlen(name), res_type, &cache_entry) < 0)
    {
        return aeron_name_resolver_default_resolve(NULL, name, uri_param_name, is_re_resolution, address);
    }

    if (is_ipv6)
    {
        struct sockaddr_in6 *addr = (struct sockaddr_in6 *)address;
        addr->sin6_port = cache_entry->port;
        memcpy(&addr->sin6_addr, cache_entry->address, sizeof(addr->sin6_addr));
    }
    else
    {
        struct sockaddr_in *addr = (struct sockaddr_in *)address;
        addr->sin_port = cache_entry->port;
        memcpy(&addr->sin_addr, cache_entry->address, sizeof(addr->sin_addr));
    }

    return 0;
}

void aeron_name_resolver_driver_receive(
    aeron_udp_channel_data_paths_t *data_paths,
    void *receiver_clientd,
    void *endpoint_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
    aeron_name_resolver_driver_t *resolver = receiver_clientd;
    aeron_frame_header_t *frame_header = (aeron_frame_header_t *)buffer;

    if ((length < sizeof(aeron_frame_header_t)) || (frame_header->version != AERON_FRAME_HEADER_VERSION))
    {
        // Resolution frames error counter...
//        aeron_counter_increment(receiver->invalid_frames_counter, 1);
        return;
    }

    if (AERON_HDR_TYPE_RES != frame_header->type || length < sizeof(aeron_resolution_header_t))
    {
        // Invalid message counter...
        return;
    }

    aeron_resolution_header_t *resolution_header = (aeron_resolution_header_t *)&buffer[sizeof(aeron_frame_header_t)];

    if (AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD == resolution_header->res_type &&
        sizeof(aeron_resolution_header_ipv4_t) <= length)
    {
        aeron_resolution_header_ipv4_t *ip4_hdr = (aeron_resolution_header_ipv4_t *)resolution_header;
        if (length < sizeof(aeron_resolution_header_ipv4_t) + ip4_hdr->name_length)
        {
            return;
        }

        const char *name = (const char *)(ip4_hdr + 1);

        aeron_name_resolver_driver_cache_add_or_update(
            &resolver->cache,
            name, ip4_hdr->name_length,
            resolution_header->res_type,
            ip4_hdr->addr,
            resolution_header->udp_port);

        printf("Host name: %.*s\n", ip4_hdr->name_length, name);
    }
    else if (AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD == resolution_header->res_type &&
        sizeof(aeron_resolution_header_ipv6_t) <= length)
    {
        aeron_resolution_header_ipv6_t *ip6_hdr = (aeron_resolution_header_ipv6_t *)resolution_header;
        if (length < sizeof(aeron_resolution_header_ipv6_t) + ip6_hdr->name_length)
        {
            return;
        }

        const char *name = (const char *)(ip6_hdr + 1);

        aeron_name_resolver_driver_cache_add_or_update(
            &resolver->cache,
            name, ip6_hdr->name_length,
            resolution_header->res_type,
            ip6_hdr->addr,
            resolution_header->udp_port);

        printf("Host name: %.*s\n", ip6_hdr->name_length, name);
    }
}

int aeron_name_resolver_driver_poll(aeron_name_resolver_driver_t *resolver)
{
    struct mmsghdr mmsghdr[AERON_NAME_RESOLVER_DRIVER_NUM_RECV_BUFFERS];
    struct iovec iov[AERON_NAME_RESOLVER_DRIVER_NUM_RECV_BUFFERS];
    iov[0].iov_base = resolver->buffer;
    iov[0].iov_len = AERON_MAX_UDP_PAYLOAD_LENGTH;

    for (size_t i = 0; i < AERON_NAME_RESOLVER_DRIVER_NUM_RECV_BUFFERS; i++)
    {
        mmsghdr[i].msg_hdr.msg_name = &resolver->local_socket_addr;
        mmsghdr[i].msg_hdr.msg_namelen = sizeof(resolver->local_socket_addr);
        mmsghdr[i].msg_hdr.msg_iov = &iov[i];
        mmsghdr[i].msg_hdr.msg_iovlen = 1;
        mmsghdr[i].msg_hdr.msg_flags = 0;
        mmsghdr[i].msg_hdr.msg_control = NULL;
        mmsghdr[i].msg_hdr.msg_controllen = 0;
        mmsghdr[i].msg_len = 0;
    }

    int64_t bytes_received = 0;
    int poll_result = resolver->transport_bindings->poller_poll_func(
        &resolver->poller,
        mmsghdr,
        AERON_NAME_RESOLVER_DRIVER_NUM_RECV_BUFFERS,
        &bytes_received,
        resolver->data_paths.recv_func,
        resolver->transport_bindings->recvmmsg_func,
        resolver);

    if (poll_result < 0)
    {
        fprintf(stderr, "Failed to poll: %d\n", poll_result);
        // Distinct error log...
    }

    return bytes_received > 0 ? (int)bytes_received : 0;
}

int aeron_name_resolver_driver_send_self_resolutions(aeron_name_resolver_driver_t *resolver, int64_t now_ms)
{
    if (NULL == resolver->bootstrap_neighbor)
    {
        return 0;
    }

    const size_t entry_offset = sizeof(aeron_frame_header_t);

    aeron_frame_header_t *frame_header = (aeron_frame_header_t *) &resolver->buffer[0];
    aeron_resolution_header_t *resolution_header = (aeron_resolution_header_t *)&resolver->buffer[entry_offset];

    const size_t name_length = strlen(resolver->name); // TODO: cache name length

    int entry_length = aeron_name_resolver_driver_set_resolution_header(
        resolution_header,
        sizeof(resolver->buffer) - entry_offset,
        AERON_RES_HEADER_SELF_FLAG,
        &resolver->local_socket_addr,
        resolver->name,
        name_length);

    assert(entry_length > 0 && "Bug! Single message should always fit in buffer.");

    frame_header->type = AERON_HDR_TYPE_RES;
    frame_header->flags = UINT8_C(0);
    frame_header->version = AERON_FRAME_HEADER_VERSION;
    frame_header->frame_length = sizeof(frame_header) + entry_length;

    struct iovec iov[1];
    iov[0].iov_base = frame_header;
    iov[0].iov_len = frame_header->frame_length;
    struct msghdr msghdr;
    msghdr.msg_iov = iov;
    msghdr.msg_iovlen = 1;
    msghdr.msg_flags = 0;
    msghdr.msg_name = &resolver->bootstrap_neighbor_addr;
    msghdr.msg_namelen = sizeof(resolver->bootstrap_neighbor_addr);
    msghdr.msg_control = NULL;
    msghdr.msg_controllen = 0;

    // TODO: Track resolution send errors/short sends.
    int send_result = resolver->transport_bindings->sendmsg_func(&resolver->data_paths, &resolver->transport, &msghdr);

    if (send_result < 0)
    {
        fprintf(stderr, "Error: %s\n", aeron_errmsg());
    }

    return send_result;
}

int aeron_name_resolver_driver_do_work(aeron_name_resolver_t *resolver, int64_t now_ms)
{
    aeron_name_resolver_driver_t *driver_resolver = resolver->state;
    int work_count = 0;

    if ((driver_resolver->time_of_last_work_ms + AERON_NAME_RESOLVER_DRIVER_DUTY_CYCLE_MS) <= now_ms)
    {
        work_count += aeron_name_resolver_driver_poll(driver_resolver);

        if (driver_resolver->dead_line_self_resolutions <= now_ms)
        {
            work_count += aeron_name_resolver_driver_send_self_resolutions(driver_resolver, now_ms);
        }
    }

    return work_count;
}

int aeron_name_resolver_driver_close(aeron_name_resolver_t *resolver)
{
    aeron_free(resolver->state);
    return 0;
}

int aeron_name_resolver_driver_supplier(
    aeron_driver_context_t *context,
    aeron_name_resolver_t *resolver,
    const char *args)
{
    aeron_name_resolver_driver_t *name_resolver = NULL;

    resolver->state = NULL;
    if (aeron_name_resolver_driver_init(
        &name_resolver, context,
        context->resolver_name,
        context->resolver_interface,
        context->resolver_bootstrap_neighbor) < 0)
    {
        return -1;
    }

    resolver->lookup_func = aeron_name_resolver_default_lookup;
    resolver->resolve_func = aeron_name_resolver_driver_resolve;
    resolver->do_work_func = aeron_name_resolver_driver_do_work;
    resolver->close_func = aeron_name_resolver_driver_close;

    resolver->state = name_resolver;

    return 0;
}

int aeron_name_resolver_driver_set_resolution_header(
    aeron_resolution_header_t *resolution_header,
    size_t capacity,
    uint8_t flags,
    struct sockaddr_storage *address,
    const char *name,
    size_t name_length)
{
    size_t name_offset;
    size_t entry_length;

    switch (address->ss_family)
    {
        case AF_INET:
            entry_length = AERON_ALIGN(sizeof(aeron_resolution_header_ipv4_t) + name_length, sizeof(int64_t));
            if (capacity < entry_length)
            {
                return 0;
            }

            aeron_resolution_header_ipv4_t *hdr_ipv4 = (aeron_resolution_header_ipv4_t *) resolution_header;
            struct sockaddr_in *addr_in = (struct sockaddr_in*)address;

            hdr_ipv4->resolution_header.res_type = AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD;
            hdr_ipv4->resolution_header.udp_port = addr_in->sin_port;
            memcpy(&hdr_ipv4->addr, &addr_in->sin_addr, sizeof(hdr_ipv4->addr));
            hdr_ipv4->name_length = name_length;
            name_offset = sizeof(aeron_resolution_header_ipv4_t);

            break;

        case AF_INET6:
            entry_length = AERON_ALIGN(sizeof(aeron_resolution_header_ipv6_t) + name_length, sizeof(int64_t));
            if (capacity < entry_length)
            {
                return 0;
            }

            aeron_resolution_header_ipv6_t *hdr_ipv6 = (aeron_resolution_header_ipv6_t *) resolution_header;
            struct sockaddr_in6 *addr_in6 = (struct sockaddr_in6*)&address;

            hdr_ipv6->resolution_header.res_type = AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD;
            hdr_ipv6->resolution_header.udp_port = addr_in6->sin6_port;
            memcpy(&hdr_ipv6->addr, &addr_in6->sin6_addr, sizeof(hdr_ipv6->addr));
            hdr_ipv6->name_length = name_length;
            name_offset = sizeof(aeron_resolution_header_ipv6_t);

            break;

        default:
            return -1;
    }

    resolution_header->res_flags = flags;

    uint8_t *buffer = (uint8_t *)resolution_header;
    memcpy(&buffer[name_offset], name, name_length);

    return entry_length;
}
