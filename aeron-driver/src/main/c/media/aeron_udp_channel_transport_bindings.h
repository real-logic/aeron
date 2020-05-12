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

#ifndef AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_H
#define AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_H

#include <uri/aeron_uri.h>
#include "aeron_socket.h"

#include "aeron_driver_common.h"
#include "util/aeron_bitutil.h"

#define AERON_DRIVER_UDP_NUM_RECV_BUFFERS 2
#define AERON_DRIVER_MAX_UDP_PACKET_LENGTH (64 * 1024)
#define AERON_MAX_UDP_PAYLOAD_BUFFER_LENGTH (AERON_DRIVER_MAX_UDP_PACKET_LENGTH + AERON_CACHE_LINE_LENGTH)

typedef struct aeron_udp_channel_recv_buffers_stct
{
    uint8_t buffers[AERON_MAX_UDP_PAYLOAD_BUFFER_LENGTH * AERON_DRIVER_UDP_NUM_RECV_BUFFERS];
    struct iovec iov[AERON_DRIVER_UDP_NUM_RECV_BUFFERS];
    struct sockaddr_storage addrs[AERON_DRIVER_UDP_NUM_RECV_BUFFERS];
    size_t count;
}
aeron_udp_channel_recv_buffers_t;

#define AERON_DRIVER_UDP_NUM_SEND_BUFFERS 2
typedef struct aeron_udp_channel_send_buffers_stct
{
    struct iovec iov[AERON_DRIVER_UDP_NUM_SEND_BUFFERS];
    struct sockaddr_storage* addrv[AERON_DRIVER_UDP_NUM_SEND_BUFFERS];
    int addr_lenv[AERON_DRIVER_UDP_NUM_SEND_BUFFERS];
    size_t count;
    int bytes_sent;
}
aeron_udp_channel_send_buffers_t;

typedef enum aeron_udp_channel_transport_affinity_en
{
    AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_SENDER,
    AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_RECEIVER,
    AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_CONDUCTOR
}
aeron_udp_channel_transport_affinity_t;

typedef struct aeron_udp_channel_transport_stct aeron_udp_channel_transport_t;
typedef struct aeron_udp_transport_poller_stct aeron_udp_transport_poller_t;
typedef struct aeron_udp_channel_data_paths_stct aeron_udp_channel_data_paths_t;

typedef int (*aeron_udp_channel_transport_init_func_t)(
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *bind_addr,
    struct sockaddr_storage *multicast_if_addr,
    unsigned int multicast_if_index,
    uint8_t ttl,
    size_t socket_rcvbuf,
    size_t socket_sndbuf,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity);

typedef int (*aeron_udp_channel_transport_close_func_t)(aeron_udp_channel_transport_t *transport);

typedef void (*aeron_udp_transport_recv_func_t)(
    aeron_udp_channel_data_paths_t *data_paths,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

typedef int (*aeron_udp_channel_transport_recvmmsg_func_t)(
    aeron_udp_channel_transport_t *transport,
    aeron_udp_channel_recv_buffers_t *msgvec,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd);

typedef int (*aeron_udp_channel_transport_sendmmsg_func_t)(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    aeron_udp_channel_send_buffers_t *send_buffers);

typedef int (*aeron_udp_channel_transport_get_so_rcvbuf_func_t)(
    aeron_udp_channel_transport_t *transport, size_t *so_rcvbuf);

typedef int (*aeron_udp_channel_transport_bind_addr_and_port_func_t)(
    aeron_udp_channel_transport_t *transport, char *buffer, size_t length);

typedef int (*aeron_udp_transport_poller_init_func_t)(
    aeron_udp_transport_poller_t *poller,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity);
typedef int (*aeron_udp_transport_poller_close_func_t)(aeron_udp_transport_poller_t *poller);

typedef int (*aeron_udp_transport_poller_add_func_t)(
    aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport);
typedef int (*aeron_udp_transport_poller_remove_func_t)(
    aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport);

typedef int (*aeron_udp_transport_poller_poll_func_t)(
    aeron_udp_transport_poller_t *poller,
    aeron_udp_channel_recv_buffers_t *msgvec,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    aeron_udp_channel_transport_recvmmsg_func_t recvmmsg_func,
    void *clientd);

typedef struct aeron_udp_channel_transport_bindings_stct aeron_udp_channel_transport_bindings_t;

struct aeron_udp_channel_transport_bindings_stct
{
    aeron_udp_channel_transport_init_func_t init_func;
    aeron_udp_channel_transport_close_func_t close_func;
    aeron_udp_channel_transport_recvmmsg_func_t recvmmsg_func;
    aeron_udp_channel_transport_sendmmsg_func_t sendmmsg_func;
    aeron_udp_channel_transport_get_so_rcvbuf_func_t get_so_rcvbuf_func;
    aeron_udp_channel_transport_bind_addr_and_port_func_t bind_addr_and_port_func;
    aeron_udp_transport_poller_init_func_t poller_init_func;
    aeron_udp_transport_poller_close_func_t poller_close_func;
    aeron_udp_transport_poller_add_func_t poller_add_func;
    aeron_udp_transport_poller_remove_func_t poller_remove_func;
    aeron_udp_transport_poller_poll_func_t poller_poll_func;
    struct meta_info_fields
    {
        const char *name;
        const char *type;
        const aeron_udp_channel_transport_bindings_t *next_binding;
        const void *source_symbol;
    }
    meta_info;
};

aeron_udp_channel_transport_bindings_t *aeron_udp_channel_transport_bindings_load_media(const char *bindings_name);

typedef struct aeron_udp_channel_interceptor_bindings_stct aeron_udp_channel_interceptor_bindings_t;

typedef aeron_udp_channel_interceptor_bindings_t *(aeron_udp_channel_interceptor_bindings_load_func_t)(
    aeron_udp_channel_interceptor_bindings_t *delegate_bindings);

aeron_udp_channel_interceptor_bindings_t *aeron_udp_channel_interceptor_bindings_load(
    aeron_udp_channel_interceptor_bindings_t *existing_interceptor_bindings,
    const char *interceptors);

typedef struct aeron_udp_channel_outgoing_interceptor_stct aeron_udp_channel_outgoing_interceptor_t;
typedef struct aeron_udp_channel_incoming_interceptor_stct aeron_udp_channel_incoming_interceptor_t;

typedef int (*aeron_udp_channel_interceptor_outgoing_mmsg_func_t)(
    void *interceptor_state,
    aeron_udp_channel_outgoing_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    aeron_udp_channel_send_buffers_t *send_buffers);

typedef void (*aeron_udp_channel_interceptor_incoming_func_t)(
    void *interceptor_state,
    aeron_udp_channel_incoming_interceptor_t *delegate,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

typedef int (*aeron_udp_channel_interceptor_init_func_t)(
    void **interceptor_state,
    aeron_udp_channel_transport_affinity_t affinity);

typedef int (*aeron_udp_channel_interceptor_close_func_t)(
    void *interceptor_state);

// TODO: in context
struct aeron_udp_channel_interceptor_bindings_stct
{
    aeron_udp_channel_interceptor_init_func_t outgoing_init_func;
    aeron_udp_channel_interceptor_init_func_t incoming_init_func;
    aeron_udp_channel_interceptor_outgoing_mmsg_func_t outgoing_mmsg_func;
    aeron_udp_channel_interceptor_incoming_func_t incoming_func;
    aeron_udp_channel_interceptor_close_func_t outgoing_close_func;
    aeron_udp_channel_interceptor_close_func_t incoming_close_func;
    struct interceptor_meta_info_fields
    {
        const char *name;
        const char *type;
        const aeron_udp_channel_interceptor_bindings_t *next_interceptor_bindings;
        const void *source_symbol;
    }
    meta_info;
};

struct aeron_udp_channel_outgoing_interceptor_stct
{
    void *interceptor_state;
    aeron_udp_channel_interceptor_outgoing_mmsg_func_t outgoing_mmsg_func;
    aeron_udp_channel_interceptor_close_func_t close_func;
    aeron_udp_channel_outgoing_interceptor_t *next_interceptor;
};

struct aeron_udp_channel_incoming_interceptor_stct
{
    void *interceptor_state;
    aeron_udp_channel_interceptor_incoming_func_t incoming_func;
    aeron_udp_channel_interceptor_close_func_t close_func;
    aeron_udp_channel_incoming_interceptor_t *next_interceptor;
};

// TODO: in sender and receiver to store data paths
struct aeron_udp_channel_data_paths_stct
{
    aeron_udp_channel_outgoing_interceptor_t *outgoing_interceptors;
    aeron_udp_channel_incoming_interceptor_t *incoming_interceptors;
    aeron_udp_channel_transport_sendmmsg_func_t sendmmsg_func;
    aeron_udp_transport_recv_func_t recv_func;
};

inline int aeron_udp_channel_outgoing_interceptor_sendmmsg(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    aeron_udp_channel_send_buffers_t *send_buffers)
{
    aeron_udp_channel_outgoing_interceptor_t *interceptor = data_paths->outgoing_interceptors;

    /* use first interceptor and pass in delegate */
    return interceptor->outgoing_mmsg_func(
        interceptor->interceptor_state, interceptor->next_interceptor, transport, send_buffers);
}

inline int aeron_udp_channel_outgoing_interceptor_mmsg_to_transport(
    void *interceptor_state,
    aeron_udp_channel_outgoing_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    aeron_udp_channel_send_buffers_t *send_buffers)
{
    aeron_udp_channel_transport_sendmmsg_func_t func =
        ((aeron_udp_channel_transport_bindings_t *)interceptor_state)->sendmmsg_func;

    return func(NULL, transport, send_buffers);
}

inline void aeron_udp_channel_incoming_interceptor_recv_func(
    aeron_udp_channel_data_paths_t *data_paths,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
    aeron_udp_channel_incoming_interceptor_t *interceptor = data_paths->incoming_interceptors;

    interceptor->incoming_func(
        interceptor->interceptor_state,
        interceptor->next_interceptor,
        receiver_clientd,
        endpoint_clientd,
        destination_clientd,
        buffer,
        length,
        addr);
}

inline void aeron_udp_channel_incoming_interceptor_to_endpoint(
    void *interceptor_state,
    aeron_udp_channel_incoming_interceptor_t *delegate,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
    aeron_udp_transport_recv_func_t func = (aeron_udp_transport_recv_func_t)interceptor_state;
#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic pop
#endif

    func(NULL, receiver_clientd, endpoint_clientd, destination_clientd, buffer, length, addr);
}

void aeron_udp_channel_recv_buffers_init(aeron_udp_channel_recv_buffers_t *buffers, uint32_t iov_len);

int aeron_udp_channel_data_paths_init(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_interceptor_bindings_t *outgoing_interceptor_bindings,
    aeron_udp_channel_interceptor_bindings_t *incoming_interceptor_bindings,
    aeron_udp_channel_transport_bindings_t *media_bindings,
    aeron_udp_transport_recv_func_t recv_func,
    aeron_udp_channel_transport_affinity_t affinity);

int aeron_udp_channel_data_paths_delete(aeron_udp_channel_data_paths_t *data_paths);

#endif //AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_H
