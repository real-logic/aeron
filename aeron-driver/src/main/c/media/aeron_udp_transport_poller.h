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

#ifndef AERON_UDP_TRANSPORT_POLLER_H
#define AERON_UDP_TRANSPORT_POLLER_H

#if defined(HAVE_EPOLL)
#include <sys/epoll.h>
#elif defined(HAVE_POLL)
#include <poll.h>
#endif

#include "media/aeron_udp_channel_transport.h"

#define AERON_UDP_TRANSPORT_POLLER_ITERATION_THRESHOLD (5)

typedef struct aeron_udp_channel_transport_entry_stct
{
    aeron_udp_channel_transport_t *transport;
}
aeron_udp_channel_transport_entry_t;

typedef struct aeron_udp_transport_poller_stct
{
    struct aeron_udp_channel_transports_stct
    {
        aeron_udp_channel_transport_entry_t *array;
        size_t length;
        size_t capacity;
    }
    transports;

#if defined(HAVE_EPOLL)
    int epoll_fd;
    struct epoll_event *epoll_events;
#elif defined(HAVE_POLL)
    struct pollfd *pollfds;
#endif
}
aeron_udp_transport_poller_t;

int aeron_udp_transport_poller_init(aeron_udp_transport_poller_t *poller);
int aeron_udp_transport_poller_close(aeron_udp_transport_poller_t *poller);

int aeron_udp_transport_poller_add(aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport);
int aeron_udp_transport_poller_remove(aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport);

int aeron_udp_transport_poller_poll(
    aeron_udp_transport_poller_t *poller,
    struct mmsghdr *msgvec,
    size_t vlen,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd);

#endif //AERON_UDP_TRANSPORT_POLLER_H
