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
#if defined(AERON_COMPILER_MSVC) && defined(AERON_CPU_X64)
#include <io.h>
#else
#include <unistd.h>
#endif

#include "util/aeron_arrayutil.h"
#include "aeron_alloc.h"
#include "media/aeron_udp_transport_poller.h"
#include "aeron_send_channel_endpoint.h"

int aeron_udp_transport_poller_init(
    aeron_udp_transport_poller_t *poller,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity)
{
    poller->transports.array = NULL;
    poller->transports.length = 0;
    poller->transports.capacity = 0;

#if defined(HAVE_EPOLL)
    if ((poller->epoll_fd = epoll_create1(0)) < 0)
    {
        aeron_set_err_from_last_err_code("epoll_create1");
        return -1;
    }
    poller->epoll_events = NULL;
#elif defined(HAVE_POLL) || defined(HAVE_WSAPOLL)
    poller->pollfds = NULL;
#endif

    poller->bindings_clientd = NULL;
    return 0;
}

int aeron_udp_transport_poller_close(aeron_udp_transport_poller_t *poller)
{
    aeron_free(poller->transports.array);
#if defined(HAVE_EPOLL)
    close(poller->epoll_fd);
    aeron_free(poller->epoll_events);
#elif defined(HAVE_POLL) || defined(HAVE_WSAPOLL)
    aeron_free(poller->pollfds);
#endif
    return 0;
}

int aeron_udp_transport_poller_add(aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport)
{
    int ensure_capacity_result = 0;
    size_t old_capacity = poller->transports.capacity, index = poller->transports.length;

    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, poller->transports, aeron_udp_channel_transport_entry_t);
    if (ensure_capacity_result < 0)
    {
        return -1;
    }

    poller->transports.array[index].transport = transport;

#if defined(HAVE_EPOLL)
    size_t new_capacity = poller->transports.capacity;

    if (new_capacity > old_capacity)
    {
        if (aeron_array_ensure_capacity((uint8_t **)&poller->epoll_events, sizeof(struct epoll_event), old_capacity, new_capacity) < 0)
        {
            return -1;
        }
    }

    struct epoll_event event;

    event.data.fd = transport->fd;
    event.data.ptr = transport;
    event.events = EPOLLIN;
    int result = epoll_ctl(poller->epoll_fd, EPOLL_CTL_ADD, transport->fd, &event);
    if (result < 0)
    {
        aeron_set_err_from_last_err_code("epoll_ctl(EPOLL_CTL_ADD)");
        return -1;
    }

#elif defined(HAVE_POLL) || defined(HAVE_WSAPOLL)
    size_t new_capacity = poller->transports.capacity;

    if (new_capacity > old_capacity)
    {
        if (aeron_array_ensure_capacity((uint8_t **)&poller->pollfds, sizeof(struct pollfd), old_capacity, new_capacity) < 0)
        {
            return -1;
        }
    }

    poller->pollfds[index].fd = transport->fd;
    poller->pollfds[index].events = POLLIN;
    poller->pollfds[index].revents = 0;
#endif

    poller->transports.length++;

    return 0;
}

int aeron_udp_transport_poller_remove(aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport)
{
    int index = -1, last_index = (int)poller->transports.length - 1;

    for (int i = last_index; i >= 0; i--)
    {
        if (poller->transports.array[i].transport == transport)
        {
            index = i;
            break;
        }
    }

    if (index >= 0)
    {
        aeron_array_fast_unordered_remove(
            (uint8_t *)poller->transports.array,
            sizeof(aeron_udp_channel_transport_entry_t),
            (size_t)index,
            (size_t)last_index);

#if defined(HAVE_EPOLL)
        aeron_array_fast_unordered_remove(
            (uint8_t *)poller->epoll_events,
            sizeof(struct epoll_event),
            (size_t)index,
            (size_t)last_index);

        struct epoll_event event;

        event.data.fd = transport->fd;
        event.data.ptr = transport;
        event.events = EPOLLIN;
        int result = epoll_ctl(poller->epoll_fd, EPOLL_CTL_DEL, transport->fd, &event);
        if (result < 0)
        {
            aeron_set_err_from_last_err_code("epoll_ctl(EPOLL_CTL_DEL)");
            return -1;
        }

#elif defined(HAVE_POLL) || defined(HAVE_WSAPOLL)
        aeron_array_fast_unordered_remove(
            (uint8_t *)poller->pollfds,
            sizeof(struct pollfd),
            (size_t)index,
            (size_t)last_index);
#endif
        poller->transports.length--;
    }

    return 0;
}

int aeron_udp_transport_poller_poll(
    aeron_udp_transport_poller_t *poller,
    aeron_udp_channel_recv_buffers_t *msgvec,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    aeron_udp_channel_transport_recvmmsg_func_t recvmmsg_func,
    void *clientd)
{
    int work_count = 0;

    if (poller->transports.length <= AERON_UDP_TRANSPORT_POLLER_ITERATION_THRESHOLD)
    {
        for (size_t i = 0, length = poller->transports.length; i < length; i++)
        {
            int recv_result = recvmmsg_func(
                poller->transports.array[i].transport, msgvec, bytes_rcved, recv_func, clientd);
            if (recv_result < 0)
            {
                return recv_result;
            }

            work_count += recv_result;
        }
    }
    else
    {
#if defined(HAVE_EPOLL)
        int result = epoll_wait(poller->epoll_fd, poller->epoll_events, (int)poller->transports.length, 0);

        if (result < 0)
        {
            int err = errno;

            if (EINTR == err || EAGAIN == err)
            {
                return 0;
            }

            aeron_set_err_from_last_err_code("epoll_wait");
            return -1;
        }
        else if (0 == result)
        {
            return 0;
        }
        else
        {
            for (size_t i = 0, length = (size_t)result; i < length; i++)
            {
                if (poller->epoll_events[i].events & EPOLLIN)
                {
                    int recv_result = recvmmsg_func(
                        poller->epoll_events[i].data.ptr, msgvec, bytes_rcved, recv_func, clientd);

                    if (recv_result < 0)
                    {
                        return recv_result;
                    }

                    work_count += recv_result;
                }

                poller->epoll_events[i].events = 0;
            }
        }

#elif defined(HAVE_POLL) || defined(HAVE_WSAPOLL)
        int result = poll(poller->pollfds, (nfds_t)poller->transports.length, 0);

        if (result < 0)
        {
            int err = errno;

            if (EINTR == err || EAGAIN == err)
            {
                return 0;
            }

            aeron_set_err_from_last_err_code("poll");
            return -1;
        }
        else if (0 == result)
        {
            return 0;
        }
        else
        {
            for (size_t i = 0, length = poller->transports.length; i < length; i++)
            {
                if (poller->pollfds[i].revents & POLLIN)
                {
                    int recv_result = recvmmsg_func(
                        poller->transports.array[i].transport, msgvec, bytes_rcved, recv_func, clientd);

                    if (recv_result < 0)
                    {
                        return recv_result;
                    }

                    work_count += recv_result;
                }

                poller->pollfds[i].revents = 0;
            }
        }
#endif
    }

    return work_count;
}

int aeron_udp_transport_poller_check_send_endpoint_re_resolutions(
    aeron_udp_transport_poller_t *poller,
    int64_t now_ns,
    struct aeron_driver_conductor_proxy_stct *conductor_proxy)
{
    for (size_t i = 0; i < poller->transports.length; i++)
    {
        aeron_udp_channel_transport_t *transport = poller->transports.array[i].transport;
        aeron_send_channel_endpoint_t *endpoint = transport->dispatch_clientd;

        aeron_send_channel_endpoint_check_for_re_resolution(endpoint, now_ns, conductor_proxy);
    }

    return 0;
}

int aeron_udp_transport_poller_check_receive_endpoint_re_resolutions(
    aeron_udp_transport_poller_t *poller,
    int64_t now_ns,
    aeron_driver_conductor_proxy_t *conductor_proxy)
{
    for (size_t i = 0; i < poller->transports.length; i++)
    {
        aeron_udp_channel_transport_t *transport = poller->transports.array[i].transport;
        aeron_receive_channel_endpoint_t *endpoint = transport->dispatch_clientd;

        aeron_receive_channel_endpoint_check_for_re_resolution(endpoint, now_ns, conductor_proxy);
    }

    return 0;
}
