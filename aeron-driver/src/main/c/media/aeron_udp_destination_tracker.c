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

#include <uri/aeron_uri.h>
#include "protocol/aeron_udp_protocol.h"
#include "util/aeron_netutil.h"
#include "util/aeron_arrayutil.h"
#include "media/aeron_send_channel_endpoint.h"
#include "aeron_driver_conductor.h"
#include "media/aeron_udp_destination_tracker.h"

int aeron_udp_destination_tracker_init(
    aeron_udp_destination_tracker_t *tracker,
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_clock_cache_t *cached_clock,
    bool is_manual_control_model,
    int64_t timeout_ns)
{
    tracker->cached_clock = cached_clock;
    tracker->data_paths = data_paths;
    tracker->destination_timeout_ns = timeout_ns;
    tracker->destinations.array = NULL;
    tracker->destinations.length = 0;
    tracker->destinations.capacity = 0;
    tracker->is_manual_control_mode = is_manual_control_model;

    return 0;
}

int aeron_udp_destination_tracker_close(aeron_udp_destination_tracker_t *tracker)
{
    if (NULL != tracker)
    {
        for (size_t i = 0; i < tracker->destinations.length; i++)
        {
            aeron_uri_close(tracker->destinations.array[i].uri);
            aeron_free(tracker->destinations.array[i].uri);
        }

        aeron_free(tracker->destinations.array);
    }

    return 0;
}

int aeron_udp_destination_tracker_sendmmsg(
    aeron_udp_destination_tracker_t *tracker,
    aeron_udp_channel_transport_t *transport,
    aeron_udp_channel_send_buffers_t *send_buffers)
{
    const int64_t now_ns = aeron_clock_cached_nano_time(tracker->cached_clock);
    const bool is_dynamic_control_mode = !tracker->is_manual_control_mode;
    int min_msgs_sent = (int)send_buffers->count;

    for (int last_index = (int)tracker->destinations.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_udp_destination_entry_t *entry = &tracker->destinations.array[i];

        if (is_dynamic_control_mode && now_ns > (entry->time_of_last_activity_ns + tracker->destination_timeout_ns))
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *)tracker->destinations.array,
                sizeof(aeron_udp_destination_entry_t),
                (size_t)i,
                (size_t)last_index);
            last_index--;
            tracker->destinations.length--;
        }
        else
        {
            for (size_t j = 0; j < send_buffers->count; j++)
            {
                send_buffers->addrv[j] = &entry->addr;
                send_buffers->addr_lenv[j] = AERON_ADDR_LEN(&entry->addr);
            }

            const int sendmmsg_result = tracker->data_paths->sendmmsg_func(
                tracker->data_paths, transport, send_buffers);

            min_msgs_sent = sendmmsg_result < min_msgs_sent ? sendmmsg_result : min_msgs_sent;
        }
    }

    return min_msgs_sent;
}

bool aeron_udp_destination_tracker_same_port(struct sockaddr_storage *lhs, struct sockaddr_storage *rhs)
{
    bool result = false;

    if (AF_INET6 == lhs->ss_family)
    {
        struct sockaddr_in6 *lhs_in6_addr = (struct sockaddr_in6 *)lhs;
        struct sockaddr_in6 *rhs_in6_addr = (struct sockaddr_in6 *)rhs;

        return ntohs(lhs_in6_addr->sin6_port) == ntohs(rhs_in6_addr->sin6_port);
    }
    else if (AF_INET == lhs->ss_family)
    {
        struct sockaddr_in *lhs_in_addr = (struct sockaddr_in *)lhs;
        struct sockaddr_in *rhs_in_addr = (struct sockaddr_in *)rhs;

        return ntohs(lhs_in_addr->sin_port) == ntohs(rhs_in_addr->sin_port);
    }

    return result;
}

bool aeron_udp_destination_tracker_same_addr(struct sockaddr_storage *lhs, struct sockaddr_storage *rhs)
{
    bool result = false;

    if (AF_INET6 == lhs->ss_family)
    {
        struct sockaddr_in6 *lhs_in6_addr = (struct sockaddr_in6 *)lhs;
        struct sockaddr_in6 *rhs_in6_addr = (struct sockaddr_in6 *)rhs;

        return 0 == memcmp(&lhs_in6_addr->sin6_addr, &rhs_in6_addr->sin6_addr, sizeof(struct in6_addr));
    }
    else if (AF_INET == lhs->ss_family)
    {
        struct sockaddr_in *lhs_in_addr = (struct sockaddr_in *)lhs;
        struct sockaddr_in *rhs_in_addr = (struct sockaddr_in *)rhs;

        return 0 == memcmp(&lhs_in_addr->sin_addr, &rhs_in_addr->sin_addr, sizeof(struct in_addr));
    }

    return result;
}

int aeron_udp_destination_tracker_add_destination(
    aeron_udp_destination_tracker_t *tracker,
    int64_t receiver_id,
    bool is_receiver_id_valid,
    int64_t now_ns,
    aeron_uri_t *uri,
    struct sockaddr_storage *addr)
{
    int result = 0;

    AERON_ARRAY_ENSURE_CAPACITY(result, tracker->destinations, aeron_udp_destination_entry_t);
    if (result >= 0)
    {
        aeron_udp_destination_entry_t *entry = &tracker->destinations.array[tracker->destinations.length++];

        entry->receiver_id = receiver_id;
        entry->is_receiver_id_valid = is_receiver_id_valid;
        entry->time_of_last_activity_ns = now_ns;
        entry->destination_timeout_ns = AERON_UDP_DESTINATION_TRACKER_DESTINATION_TIMEOUT_NS;
        entry->uri = uri;
        memcpy(&entry->addr, addr, sizeof(struct sockaddr_storage));
    }

    return result;
}

int aeron_udp_destination_tracker_on_status_message(
    aeron_udp_destination_tracker_t *tracker, const uint8_t *buffer, size_t len, struct sockaddr_storage *addr)
{
    const aeron_status_message_header_t *status_message_header = (aeron_status_message_header_t *)buffer;
    const int64_t now_ns = aeron_clock_cached_nano_time(tracker->cached_clock);
    const int64_t receiver_id = status_message_header->receiver_id;
    const bool is_dynamic_control_mode = !tracker->is_manual_control_mode;

    int result = 0;
    bool is_existing = false;

    for (size_t i = 0, size = tracker->destinations.length; i < size; i++)
    {
        aeron_udp_destination_entry_t *entry = &tracker->destinations.array[i];

        if (entry->is_receiver_id_valid && receiver_id == entry->receiver_id &&
            aeron_udp_destination_tracker_same_port(&entry->addr, addr))
        {
            entry->time_of_last_activity_ns = now_ns;
            is_existing = true;
            break;
        }
        else if (!entry->is_receiver_id_valid &&
            aeron_udp_destination_tracker_same_addr(&entry->addr, addr) &&
            aeron_udp_destination_tracker_same_port(&entry->addr, addr))
        {
            entry->time_of_last_activity_ns = now_ns;
            entry->receiver_id = receiver_id;
            entry->is_receiver_id_valid = true;
            is_existing = true;
            break;
        }
    }

    if (is_dynamic_control_mode && !is_existing)
    {
        result = aeron_udp_destination_tracker_add_destination(tracker, receiver_id, true, now_ns, NULL, addr);
    }

    return result;
}

int aeron_udp_destination_tracker_manual_add_destination(
    aeron_udp_destination_tracker_t *tracker,
    int64_t now_ns,
    aeron_uri_t *uri,
    struct sockaddr_storage *addr)
{
    if (!tracker->is_manual_control_mode)
    {
        return 0;
    }

    return aeron_udp_destination_tracker_add_destination(tracker, now_ns, 0, false, uri, addr);
}

int aeron_udp_destination_tracker_address_compare(struct sockaddr_storage *lhs, struct sockaddr_storage *rhs)
{
    if (lhs->ss_family == rhs->ss_family)
    {
        size_t len = (AF_INET == lhs->ss_family) ? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6);

        return memcmp(lhs, rhs, len);
    }

    return 1;
}

int aeron_udp_destination_tracker_remove_destination(
    aeron_udp_destination_tracker_t *tracker,
    struct sockaddr_storage *addr,
    aeron_uri_t **removed_uri)
{
    for (int last_index = (int) tracker->destinations.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_udp_destination_entry_t *entry = &tracker->destinations.array[i];

        if (aeron_udp_destination_tracker_address_compare(&entry->addr, addr) == 0)
        {
            *removed_uri = entry->uri;

            aeron_array_fast_unordered_remove(
                (uint8_t *) tracker->destinations.array,
                sizeof(aeron_udp_destination_entry_t),
                (size_t)i,
                (size_t)last_index);

            tracker->destinations.length--;
            break;
        }
    }

    return 0;
}

void aeron_udp_destination_tracker_check_for_re_resolution(
    aeron_udp_destination_tracker_t *tracker,
    aeron_send_channel_endpoint_t *endpoint,
    int64_t now_ns,
    aeron_driver_conductor_proxy_t *conductor_proxy)
{
    if (!tracker->is_manual_control_mode)
    {
        return;
    }

    for (size_t i = 0; i < tracker->destinations.length; i++)
    {
        aeron_udp_destination_entry_t *destination = &tracker->destinations.array[i];

        if (now_ns > (destination->time_of_last_activity_ns + destination->destination_timeout_ns))
        {
            assert(NULL != destination->uri);

            aeron_driver_conductor_proxy_on_re_resolve_endpoint(
                conductor_proxy, destination->uri->params.udp.endpoint, endpoint, &destination->addr);
            destination->time_of_last_activity_ns = now_ns;
        }
    }
}

void aeron_udp_destination_tracker_resolution_change(
    aeron_udp_destination_tracker_t *tracker, const char *endpoint_name, struct sockaddr_storage *addr)
{
    if (!tracker->is_manual_control_mode)
    {
        return;
    }

    for (size_t i = 0; i < tracker->destinations.length; i++)
    {
        aeron_udp_destination_entry_t *destination = &tracker->destinations.array[i];
        const size_t endpoint_name_len = strlen(endpoint_name);

        if (0 == strncmp(endpoint_name, destination->uri->params.udp.endpoint, endpoint_name_len + 1))
        {
            memcpy(&destination->addr, addr, sizeof(destination->addr));
        }
    }
}
