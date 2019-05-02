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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include "protocol/aeron_udp_protocol.h"
#include "util/aeron_netutil.h"
#include "util/aeron_arrayutil.h"
#include "media/aeron_udp_destination_tracker.h"

#if !defined(HAVE_STRUCT_MMSGHDR)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif

int aeron_udp_destination_tracker_init(
    aeron_udp_destination_tracker_t *tracker, aeron_clock_func_t clock, int64_t timeout_ns)
{
    tracker->nano_clock = clock;
    tracker->destination_timeout_ns = timeout_ns;
    tracker->destinations.array = NULL;
    tracker->destinations.length = 0;
    tracker->destinations.capacity = 0;
    tracker->is_manual_control_mode =
        timeout_ns == AERON_UDP_DESTINATION_TRACKER_MANUAL_DESTINATION_TIMEOUT_NS ? true : false;

    return 0;
}

int aeron_udp_destination_tracker_close(aeron_udp_destination_tracker_t *tracker)
{
    if (NULL != tracker)
    {
        aeron_free(tracker->destinations.array);
    }

    return 0;
}

int aeron_udp_destination_tracker_sendmmsg(
    aeron_udp_destination_tracker_t *tracker,
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *mmsghdr, size_t vlen)
{
    int64_t now_ns = tracker->nano_clock();
    int min_msgs_sent = (int)vlen;

    for (int last_index = (int)tracker->destinations.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_udp_destination_entry_t *entry = &tracker->destinations.array[i];

        if (now_ns > (entry->time_of_last_activity_ns + tracker->destination_timeout_ns))
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
            for (size_t j = 0; j < vlen; j++)
            {
                mmsghdr[j].msg_hdr.msg_name = &entry->addr;
                mmsghdr[j].msg_hdr.msg_namelen = AERON_ADDR_LEN(&entry->addr);
                mmsghdr[j].msg_len = 0;
            }

            const int sendmmsg_result = aeron_udp_channel_transport_sendmmsg(transport, mmsghdr, vlen);

            min_msgs_sent = sendmmsg_result < min_msgs_sent ? sendmmsg_result : min_msgs_sent;
        }
    }

    return min_msgs_sent;
}

int aeron_udp_destination_tracker_sendmsg(
    aeron_udp_destination_tracker_t *tracker, aeron_udp_channel_transport_t *transport, struct msghdr *msghdr)
{
    int64_t now_ns = tracker->nano_clock();
    int min_bytes_sent = (int)msghdr->msg_iov->iov_len;

    for (int last_index = (int)tracker->destinations.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_udp_destination_entry_t *entry = &tracker->destinations.array[i];

        if (now_ns > (entry->time_of_last_activity_ns + tracker->destination_timeout_ns))
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
            msghdr->msg_name = &entry->addr;
            msghdr->msg_namelen = AERON_ADDR_LEN(&entry->addr);

            const int sendmsg_result = aeron_udp_channel_transport_sendmsg(transport, msghdr);

            min_bytes_sent = sendmsg_result < min_bytes_sent ? sendmsg_result : min_bytes_sent;
        }
    }

    return min_bytes_sent;
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

int aeron_udp_destination_tracker_on_status_message(
    aeron_udp_destination_tracker_t *tracker, const uint8_t *buffer, size_t len, struct sockaddr_storage *addr)
{
    int result = 0;

    if (tracker->destination_timeout_ns > 0)
    {
        aeron_status_message_header_t *status_message_header = (aeron_status_message_header_t *)buffer;
        const int64_t now_ns = tracker->nano_clock();
        const int64_t receiver_id = status_message_header->receiver_id;
        bool is_existing = false;

        for (size_t i = 0, size = tracker->destinations.length; i < size; i++)
        {
            aeron_udp_destination_entry_t *entry = &tracker->destinations.array[i];

            if (receiver_id == entry->receiver_id && aeron_udp_destination_tracker_same_port(&entry->addr, addr))
            {
                entry->time_of_last_activity_ns = now_ns;
                is_existing = true;
                break;
            }
        }

        if (!is_existing)
        {
            result = aeron_udp_destination_tracker_add_destination(tracker, receiver_id, now_ns, addr);
        }
    }

    return result;
}

int aeron_udp_destination_tracker_add_destination(
    aeron_udp_destination_tracker_t *tracker, int64_t receiver_id, int64_t now_ns, struct sockaddr_storage *addr)
{
    int result = 0;

    AERON_ARRAY_ENSURE_CAPACITY(result, tracker->destinations, aeron_udp_destination_entry_t);
    if (result >= 0)
    {
        aeron_udp_destination_entry_t *entry = &tracker->destinations.array[tracker->destinations.length++];

        entry->receiver_id = receiver_id;
        entry->time_of_last_activity_ns = now_ns;
        memcpy(&entry->addr, addr, sizeof(struct sockaddr_storage));
    }

    return result;
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
    aeron_udp_destination_tracker_t *tracker, struct sockaddr_storage *addr)
{
    for (int last_index = (int) tracker->destinations.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_udp_destination_entry_t *entry = &tracker->destinations.array[i];

        if (aeron_udp_destination_tracker_address_compare(&entry->addr, addr) == 0)
        {
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
