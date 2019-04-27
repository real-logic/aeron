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

#ifndef AERON_UDP_DESTINATION_TRACKER_H
#define AERON_UDP_DESTINATION_TRACKER_H

#include "aeron_socket.h"
#include "aeronmd.h"
#include "aeron_udp_channel_transport.h"

#define AERON_UDP_DESTINATION_TRACKER_DESTINATION_TIMEOUT_NS (5 * 1000 * 1000 * 1000L)
#define AERON_UDP_DESTINATION_TRACKER_MANUAL_DESTINATION_TIMEOUT_NS (0L)

typedef struct aeron_udp_destination_entry_stct
{
    int64_t time_of_last_activity_ns;
    int64_t receiver_id;
    struct sockaddr_storage addr;
}
aeron_udp_destination_entry_t;

typedef struct aeron_udp_destination_tracker_stct
{
    struct aeron_udp_destination_tracker_destinations_stct
    {
        aeron_udp_destination_entry_t *array;
        size_t length;
        size_t capacity;
    }
    destinations;

    bool is_manual_control_mode;
    aeron_clock_func_t nano_clock;
    int64_t destination_timeout_ns;
}
aeron_udp_destination_tracker_t;

int aeron_udp_destination_tracker_init(
    aeron_udp_destination_tracker_t *tracker, aeron_clock_func_t clock, int64_t timeout_ns);
int aeron_udp_destination_tracker_close(aeron_udp_destination_tracker_t *tracker);

int aeron_udp_destination_tracker_sendmmsg(
    aeron_udp_destination_tracker_t *tracker, aeron_udp_channel_transport_t *transport, struct mmsghdr *mmsghdr, size_t vlen);
int aeron_udp_destination_tracker_sendmsg(
    aeron_udp_destination_tracker_t *tracker, aeron_udp_channel_transport_t *transport, struct msghdr *msghdr);

int aeron_udp_destination_tracker_on_status_message(
    aeron_udp_destination_tracker_t *tracker, const uint8_t *buffer, size_t len, struct sockaddr_storage *addr);

int aeron_udp_destination_tracker_add_destination(
    aeron_udp_destination_tracker_t *tracker, int64_t receiver_id, int64_t now_ns, struct sockaddr_storage *addr);
int aeron_udp_destination_tracker_remove_destination(
    aeron_udp_destination_tracker_t *tracker, struct sockaddr_storage *addr);

#endif //AERON_UDP_DESTINATION_TRACKER_H
