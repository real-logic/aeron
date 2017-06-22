/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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
#define _GNU_SOURCE
#endif

#include <sys/socket.h>

#if !defined(HAVE_RECVMMSG)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif

#include "util/aeron_arrayutil.h"
#include "media/aeron_send_channel_endpoint.h"
#include "aeron_driver_sender.h"
#include "aeron_driver_conductor_proxy.h"
#include "aeron_network_publication.h"

int aeron_driver_sender_init(
    aeron_driver_sender_t *sender, aeron_driver_context_t *context, aeron_system_counters_t *system_counters)
{
    if (aeron_udp_transport_poller_init(&sender->poller) < 0)
    {
        return -1;
    }

    sender->context = context;
    sender->sender_proxy.sender = sender;
    sender->sender_proxy.command_queue = &context->sender_command_queue;
    sender->sender_proxy.fail_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_SENDER_PROXY_FAILS);
    sender->sender_proxy.threading_mode = context->threading_mode;

    sender->network_publicaitons.array = NULL;
    sender->network_publicaitons.length = 0;
    sender->network_publicaitons.capacity = 0;

    sender->round_robin_index = 0;
    sender->total_bytes_sent =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_BYTES_SENT);
    return 0;
}

void aeron_driver_sender_on_command(void *clientd, volatile void *item)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)item;

    cmd->func(clientd, cmd);

    /* recycle cmd by sending to conductor as on_cmd_free */
    aeron_driver_conductor_proxy_on_delete_cmd(sender->context->conductor_proxy, cmd);
}

int aeron_driver_sender_do_work(void *clientd)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    int work_count = 0;

    work_count +=
        aeron_spsc_concurrent_array_queue_drain(
            sender->sender_proxy.command_queue, aeron_driver_sender_on_command, sender, 10);

    int64_t now_ns = sender->context->nano_clock();
    int bytes_sent = aeron_driver_sender_do_send(sender, now_ns);

    /* TODO: add duty cycle logic to reduce polling */
    {
        /* TODO: add struct mmsghdr and buffers and addrs */
        struct mmsghdr mmsghdr;

        aeron_udp_transport_poller_poll(&sender->poller, &mmsghdr, 1, aeron_send_channel_endpoint_dispatch, sender);
    }

    return work_count;
}

void aeron_driver_sender_on_close(void *clientd)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;

    aeron_udp_transport_poller_close(&sender->poller);
    aeron_free(sender->network_publicaitons.array);
}

void aeron_driver_sender_on_add_endpoint(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)command;
    aeron_send_channel_endpoint_t *endpoint = (aeron_send_channel_endpoint_t *)cmd->item;

    if (aeron_udp_transport_poller_add(&sender->poller, &endpoint->transport) < 0)
    {
        /* TODO: */
    }
}

void aeron_driver_sender_on_remove_endpoint(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)command;
    aeron_send_channel_endpoint_t *endpoint = (aeron_send_channel_endpoint_t *)cmd->item;

    if (aeron_udp_transport_poller_remove(&sender->poller, &endpoint->transport) < 0)
    {
        /* TODO: */
    }
}

void aeron_driver_sender_on_add_publication(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)command;
    aeron_network_publication_t *publication = (aeron_network_publication_t *)cmd->item;

    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(
        ensure_capacity_result, sender->network_publicaitons, sizeof(aeron_driver_sender_network_publication_entry_t));

    if (ensure_capacity_result < 0)
    {
        /* TODO: */
        return;
    }

    sender->network_publicaitons.array[sender->network_publicaitons.length++].publication = publication;
    /* TODO: add to reception map in endpoint for routing SMs, NAKs, etc. */
}

void aeron_driver_sender_on_remove_publication(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)command;
    aeron_network_publication_t *publication = (aeron_network_publication_t *)cmd->item;

    for (size_t i = 0, size = sender->network_publicaitons.length, last_index = size - 1; i < size; i++)
    {
        if (publication == sender->network_publicaitons.array[i].publication)
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *)sender->network_publicaitons.array,
                sizeof(aeron_driver_sender_network_publication_entry_t),
                i,
                last_index);
            sender->network_publicaitons.length--;
        }
    }

    /* TODO: remove from reception map in endpoint for routing SMs, NAKs, etc. */
}

int aeron_driver_sender_do_send(aeron_driver_sender_t *sender, int64_t now_ns)
{
    int bytes_sent = 0;
    aeron_driver_sender_network_publication_entry_t *publications = sender->network_publicaitons.array;
    size_t length = sender->network_publicaitons.length;
    size_t starting_index = sender->round_robin_index;

    if (starting_index >= length)
    {
        sender->round_robin_index = starting_index = 0;
    }

    for (size_t i = starting_index; i < length; i++)
    {
        bytes_sent += aeron_network_publication_send(publications[i].publication, now_ns);
    }

    for (size_t i = 0; i < starting_index; i++)
    {
        bytes_sent += aeron_network_publication_send(publications[i].publication, now_ns);
    }

    aeron_counter_add_ordered(sender->total_bytes_sent, bytes_sent);

    return bytes_sent;
}
