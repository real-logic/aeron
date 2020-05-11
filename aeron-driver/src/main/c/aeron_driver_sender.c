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

#include <aeron_socket.h>
#include <stdio.h>

#if !defined(HAVE_STRUCT_MMSGHDR)
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

int aeron_driver_sender_init(
    aeron_driver_sender_t *sender,
    aeron_driver_context_t *context,
    aeron_system_counters_t *system_counters,
    aeron_distinct_error_log_t *error_log)
{
    if (context->udp_channel_transport_bindings->poller_init_func(
        &sender->poller, context, AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_SENDER) < 0)
    {
        return -1;
    }

    aeron_udp_channel_recv_buffers_init(&(sender->recv_buffers), AERON_DRIVER_MAX_UDP_PACKET_LENGTH);
    sender->recv_buffers.count = AERON_DRIVER_SENDER_NUM_RECV_BUFFERS;

    if (aeron_udp_channel_data_paths_init(
        &sender->data_paths,
        context->udp_channel_outgoing_interceptor_bindings,
        context->udp_channel_incoming_interceptor_bindings,
        context->udp_channel_transport_bindings,
        aeron_send_channel_endpoint_dispatch,
        AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_SENDER) < 0)
    {
        return -1;
    }

    sender->context = context;
    sender->poller_poll_func = context->udp_channel_transport_bindings->poller_poll_func;
    sender->recvmmsg_func = context->udp_channel_transport_bindings->recvmmsg_func;
    sender->error_log = error_log;
    sender->sender_proxy.sender = sender;
    sender->sender_proxy.command_queue = &context->sender_command_queue;
    sender->sender_proxy.fail_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_SENDER_PROXY_FAILS);
    sender->sender_proxy.threading_mode = context->threading_mode;

    sender->network_publications.array = NULL;
    sender->network_publications.length = 0;
    sender->network_publications.capacity = 0;

    sender->round_robin_index = 0;
    sender->duty_cycle_counter = 0;
    sender->duty_cycle_ratio = context->send_to_sm_poll_ratio;
    sender->status_message_read_timeout_ns = context->status_message_timeout_ns / 2;
    sender->control_poll_timeout_ns = 0;
    sender->total_bytes_sent_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_BYTES_SENT);
    sender->errors_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_ERRORS);
    sender->invalid_frames_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_INVALID_PACKETS);
    sender->status_messages_received_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_STATUS_MESSAGES_RECEIVED);
    sender->nak_messages_received_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_NAK_MESSAGES_RECEIVED);
    sender->resolution_changes_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_RESOLUTION_CHANGES);

    sender->re_resolution_deadline_ns =
        aeron_clock_cached_nano_time(context->cached_clock) + context->re_resolution_check_interval_ns;

    return 0;
}

void aeron_driver_sender_on_command(void *clientd, volatile void *item)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)item;
    bool is_delete_cmd = cmd->func == aeron_command_on_delete_cmd;

    cmd->func(clientd, cmd);

    if (!is_delete_cmd)
    {
        aeron_driver_conductor_proxy_on_delete_cmd(sender->context->conductor_proxy, cmd);
    }
}

int aeron_driver_sender_do_work(void *clientd)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    int work_count = 0;

    work_count += (int)aeron_spsc_concurrent_array_queue_drain(
        sender->sender_proxy.command_queue, aeron_driver_sender_on_command, sender, 10);

    int64_t now_ns = aeron_clock_cached_nano_time(sender->context->cached_clock);
    int64_t bytes_received = 0;
    int bytes_sent = aeron_driver_sender_do_send(sender, now_ns);
    int poll_result;

    if (0 == bytes_sent ||
        ++sender->duty_cycle_counter >= sender->duty_cycle_ratio ||
        now_ns > sender->control_poll_timeout_ns)
    {
        poll_result = sender->poller_poll_func(
            &sender->poller,
            &sender->recv_buffers,
            &bytes_received,
            sender->data_paths.recv_func,
            sender->recvmmsg_func,
            sender);

        if (poll_result < 0)
        {
            AERON_DRIVER_SENDER_ERROR(sender, "sender poller_poll: %s", aeron_errmsg());
        }

        if (sender->context->re_resolution_check_interval_ns > 0 && (sender->re_resolution_deadline_ns - now_ns) < 0)
        {
            aeron_udp_transport_poller_check_send_endpoint_re_resolutions(
                &sender->poller, now_ns, sender->context->conductor_proxy);

            sender->re_resolution_deadline_ns = now_ns + sender->context->re_resolution_check_interval_ns;
        }

        work_count += (poll_result < 0) ? 0 : poll_result;

        sender->duty_cycle_counter = 0;
        sender->control_poll_timeout_ns = now_ns + sender->status_message_read_timeout_ns;
    }

    return work_count + bytes_sent;
}

void aeron_driver_sender_on_close(void *clientd)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;

    aeron_udp_channel_data_paths_delete(&sender->data_paths);

    sender->context->udp_channel_transport_bindings->poller_close_func(&sender->poller);
    aeron_free(sender->network_publications.array);
}

void aeron_driver_sender_on_add_endpoint(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)command;
    aeron_send_channel_endpoint_t *endpoint = (aeron_send_channel_endpoint_t *)cmd->item;

    if (sender->context->udp_channel_transport_bindings->poller_add_func(&sender->poller, &endpoint->transport) < 0)
    {
        AERON_DRIVER_SENDER_ERROR(sender, "sender on_add_endpoint: %s", aeron_errmsg());
    }
}

void aeron_driver_sender_on_remove_endpoint(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)command;
    aeron_send_channel_endpoint_t *endpoint = (aeron_send_channel_endpoint_t *)cmd->item;

    if (sender->context->udp_channel_transport_bindings->poller_remove_func(&sender->poller, &endpoint->transport) < 0)
    {
        AERON_DRIVER_SENDER_ERROR(sender, "sender on_remove_endpoint: %s", aeron_errmsg());
    }

    aeron_send_channel_endpoint_sender_release(endpoint);
}

void aeron_driver_sender_on_add_publication(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)command;
    aeron_network_publication_t *publication = (aeron_network_publication_t *)cmd->item;

    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(
        ensure_capacity_result, sender->network_publications, aeron_driver_sender_network_publication_entry_t);

    if (ensure_capacity_result < 0)
    {
        AERON_DRIVER_SENDER_ERROR(sender, "sender on_add_publication: %s", aeron_errmsg());
        return;
    }

    sender->network_publications.array[sender->network_publications.length++].publication = publication;
    if (aeron_send_channel_endpoint_add_publication(publication->endpoint, publication) < 0)
    {
        AERON_DRIVER_SENDER_ERROR(sender, "sender on_add_publication add_publication: %s", aeron_errmsg());
    }
}

void aeron_driver_sender_on_remove_publication(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)command;
    aeron_network_publication_t *publication = (aeron_network_publication_t *)cmd->item;

    for (size_t i = 0, size = sender->network_publications.length, last_index = size - 1; i < size; i++)
    {
        if (publication == sender->network_publications.array[i].publication)
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *)sender->network_publications.array,
                sizeof(aeron_driver_sender_network_publication_entry_t),
                i,
                last_index);
            sender->network_publications.length--;
            break;
        }
    }

    if (aeron_send_channel_endpoint_remove_publication(publication->endpoint, publication) < 0)
    {
        AERON_DRIVER_SENDER_ERROR(sender, "sender on_remove_publication: %s", aeron_errmsg());
    }

    aeron_network_publication_sender_release(publication);
}

void aeron_driver_sender_on_add_destination(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_destination_t *cmd = (aeron_command_destination_t *)command;

    if (aeron_send_channel_endpoint_add_destination(cmd->endpoint, cmd->uri, &cmd->control_address) < 0)
    {
        AERON_DRIVER_SENDER_ERROR(sender, "sender on_add_destination: %s", aeron_errmsg());
    }
}

void aeron_driver_sender_on_remove_destination(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_destination_t *cmd = (aeron_command_destination_t *)command;
    aeron_uri_t *old_uri = NULL;

    if (aeron_send_channel_endpoint_remove_destination(cmd->endpoint, &cmd->control_address, &old_uri) < 0)
    {
        AERON_DRIVER_SENDER_ERROR(sender, "sender on_remove_destination: %s", aeron_errmsg());
    }

    if (NULL != old_uri)
    {
        aeron_conductor_proxy_on_delete_send_destination(sender->context->conductor_proxy, old_uri);
    }
}

void aeron_driver_sender_on_resolution_change(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = clientd;
    aeron_command_sender_resolution_change_t *resolution_change = (aeron_command_sender_resolution_change_t *)command;
    aeron_send_channel_endpoint_t *endpoint = resolution_change->endpoint;

    aeron_send_channel_endpoint_resolution_change(
        endpoint, resolution_change->endpoint_name, &resolution_change->new_addr);
    aeron_counter_add_ordered(sender->resolution_changes_counter, 1);
}

int aeron_driver_sender_do_send(aeron_driver_sender_t *sender, int64_t now_ns)
{
    int bytes_sent = 0;
    aeron_driver_sender_network_publication_entry_t *publications = sender->network_publications.array;
    size_t length = sender->network_publications.length;
    size_t starting_index = sender->round_robin_index++;

    if (starting_index >= length)
    {
        sender->round_robin_index = starting_index = 0;
    }

    for (size_t i = starting_index; i < length; i++)
    {
        int result = aeron_network_publication_send(publications[i].publication, now_ns);
        if (result < 0)
        {
            AERON_DRIVER_SENDER_ERROR(sender, "sender do_send: %s", aeron_errmsg());
        }
        else
        {
            bytes_sent += result;
        }
    }

    for (size_t i = 0; i < starting_index; i++)
    {
        int result = aeron_network_publication_send(publications[i].publication, now_ns);
        if (result < 0)
        {
            AERON_DRIVER_SENDER_ERROR(sender, "sender do_send: %s", aeron_errmsg());
        }
        else
        {
            bytes_sent += result;
        }
    }

    aeron_counter_add_ordered(sender->total_bytes_sent_counter, bytes_sent);

    return bytes_sent;
}
