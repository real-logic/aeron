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
#include <stdio.h>
#include "util/aeron_arrayutil.h"
#include "media/aeron_receive_channel_endpoint.h"
#include "aeron_driver_receiver.h"
#include "aeron_publication_image.h"

#if !defined(HAVE_RECVMMSG)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif

int aeron_driver_receiver_init(
    aeron_driver_receiver_t *receiver,
    aeron_driver_context_t *context,
    aeron_system_counters_t *system_counters,
    aeron_distinct_error_log_t *error_log)
{
    if (aeron_udp_transport_poller_init(&receiver->poller) < 0)
    {
        return -1;
    }

    for (size_t i = 0; i < AERON_DRIVER_RECEIVER_NUM_RECV_BUFFERS; i++)
    {
        size_t offset = 0;
        if (aeron_alloc_aligned(
            (void **)&receiver->recv_buffers.buffers[i],
            &offset,
            AERON_DRIVER_RECEIVER_MAX_UDP_PACKET_LENGTH,
            AERON_CACHE_LINE_LENGTH * 2) < 0)
        {
            return -1;
        }

        receiver->recv_buffers.iov[i].iov_base = receiver->recv_buffers.buffers[i] + offset;
        receiver->recv_buffers.iov[i].iov_len = AERON_DRIVER_RECEIVER_MAX_UDP_PACKET_LENGTH;
    }

    receiver->images.array = NULL;
    receiver->images.length = 0;
    receiver->images.capacity = 0;

    receiver->pending_setups.array = NULL;
    receiver->pending_setups.length = 0;
    receiver->pending_setups.capacity = 0;

    receiver->context = context;
    receiver->error_log = error_log;

    receiver->receiver_proxy.command_queue = &context->receiver_command_queue;
    receiver->receiver_proxy.fail_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_RECEIVER_PROXY_FAILS);
    receiver->receiver_proxy.threading_mode = context->threading_mode;
    receiver->receiver_proxy.receiver = receiver;

    receiver->errors_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_ERRORS);
    receiver->invalid_frames_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_INVALID_PACKETS);
    return 0;
}

void aeron_driver_receiver_on_command(void *clientd, volatile void *item)
{
    aeron_command_base_t *cmd = (aeron_command_base_t *)item;

    cmd->func(clientd, cmd);
}

int aeron_driver_receiver_do_work(void *clientd)
{
    struct mmsghdr mmsghdr[AERON_DRIVER_RECEIVER_NUM_RECV_BUFFERS];
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    int work_count = 0;

    work_count +=
        aeron_spsc_concurrent_array_queue_drain(
            receiver->receiver_proxy.command_queue, aeron_driver_receiver_on_command, receiver, 10);

    for (size_t i = 0; i < AERON_DRIVER_RECEIVER_NUM_RECV_BUFFERS; i++)
    {
        mmsghdr[i].msg_hdr.msg_name = &receiver->recv_buffers.addrs[i];
        mmsghdr[i].msg_hdr.msg_namelen = sizeof(receiver->recv_buffers.addrs[i]);
        mmsghdr[i].msg_hdr.msg_iov = &receiver->recv_buffers.iov[i];
        mmsghdr[i].msg_hdr.msg_iovlen = 1;
        mmsghdr[i].msg_hdr.msg_flags = 0;
        mmsghdr[i].msg_len = 0;
    }

    int poll_result = aeron_udp_transport_poller_poll(
        &receiver->poller,
        mmsghdr,
        AERON_DRIVER_RECEIVER_NUM_RECV_BUFFERS,
        aeron_receive_channel_endpoint_dispatch,
        receiver);

    if (poll_result < 0)
    {
        AERON_DRIVER_RECEIVER_ERROR(receiver, "receiver poller_poll: %s", aeron_errmsg());
    }

    work_count += (poll_result < 0) ? 0 : poll_result;

    for (size_t i = 0, length = receiver->images.length; i < length; i++)
    {
        aeron_publication_image_t *image = receiver->images.array[i].image;

        int send_sm_result = aeron_publicaion_image_send_pending_status_message(image);
        if (send_sm_result < 0)
        {
            AERON_DRIVER_RECEIVER_ERROR(receiver, "receiver send SM: %s", aeron_errmsg());
        }

        work_count += (send_sm_result < 0) ? 0 : send_sm_result;

        int send_nak_result = aeron_publicaion_image_send_pending_loss(image);
        if (send_nak_result < 0)
        {
            AERON_DRIVER_RECEIVER_ERROR(receiver, "receiver send SM: %s", aeron_errmsg());
        }

        /* TODO: initiate RTTM */

        work_count += (send_nak_result < 0) ? 0 : send_nak_result;
    }

    int64_t now_ns = receiver->context->nano_clock();

    for (int last_index = (int)receiver->pending_setups.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_driver_receiver_pending_setup_entry_t *entry = &receiver->pending_setups.array[i];

        if (now_ns > (entry->time_of_status_message_ns + AERON_DRIVER_RECEIVER_PENDING_SETUP_TIMEOUT_NS))
        {
            /* TODO: handle control SETUP SMs for Multi-Destination-Cast */

            if (aeron_receive_channel_endpoint_on_remove_pending_setup(
                entry->endpoint, entry->session_id, entry->stream_id) < 0)
            {
                AERON_DRIVER_RECEIVER_ERROR(receiver, "receiver pending setups: %s", aeron_errmsg());
            }

            aeron_array_fast_unordered_remove(
                (uint8_t *) receiver->pending_setups.array,
                sizeof(aeron_driver_receiver_pending_setup_entry_t),
                (size_t) i,
                (size_t) last_index);
            last_index--;
            receiver->pending_setups.length--;
        }
    }

    /* TODO: add_ordered total bytes_received */

    return work_count;
}

void aeron_driver_receiver_on_close(void *clientd)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;

    for (size_t i = 0; i < AERON_DRIVER_RECEIVER_NUM_RECV_BUFFERS; i++)
    {
        aeron_free(receiver->recv_buffers.buffers[i]);
    }

    aeron_free(receiver->images.array);
    aeron_free(receiver->pending_setups.array);

    aeron_udp_transport_poller_close(&receiver->poller);
}

void aeron_driver_receiver_on_add_endpoint(void *clientd, void *command)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)command;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)cmd->item;

    if (aeron_udp_transport_poller_add(&receiver->poller, &endpoint->transport) < 0)
    {
        AERON_DRIVER_RECEIVER_ERROR(receiver, "receiver on_add_endpoint: %s", aeron_errmsg());
    }

    aeron_driver_conductor_proxy_on_delete_cmd(receiver->context->conductor_proxy, command);
}

void aeron_driver_receiver_on_remove_endpoint(void *clientd, void *command)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)command;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)cmd->item;

    if (aeron_udp_transport_poller_remove(&receiver->poller, &endpoint->transport) < 0)
    {
        AERON_DRIVER_RECEIVER_ERROR(receiver, "receiver on_remove_endpoint: %s", aeron_errmsg());
    }

    aeron_receive_channel_endpoint_receiver_release(endpoint);
    aeron_driver_conductor_proxy_on_delete_cmd(receiver->context->conductor_proxy, command);
}

void aeron_driver_receiver_on_add_subscription(void *clientd, void *item)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    aeron_command_subscription_t *cmd = (aeron_command_subscription_t *)item;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)cmd->endpoint;

    if (aeron_receive_channel_endpoint_on_add_subscription(endpoint, cmd->stream_id) < 0)
    {
        AERON_DRIVER_RECEIVER_ERROR(receiver, "receiver on_add_subscription: %s", aeron_errmsg());
    }

    aeron_driver_conductor_proxy_on_delete_cmd(receiver->context->conductor_proxy, item);
}

void aeron_driver_receiver_on_remove_subscription(void *clientd, void *item)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    aeron_command_subscription_t *cmd = (aeron_command_subscription_t *)item;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)cmd->endpoint;

    if (aeron_receive_channel_endpoint_on_remove_subscription(endpoint, cmd->stream_id) < 0)
    {
        AERON_DRIVER_RECEIVER_ERROR(receiver, "receiver on_remove_subscription: %s", aeron_errmsg());
    }

    aeron_driver_conductor_proxy_on_delete_cmd(receiver->context->conductor_proxy, item);
}

void aeron_driver_receiver_on_add_publication_image(void *clientd, void *item)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    aeron_command_publication_image_t *cmd = (aeron_command_publication_image_t *)item;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)cmd->endpoint;

    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, receiver->images, aeron_driver_receiver_image_entry_t);

    if (aeron_receive_channel_endpoint_on_add_publication_image(endpoint, cmd->image) < 0 || ensure_capacity_result < 0)
    {
        AERON_DRIVER_RECEIVER_ERROR(receiver, "receiver on_add_publication_image: %s", aeron_errmsg());
    }

    receiver->images.array[receiver->images.length++].image = cmd->image;
    aeron_driver_conductor_proxy_on_delete_cmd(receiver->context->conductor_proxy, item);
}

void aeron_driver_receiver_on_remove_publication_image(void *clientd, void *item)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    aeron_command_publication_image_t *cmd = (aeron_command_publication_image_t *)item;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)cmd->endpoint;

    if (aeron_receive_channel_endpoint_on_remove_publication_image(endpoint, cmd->image) < 0)
    {
        AERON_DRIVER_RECEIVER_ERROR(receiver, "receiver on_remove_publication_image: %s", aeron_errmsg());
    }

    for (size_t i = 0, size = receiver->images.length, last_index = size - 1; i < size; i++)
    {
        if (cmd->image == receiver->images.array[i].image)
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *)receiver->images.array, sizeof(aeron_driver_receiver_image_entry_t), i, last_index);
            receiver->images.length--;
            break;
        }
    }

    aeron_driver_conductor_proxy_on_delete_cmd(receiver->context->conductor_proxy, item);
}

void aeron_driver_receiver_on_remove_cooldown(void *clientd, void *item)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    aeron_command_remove_cooldown_t *cmd = (aeron_command_remove_cooldown_t *)item;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)cmd->endpoint;

    if (aeron_receive_channel_endpoint_on_remove_cooldown(endpoint, cmd->session_id, cmd->stream_id) < 0)
    {
        AERON_DRIVER_RECEIVER_ERROR(receiver, "receiver on_remove_cooldown: %s", aeron_errmsg());
    }

    aeron_driver_conductor_proxy_on_delete_cmd(receiver->context->conductor_proxy, item);
}

int aeron_driver_receiver_add_pending_setup(
    aeron_driver_receiver_t *receiver,
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t session_id,
    int32_t stream_id)
{
    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(
        ensure_capacity_result, receiver->pending_setups, aeron_driver_receiver_pending_setup_entry_t);

    if (ensure_capacity_result < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "receiver add_pending_setup: %s", strerror(errcode));
        return ensure_capacity_result;
    }

    aeron_driver_receiver_pending_setup_entry_t *entry =
        &receiver->pending_setups.array[receiver->pending_setups.length++];

    entry->endpoint = endpoint;
    entry->session_id = session_id;
    entry->stream_id = stream_id;
    entry->time_of_status_message_ns = receiver->context->nano_clock();

    return ensure_capacity_result;
}

extern size_t aeron_driver_receiver_num_images(aeron_driver_receiver_t *receiver);
