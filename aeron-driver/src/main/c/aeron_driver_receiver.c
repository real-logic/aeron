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
#include "media/aeron_receive_channel_endpoint.h"
#include "aeron_driver_receiver.h"
#include "aeron_driver_conductor_proxy.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

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

    receiver->context = context;
    receiver->error_log = error_log;

    receiver->receiver_proxy.command_queue = &context->receiver_command_queue;
    receiver->receiver_proxy.fail_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_RECEIVER_PROXY_FAILS);
    receiver->receiver_proxy.threading_mode = context->threading_mode;
    receiver->receiver_proxy.receiver = receiver;

    receiver->errors_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_ERRORS);
    return 0;
}

#define AERON_DRIVER_RECEIVER_ERROR(receiver, format, ...) \
do \
{ \
    char error_buffer[AERON_MAX_PATH]; \
    int err_code = aeron_errcode(); \
    snprintf(error_buffer, sizeof(error_buffer) - 1, format, __VA_ARGS__); \
    aeron_distinct_error_log_record(receiver->error_log, err_code, aeron_errmsg(), error_buffer); \
    aeron_counter_increment(receiver->errors_counter, 1); \
    aeron_set_err(0, "%s", "no error"); \
} \
while(0)

void aeron_driver_receiver_on_command(void *clientd, volatile void *item)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)item;

    cmd->func(clientd, cmd);

    /* recycle cmd by sending to conductor as on_cmd_free */
    aeron_driver_conductor_proxy_on_delete_cmd(receiver->context->conductor_proxy, cmd);
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

    //int64_t now_ns = receiver->context->nano_clock();

    work_count += (poll_result < 0) ? 0 : poll_result;

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

    aeron_udp_transport_poller_close(&receiver->poller);
}

