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

#ifndef AERON_AERON_DRIVER_RECEIVER_H
#define AERON_AERON_DRIVER_RECEIVER_H

#include "media/aeron_udp_transport_poller.h"
#include "concurrent/aeron_distinct_error_log.h"
#include "aeron_driver_context.h"
#include "aeron_driver_receiver_proxy.h"
#include "aeron_system_counters.h"

#define AERON_DRIVER_RECEIVER_NUM_RECV_BUFFERS (2)
#define AERON_DRIVER_RECEIVER_MAX_UDP_PACKET_LENGTH (64 * 1024)

typedef struct aeron_driver_receiver_stct
{
    aeron_driver_receiver_proxy_t receiver_proxy;
    aeron_udp_transport_poller_t poller;

    struct aeron_driver_receiver_buffers_stct
    {
        uint8_t *buffers[AERON_DRIVER_RECEIVER_NUM_RECV_BUFFERS];
        struct iovec iov[AERON_DRIVER_RECEIVER_NUM_RECV_BUFFERS];
        struct sockaddr_storage addrs[AERON_DRIVER_RECEIVER_NUM_RECV_BUFFERS];
    }
    recv_buffers;

    aeron_driver_context_t *context;
    aeron_distinct_error_log_t *error_log;

    int64_t *errors_counter;
    int64_t *invalid_frames_counter;
}
aeron_driver_receiver_t;

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

int aeron_driver_receiver_init(
    aeron_driver_receiver_t *receiver,
    aeron_driver_context_t *context,
    aeron_system_counters_t *system_counters,
    aeron_distinct_error_log_t *error_log);

int aeron_driver_receiver_do_work(void *clientd);
void aeron_driver_receiver_on_close(void *clientd);

#endif //AERON_AERON_DRIVER_RECEIVER_H
