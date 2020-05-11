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

#ifndef AERON_DRIVER_RECEIVER_H
#define AERON_DRIVER_RECEIVER_H

#include "media/aeron_udp_transport_poller.h"
#include "concurrent/aeron_distinct_error_log.h"
#include "aeron_driver_context.h"
#include "aeron_driver_receiver_proxy.h"
#include "aeron_system_counters.h"
#include "media/aeron_udp_channel.h"

#define AERON_DRIVER_RECEIVER_NUM_RECV_BUFFERS (2)

#define AERON_DRIVER_RECEIVER_PENDING_SETUP_TIMEOUT_NS (1000 * 1000 * 1000LL)

typedef struct aeron_driver_receiver_image_entry_stct
{
    aeron_publication_image_t *image;
}
aeron_driver_receiver_image_entry_t;

typedef struct aeron_driver_receiver_pending_setup_entry_stct
{
    bool is_periodic;
    int32_t session_id;
    int32_t stream_id;
    aeron_receive_channel_endpoint_t *endpoint;
    aeron_receive_destination_t *destination;
    int64_t time_of_status_message_ns;
    struct sockaddr_storage control_addr;
}
aeron_driver_receiver_pending_setup_entry_t;

typedef struct aeron_driver_receiver_stct
{
    aeron_driver_receiver_proxy_t receiver_proxy;
    aeron_udp_transport_poller_t poller;

    aeron_udp_channel_recv_buffers_t recv_buffers;

    struct aeron_driver_receiver_images_stct
    {
        aeron_driver_receiver_image_entry_t *array;
        size_t length;
        size_t capacity;
    }
    images;

    struct aeron_driver_receiver_pending_setups_stct
    {
        aeron_driver_receiver_pending_setup_entry_t *array;
        size_t length;
        size_t capacity;
    }
    pending_setups;

    aeron_udp_channel_data_paths_t data_paths;

    aeron_driver_context_t *context;
    aeron_udp_transport_poller_poll_func_t poller_poll_func;
    aeron_udp_channel_transport_recvmmsg_func_t recvmmsg_func;
    aeron_distinct_error_log_t *error_log;
    int64_t re_resolution_deadline_ns;

    int64_t *errors_counter;
    int64_t *invalid_frames_counter;
    int64_t *total_bytes_received_counter;
    int64_t *resolution_changes_counter;
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

void aeron_driver_receiver_on_add_endpoint(void *clientd, void *item);
void aeron_driver_receiver_on_remove_endpoint(void *clientd, void *item);

void aeron_driver_receiver_on_add_subscription(void *clientd, void *item);
void aeron_driver_receiver_on_remove_subscription(void *clientd, void *item);
void aeron_driver_receiver_on_add_subscription_by_session(void *clientd, void *item);
void aeron_driver_receiver_on_remove_subscription_by_session(void *clientd, void *item);

void aeron_driver_receiver_on_add_destination(void *clientd, void *item);
void aeron_driver_receiver_on_remove_destination(void *clientd, void *item);

void aeron_driver_receiver_on_add_publication_image(void *clientd, void *item);
void aeron_driver_receiver_on_remove_publication_image(void *clientd, void *item);

void aeron_driver_receiver_on_remove_cool_down(void *clientd, void *item);

void aeron_driver_receiver_on_resolution_change(void *clientd, void *item);

int aeron_driver_receiver_add_pending_setup(
    aeron_driver_receiver_t *receiver,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    int32_t session_id,
    int32_t stream_id,
    struct sockaddr_storage *control_addr);

inline size_t aeron_driver_receiver_num_images(aeron_driver_receiver_t *receiver)
{
    return receiver->images.length;
}

#endif //AERON_DRIVER_RECEIVER_H
