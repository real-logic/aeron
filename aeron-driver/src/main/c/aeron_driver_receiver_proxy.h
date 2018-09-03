/*
 * Copyright 2014-2018 Real Logic Ltd.
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

#ifndef AERON_AERON_DRIVER_RECEIVER_PROXY_H
#define AERON_AERON_DRIVER_RECEIVER_PROXY_H

#include "aeron_driver_context.h"

typedef struct aeron_driver_receiver_stct aeron_driver_receiver_t;
typedef struct aeron_receive_channel_endpoint_stct aeron_receive_channel_endpoint_t;
typedef struct aeron_publication_image_stct aeron_publication_image_t;

typedef struct aeron_driver_receiver_proxy_stct
{
    aeron_driver_receiver_t *receiver;
    aeron_threading_mode_t threading_mode;
    aeron_spsc_concurrent_array_queue_t *command_queue;
    int64_t *fail_counter;
}
aeron_driver_receiver_proxy_t;

void aeron_driver_receiver_proxy_on_delete_create_publication_image_cmd(
    aeron_driver_receiver_proxy_t *receiver_proxy, aeron_command_base_t *cmd);

void aeron_driver_receiver_proxy_on_add_endpoint(
    aeron_driver_receiver_proxy_t *receiver_proxy, aeron_receive_channel_endpoint_t *endpoint);
void aeron_driver_receiver_proxy_on_remove_endpoint(
    aeron_driver_receiver_proxy_t *receiver_proxy, aeron_receive_channel_endpoint_t *endpoint);

typedef struct aeron_command_subscription_stct
{
    aeron_command_base_t base;
    void *endpoint;
    int32_t stream_id;
}
aeron_command_subscription_t;

void aeron_driver_receiver_proxy_on_add_subscription(
    aeron_driver_receiver_proxy_t *receiver_proxy, aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id);
void aeron_driver_receiver_proxy_on_remove_subscription(
    aeron_driver_receiver_proxy_t *receiver_proxy, aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id);

typedef struct aeron_command_publication_image_stct
{
    aeron_command_base_t base;
    void *endpoint;
    void *image;
}
aeron_command_publication_image_t;

typedef struct aeron_command_remove_cool_down_stct
{
    aeron_command_base_t base;
    void *endpoint;
    int32_t session_id;
    int32_t stream_id;
}
aeron_command_remove_cool_down_t;

void aeron_driver_receiver_proxy_on_add_publication_image(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_publication_image_t *image);
void aeron_driver_receiver_proxy_on_remove_publication_image(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_publication_image_t *image);
void aeron_driver_receiver_proxy_on_remove_cool_down(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t session_id,
    int32_t stream_id);

#endif //AERON_AERON_DRIVER_RECEIVER_PROXY_H
