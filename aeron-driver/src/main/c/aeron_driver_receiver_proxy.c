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

#include <sched.h>
#include "concurrent/aeron_counters_manager.h"
#include "aeron_driver_receiver_proxy.h"
#include "aeron_driver_receiver.h"
#include "aeron_alloc.h"

void aeron_driver_receiver_proxy_offer(aeron_driver_receiver_proxy_t *receiver_proxy, void *cmd)
{
    while (aeron_spsc_concurrent_array_queue_offer(receiver_proxy->command_queue, cmd) != AERON_OFFER_SUCCESS)
    {
        aeron_counter_ordered_increment(receiver_proxy->fail_counter, 1);
        sched_yield();
    }
}

void aeron_driver_receiver_proxy_on_delete_create_publication_image_cmd(
    aeron_driver_receiver_proxy_t *receiver_proxy, aeron_command_base_t *cmd)
{
    if (AERON_THREADING_MODE_SHARED == receiver_proxy->threading_mode)
    {
        return;
    }
    else
    {
        cmd->func = aeron_command_on_delete_cmd;
        cmd->item = NULL;

        aeron_driver_receiver_proxy_offer(receiver_proxy, cmd);
    }
}

void aeron_driver_receiver_proxy_on_add_endpoint(
    aeron_driver_receiver_proxy_t *receiver_proxy, aeron_receive_channel_endpoint_t *endpoint)
{
    if (AERON_THREADING_MODE_SHARED == receiver_proxy->threading_mode)
    {
        aeron_command_base_t cmd =
            {
                .func = aeron_driver_receiver_on_add_endpoint,
                .item = endpoint
            };

        aeron_driver_receiver_on_add_endpoint(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_command_base_t *cmd;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_base_t)) < 0)
        {
            aeron_counter_ordered_increment(receiver_proxy->fail_counter, 1);
            return;
        }

        cmd->func = aeron_driver_receiver_on_add_endpoint;
        cmd->item = endpoint;

        aeron_driver_receiver_proxy_offer(receiver_proxy, cmd);
    }
}

void aeron_driver_receiver_proxy_on_remove_endpoint(
    aeron_driver_receiver_proxy_t *receiver_proxy, aeron_receive_channel_endpoint_t *endpoint)
{
    if (AERON_THREADING_MODE_SHARED == receiver_proxy->threading_mode)
    {
        aeron_command_base_t cmd =
            {
                .func = aeron_driver_receiver_on_remove_endpoint,
                .item = endpoint
            };

        aeron_driver_receiver_on_remove_endpoint(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_command_base_t *cmd;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_base_t)) < 0)
        {
            aeron_counter_ordered_increment(receiver_proxy->fail_counter, 1);
            return;
        }

        cmd->func = aeron_driver_receiver_on_remove_endpoint;
        cmd->item = endpoint;

        aeron_driver_receiver_proxy_offer(receiver_proxy, cmd);
    }
}

void aeron_driver_receiver_proxy_on_add_subscription(
    aeron_driver_receiver_proxy_t *receiver_proxy, aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id)
{
    if (AERON_THREADING_MODE_SHARED == receiver_proxy->threading_mode)
    {
        aeron_command_subscription_t cmd =
            {
                .base = { aeron_driver_receiver_on_add_subscription, NULL },
                .endpoint = endpoint,
                .stream_id = stream_id
            };

        aeron_driver_receiver_on_add_subscription(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_command_subscription_t *cmd;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_subscription_t)) < 0)
        {
            aeron_counter_ordered_increment(receiver_proxy->fail_counter, 1);
            return;
        }

        cmd->base.func = aeron_driver_receiver_on_add_subscription;
        cmd->base.item = NULL;
        cmd->endpoint = endpoint;
        cmd->stream_id = stream_id;

        aeron_driver_receiver_proxy_offer(receiver_proxy, cmd);
    }
}

void aeron_driver_receiver_proxy_on_remove_subscription(
    aeron_driver_receiver_proxy_t *receiver_proxy, aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id)
{
    if (AERON_THREADING_MODE_SHARED == receiver_proxy->threading_mode)
    {
        aeron_command_subscription_t cmd =
            {
                .base = { aeron_driver_receiver_on_remove_subscription, NULL },
                .endpoint = endpoint,
                .stream_id = stream_id
            };

        aeron_driver_receiver_on_remove_subscription(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_command_subscription_t *cmd;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_subscription_t)) < 0)
        {
            aeron_counter_ordered_increment(receiver_proxy->fail_counter, 1);
            return;
        }

        cmd->base.func = aeron_driver_receiver_on_remove_subscription;
        cmd->base.item = NULL;
        cmd->endpoint = endpoint;
        cmd->stream_id = stream_id;

        aeron_driver_receiver_proxy_offer(receiver_proxy, cmd);
    }
}

void aeron_driver_receiver_proxy_on_add_publication_image(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_publication_image_t *image)
{
    if (AERON_THREADING_MODE_SHARED == receiver_proxy->threading_mode)
    {
        aeron_command_publication_image_t cmd =
            {
                .base = { aeron_driver_receiver_on_add_publication_image, NULL },
                .endpoint = endpoint,
                .image = image
            };

        aeron_driver_receiver_on_add_publication_image(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_command_publication_image_t *cmd;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_publication_image_t)) < 0)
        {
            aeron_counter_ordered_increment(receiver_proxy->fail_counter, 1);
            return;
        }

        cmd->base.func = aeron_driver_receiver_on_add_publication_image;
        cmd->base.item = NULL;
        cmd->endpoint = endpoint;
        cmd->image = image;

        aeron_driver_receiver_proxy_offer(receiver_proxy, cmd);
    }
}

void aeron_driver_receiver_proxy_on_remove_publication_image(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_publication_image_t *image)
{
    if (AERON_THREADING_MODE_SHARED == receiver_proxy->threading_mode)
    {
        aeron_command_publication_image_t cmd =
            {
                .base = { aeron_driver_receiver_on_remove_publication_image, NULL },
                .endpoint = endpoint,
                .image = image
            };

        aeron_driver_receiver_on_remove_publication_image(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_command_publication_image_t *cmd;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_publication_image_t)) < 0)
        {
            aeron_counter_ordered_increment(receiver_proxy->fail_counter, 1);
            return;
        }

        cmd->base.func = aeron_driver_receiver_on_remove_publication_image;
        cmd->base.item = NULL;
        cmd->endpoint = endpoint;
        cmd->image = image;

        aeron_driver_receiver_proxy_offer(receiver_proxy, cmd);
    }
}

void aeron_driver_receiver_proxy_on_remove_cool_down(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t session_id,
    int32_t stream_id)
{
    if (AERON_THREADING_MODE_SHARED == receiver_proxy->threading_mode)
    {
        aeron_command_remove_cool_down_t cmd =
            {
                .base = { aeron_driver_receiver_on_remove_cool_down, NULL },
                .endpoint = endpoint,
                .session_id = session_id,
                .stream_id = stream_id
            };

        aeron_driver_receiver_on_remove_cool_down(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_command_remove_cool_down_t *cmd;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_remove_cool_down_t)) < 0)
        {
            aeron_counter_ordered_increment(receiver_proxy->fail_counter, 1);
            return;
        }

        cmd->base.func = aeron_driver_receiver_on_remove_cool_down;
        cmd->base.item = NULL;
        cmd->endpoint = endpoint;
        cmd->session_id = session_id;
        cmd->stream_id = stream_id;

        aeron_driver_receiver_proxy_offer(receiver_proxy, cmd);
    }
}
