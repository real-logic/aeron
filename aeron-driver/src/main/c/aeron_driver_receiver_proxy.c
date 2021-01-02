/*
 * Copyright 2014-2021 Real Logic Limited.
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

void aeron_driver_receiver_proxy_on_delete_cmd(
    aeron_driver_receiver_proxy_t *receiver_proxy, aeron_command_base_t *cmd)
{
    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
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
    receiver_proxy->on_add_endpoint_func(endpoint->conductor_fields.udp_channel);

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
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
        aeron_command_base_t *cmd = NULL;

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
    receiver_proxy->on_remove_endpoint_func(endpoint->conductor_fields.udp_channel);

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
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
        aeron_command_base_t *cmd = NULL;

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
    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_command_subscription_t cmd =
            {
                .base = { .func = aeron_driver_receiver_on_add_subscription, .item = NULL },
                .endpoint = endpoint,
                .stream_id = stream_id,
                .session_id = 0 // ignored
            };

        aeron_driver_receiver_on_add_subscription(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_command_subscription_t *cmd = NULL;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_subscription_t)) < 0)
        {
            aeron_counter_ordered_increment(receiver_proxy->fail_counter, 1);
            return;
        }

        cmd->base.func = aeron_driver_receiver_on_add_subscription;
        cmd->base.item = NULL;
        cmd->endpoint = endpoint;
        cmd->stream_id = stream_id;
        cmd->session_id = 0; // ignored

        aeron_driver_receiver_proxy_offer(receiver_proxy, cmd);
    }
}

void aeron_driver_receiver_proxy_on_remove_subscription(
    aeron_driver_receiver_proxy_t *receiver_proxy, aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id)
{
    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_command_subscription_t cmd =
            {
                .base = { .func = aeron_driver_receiver_on_remove_subscription, .item = NULL },
                .endpoint = endpoint,
                .stream_id = stream_id,
                .session_id = 0 // ignored.
            };

        aeron_driver_receiver_on_remove_subscription(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_command_subscription_t *cmd = NULL;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_subscription_t)) < 0)
        {
            aeron_counter_ordered_increment(receiver_proxy->fail_counter, 1);
            return;
        }

        cmd->base.func = aeron_driver_receiver_on_remove_subscription;
        cmd->base.item = NULL;
        cmd->endpoint = endpoint;
        cmd->stream_id = stream_id;
        cmd->session_id = 0; // ignored.

        aeron_driver_receiver_proxy_offer(receiver_proxy, cmd);
    }
}

void aeron_driver_receiver_proxy_on_add_subscription_by_session(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    int32_t session_id)
{
    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_command_subscription_t cmd =
            {
                .base = { .func = aeron_driver_receiver_on_add_subscription_by_session, .item = NULL },
                .endpoint = endpoint,
                .stream_id = stream_id,
                .session_id = session_id
            };

        aeron_driver_receiver_on_add_subscription_by_session(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_command_subscription_t *cmd = NULL;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_subscription_t)) < 0)
        {
            aeron_counter_ordered_increment(receiver_proxy->fail_counter, 1);
            return;
        }

        cmd->base.func = aeron_driver_receiver_on_add_subscription_by_session;
        cmd->base.item = NULL;
        cmd->endpoint = endpoint;
        cmd->stream_id = stream_id;
        cmd->session_id = session_id;

        aeron_driver_receiver_proxy_offer(receiver_proxy, cmd);
    }
}

void aeron_driver_receiver_proxy_on_remove_subscription_by_session(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    int32_t session_id)
{
    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_command_subscription_t cmd =
            {
                .base = { .func = aeron_driver_receiver_on_remove_subscription_by_session, .item = NULL },
                .endpoint = endpoint,
                .stream_id = stream_id,
                .session_id = session_id
            };

        aeron_driver_receiver_on_remove_subscription_by_session(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_command_subscription_t *cmd = NULL;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_subscription_t)) < 0)
        {
            aeron_counter_ordered_increment(receiver_proxy->fail_counter, 1);
            return;
        }

        cmd->base.func = aeron_driver_receiver_on_remove_subscription_by_session;
        cmd->base.item = NULL;
        cmd->endpoint = endpoint;
        cmd->stream_id = stream_id;
        cmd->session_id = session_id;

        aeron_driver_receiver_proxy_offer(receiver_proxy, cmd);
    }
}

void aeron_driver_receiver_proxy_on_add_destination(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination)
{
    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_command_add_rcv_destination_t cmd =
            {
                .base = { .func = aeron_driver_receiver_on_add_destination, .item = NULL },
                .endpoint = endpoint,
                .destination = destination
            };

        aeron_driver_receiver_on_add_destination(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_command_add_rcv_destination_t *cmd = NULL;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_subscription_t)) < 0)
        {
            aeron_counter_ordered_increment(receiver_proxy->fail_counter, 1);
            return;
        }

        cmd->base.func = aeron_driver_receiver_on_add_destination;
        cmd->base.item = NULL;
        cmd->endpoint = endpoint;
        cmd->destination = destination;

        aeron_driver_receiver_proxy_offer(receiver_proxy, cmd);
    }
}

void aeron_driver_receiver_proxy_on_remove_destination(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_udp_channel_t *channel)
{
    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_command_remove_rcv_destination_t cmd =
            {
                .base = { .func = aeron_driver_receiver_on_remove_destination, .item = NULL },
                .endpoint = endpoint,
                .channel = channel
            };

        aeron_driver_receiver_on_remove_destination(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_command_remove_rcv_destination_t *cmd = NULL;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_subscription_t)) < 0)
        {
            aeron_counter_ordered_increment(receiver_proxy->fail_counter, 1);
            return;
        }

        cmd->base.func = aeron_driver_receiver_on_remove_destination;
        cmd->base.item = NULL;
        cmd->endpoint = endpoint;
        cmd->channel = channel;

        aeron_driver_receiver_proxy_offer(receiver_proxy, cmd);
    }
}

void aeron_driver_receiver_proxy_on_add_publication_image(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_publication_image_t *image)
{
    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_command_publication_image_t cmd =
            {
                .base = { .func = aeron_driver_receiver_on_add_publication_image, .item = NULL },
                .endpoint = endpoint,
                .image = image
            };

        aeron_driver_receiver_on_add_publication_image(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_command_publication_image_t *cmd = NULL;

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
    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_command_publication_image_t cmd =
            {
                .base = { .func = aeron_driver_receiver_on_remove_publication_image, .item = NULL },
                .endpoint = endpoint,
                .image = image
            };

        aeron_driver_receiver_on_remove_publication_image(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_command_publication_image_t *cmd = NULL;

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
    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_command_remove_cool_down_t cmd =
            {
                .base = { .func = aeron_driver_receiver_on_remove_cool_down, .item = NULL },
                .endpoint = endpoint,
                .session_id = session_id,
                .stream_id = stream_id
            };

        aeron_driver_receiver_on_remove_cool_down(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_command_remove_cool_down_t *cmd = NULL;

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

void aeron_driver_receiver_proxy_on_resolution_change(
    aeron_driver_receiver_proxy_t *receiver_proxy,
    const char *endpoint_name,
    void *endpoint,
    void *destination,
    struct sockaddr_storage *new_addr)
{
    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(receiver_proxy->threading_mode))
    {
        aeron_command_receiver_resolution_change_t cmd =
            {
                .base = { .func = aeron_driver_receiver_on_resolution_change, .item = NULL },
                .endpoint_name = endpoint_name,
                .endpoint = endpoint,
                .destination = destination
            };
        memcpy(&cmd.new_addr, new_addr, sizeof(cmd.new_addr));

        aeron_driver_receiver_on_resolution_change(receiver_proxy->receiver, &cmd);
    }
    else
    {
        aeron_command_receiver_resolution_change_t *cmd = NULL;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_receiver_resolution_change_t)) < 0)
        {
            aeron_counter_ordered_increment(receiver_proxy->fail_counter, 1);
            return;
        }

        cmd->base.func = aeron_driver_receiver_on_resolution_change;
        cmd->base.item = NULL;
        cmd->endpoint_name = endpoint_name;
        cmd->endpoint = endpoint;
        cmd->destination = destination;
        memcpy(&cmd->new_addr, new_addr, sizeof(cmd->new_addr));

        aeron_driver_receiver_proxy_offer(receiver_proxy, cmd);
    }
}
