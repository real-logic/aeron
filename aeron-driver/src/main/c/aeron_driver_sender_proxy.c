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

#include "aeron_driver_sender.h"
#include "aeron_alloc.h"

void aeron_driver_sender_proxy_offer(aeron_driver_sender_proxy_t *sender_proxy, void *cmd)
{
    while (aeron_spsc_concurrent_array_queue_offer(sender_proxy->command_queue, cmd) != AERON_OFFER_SUCCESS)
    {
        aeron_counter_ordered_increment(sender_proxy->fail_counter, 1);
        sched_yield();
    }
}

void aeron_driver_sender_proxy_on_add_endpoint(
    aeron_driver_sender_proxy_t *sender_proxy, aeron_send_channel_endpoint_t *endpoint)
{
    sender_proxy->on_add_endpoint_func(endpoint->conductor_fields.udp_channel);

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(sender_proxy->threading_mode))
    {
        aeron_command_base_t cmd =
            {
                .func = aeron_driver_sender_on_add_endpoint,
                .item = endpoint
            };

        aeron_driver_sender_on_add_endpoint(sender_proxy->sender, &cmd);
    }
    else
    {
        aeron_command_base_t *cmd = NULL;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_base_t)) < 0)
        {
            aeron_counter_ordered_increment(sender_proxy->fail_counter, 1);
            return;
        }

        cmd->func = aeron_driver_sender_on_add_endpoint;
        cmd->item = endpoint;

        aeron_driver_sender_proxy_offer(sender_proxy, cmd);
    }
}

void aeron_driver_sender_proxy_on_remove_endpoint(
    aeron_driver_sender_proxy_t *sender_proxy, aeron_send_channel_endpoint_t *endpoint)
{
    sender_proxy->on_remove_endpoint_func(endpoint->conductor_fields.udp_channel);

    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(sender_proxy->threading_mode))
    {
        aeron_command_base_t cmd =
            {
                .func = aeron_driver_sender_on_remove_endpoint,
                .item = endpoint
            };

        aeron_driver_sender_on_remove_endpoint(sender_proxy->sender, &cmd);
    }
    else
    {
        aeron_command_base_t *cmd = NULL;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_base_t)) < 0)
        {
            aeron_counter_ordered_increment(sender_proxy->fail_counter, 1);
            return;
        }

        cmd->func = aeron_driver_sender_on_remove_endpoint;
        cmd->item = endpoint;

        aeron_driver_sender_proxy_offer(sender_proxy, cmd);
    }
}

void aeron_driver_sender_proxy_on_add_publication(
    aeron_driver_sender_proxy_t *sender_proxy, aeron_network_publication_t *publication)
{
    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(sender_proxy->threading_mode))
    {
        aeron_command_base_t cmd =
            {
                .func = aeron_driver_sender_on_add_publication,
                .item = publication
            };

        aeron_driver_sender_on_add_publication(sender_proxy->sender, &cmd);
    }
    else
    {
        aeron_command_base_t *cmd = NULL;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_base_t)) < 0)
        {
            aeron_counter_ordered_increment(sender_proxy->fail_counter, 1);
            return;
        }

        cmd->func = aeron_driver_sender_on_add_publication;
        cmd->item = publication;

        aeron_driver_sender_proxy_offer(sender_proxy, cmd);
    }
}

void aeron_driver_sender_proxy_on_remove_publication(
    aeron_driver_sender_proxy_t *sender_proxy, aeron_network_publication_t *publication)
{
    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(sender_proxy->threading_mode))
    {
        aeron_command_base_t cmd =
            {
                .func = aeron_driver_sender_on_remove_publication,
                .item = publication
            };

        aeron_driver_sender_on_remove_publication(sender_proxy->sender, &cmd);
    }
    else
    {
        aeron_command_base_t *cmd = NULL;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_base_t)) < 0)
        {
            aeron_counter_ordered_increment(sender_proxy->fail_counter, 1);
            return;
        }

        cmd->func = aeron_driver_sender_on_remove_publication;
        cmd->item = publication;

        aeron_driver_sender_proxy_offer(sender_proxy, cmd);
    }
}

void aeron_driver_sender_proxy_on_add_destination(
    aeron_driver_sender_proxy_t *sender_proxy,
    aeron_send_channel_endpoint_t *endpoint,
    aeron_uri_t *uri,
    struct sockaddr_storage *addr)
{
    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(sender_proxy->threading_mode))
    {
        aeron_command_destination_t cmd =
            {
                .base = { .func = aeron_driver_sender_on_add_destination, .item = NULL },
                .endpoint = endpoint,
                .uri = uri
            };
        memcpy(&cmd.control_address, addr, sizeof(cmd.control_address));

        aeron_driver_sender_on_add_destination(sender_proxy->sender, &cmd);
    }
    else
    {
        aeron_command_destination_t *cmd = NULL;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_destination_t)) < 0)
        {
            aeron_counter_ordered_increment(sender_proxy->fail_counter, 1);
            return;
        }

        cmd->base.func = aeron_driver_sender_on_add_destination;
        cmd->base.item = NULL;
        cmd->endpoint = endpoint;
        cmd->uri = uri;
        memcpy(&cmd->control_address, addr, sizeof(cmd->control_address));

        aeron_driver_sender_proxy_offer(sender_proxy, cmd);
    }
}

void aeron_driver_sender_proxy_on_remove_destination(
    aeron_driver_sender_proxy_t *sender_proxy, aeron_send_channel_endpoint_t *endpoint, struct sockaddr_storage *addr)
{
    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(sender_proxy->threading_mode))
    {
        aeron_command_destination_t cmd =
            {
                .base = { .func = aeron_driver_sender_on_remove_destination, .item = NULL },
                .endpoint = endpoint
            };
        memcpy(&cmd.control_address, addr, sizeof(cmd.control_address));

        aeron_driver_sender_on_remove_destination(sender_proxy->sender, &cmd);
    }
    else
    {
        aeron_command_destination_t *cmd = NULL;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_destination_t)) < 0)
        {
            aeron_counter_ordered_increment(sender_proxy->fail_counter, 1);
            return;
        }

        cmd->base.func = aeron_driver_sender_on_remove_destination;
        cmd->base.item = NULL;
        cmd->endpoint = endpoint;
        memcpy(&cmd->control_address, addr, sizeof(cmd->control_address));

        aeron_driver_sender_proxy_offer(sender_proxy, cmd);
    }
}

void aeron_driver_sender_proxy_on_resolution_change(
    aeron_driver_sender_proxy_t *sender_proxy,
    const char *endpoint_name,
    aeron_send_channel_endpoint_t *endpoint,
    struct sockaddr_storage *new_addr)
{
    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(sender_proxy->threading_mode))
    {
        aeron_command_sender_resolution_change_t cmd =
            {
                .base = { .func = aeron_driver_sender_on_resolution_change, .item = NULL },
                .endpoint = endpoint,
                .endpoint_name = endpoint_name,
            };
        memcpy(&cmd.new_addr, new_addr, sizeof(cmd.new_addr));

        aeron_driver_sender_on_resolution_change(sender_proxy->sender, &cmd);
    }
    else
    {
        aeron_command_sender_resolution_change_t *cmd = NULL;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_sender_resolution_change_t)) < 0)
        {
            aeron_counter_ordered_increment(sender_proxy->fail_counter, 1);
            return;
        }

        cmd->base.func = aeron_driver_sender_on_resolution_change;
        cmd->base.item = NULL;
        cmd->endpoint = endpoint;
        cmd->endpoint_name = endpoint_name;
        memcpy(&cmd->new_addr, new_addr, sizeof(cmd->new_addr));

        aeron_driver_sender_proxy_offer(sender_proxy, cmd);
    }
}

void aeron_driver_sender_proxy_on_delete_cmd(aeron_driver_sender_proxy_t *sender_proxy, aeron_command_base_t *cmd)
{
    if (AERON_THREADING_MODE_IS_SHARED_OR_INVOKER(sender_proxy->threading_mode))
    {
        /* should not get here! */
    }
    else
    {
        cmd->func = aeron_command_on_delete_cmd;
        cmd->item = NULL;

        aeron_driver_sender_proxy_offer(sender_proxy, cmd);
    }
}
