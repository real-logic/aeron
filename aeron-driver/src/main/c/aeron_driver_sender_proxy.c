/*
 * Copyright 2014-2019 Real Logic Ltd.
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

#include "aeron_driver_sender.h"
#include "aeron_alloc.h"
#include "concurrent/aeron_thread.h"

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
    if (AERON_THREADING_MODE_SHARED == sender_proxy->threading_mode)
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
        aeron_command_base_t *cmd;

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
    if (AERON_THREADING_MODE_SHARED == sender_proxy->threading_mode)
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
        aeron_command_base_t *cmd;

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
    if (AERON_THREADING_MODE_SHARED == sender_proxy->threading_mode)
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
        aeron_command_base_t *cmd;

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
    if (AERON_THREADING_MODE_SHARED == sender_proxy->threading_mode)
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
        aeron_command_base_t *cmd;

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
    aeron_driver_sender_proxy_t *sender_proxy, aeron_send_channel_endpoint_t *endpoint, struct sockaddr_storage *addr)
{
    if (AERON_THREADING_MODE_SHARED == sender_proxy->threading_mode)
    {
        aeron_command_destination_t cmd =
            {
                .base = { .func = aeron_driver_sender_on_add_destination, .item = NULL },
                .endpoint = endpoint
            };
        memcpy(&cmd.control_address, addr, sizeof(cmd.control_address));

        aeron_driver_sender_on_add_destination(sender_proxy->sender, &cmd);
    }
    else
    {
        aeron_command_destination_t *cmd;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_destination_t)) < 0)
        {
            aeron_counter_ordered_increment(sender_proxy->fail_counter, 1);
            return;
        }

        cmd->base.func = aeron_driver_sender_on_add_destination;
        cmd->base.item = NULL;
        cmd->endpoint = endpoint;
        memcpy(&cmd->control_address, addr, sizeof(cmd->control_address));

        aeron_driver_sender_proxy_offer(sender_proxy, cmd);
    }
}

void aeron_driver_sender_proxy_on_remove_destination(
    aeron_driver_sender_proxy_t *sender_proxy, aeron_send_channel_endpoint_t *endpoint, struct sockaddr_storage *addr)
{
    if (AERON_THREADING_MODE_SHARED == sender_proxy->threading_mode)
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
        aeron_command_destination_t *cmd;

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
