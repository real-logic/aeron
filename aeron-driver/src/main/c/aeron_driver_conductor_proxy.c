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

#include <sched.h>
#include "concurrent/aeron_counters_manager.h"
#include "aeron_driver_conductor_proxy.h"
#include "aeron_alloc.h"
#include "aeron_driver_conductor.h"

void aeron_driver_conductor_proxy_offer(aeron_driver_conductor_proxy_t *conductor_proxy, void *cmd)
{
    while (aeron_mpsc_concurrent_array_queue_offer(conductor_proxy->command_queue, cmd) != AERON_OFFER_SUCCESS)
    {
        aeron_counter_ordered_increment(conductor_proxy->fail_counter, 1);
        sched_yield();
    }
}

void aeron_driver_conductor_proxy_on_delete_cmd(
    aeron_driver_conductor_proxy_t *conductor_proxy, aeron_command_base_t *cmd)
{
    if (AERON_THREADING_MODE_SHARED == conductor_proxy->threading_mode)
    {
        /* should not get here! */
    }
    else
    {
        cmd->func = aeron_command_on_delete_cmd;
        cmd->item = NULL;

        aeron_driver_conductor_proxy_offer(conductor_proxy, cmd);
    }
}

void aeron_driver_conductor_proxy_on_create_publication_image_cmd(
    aeron_driver_conductor_proxy_t *conductor_proxy,
    int32_t session_id,
    int32_t stream_id,
    int32_t initial_term_id,
    int32_t active_term_id,
    int32_t term_offset,
    int32_t term_length,
    int32_t mtu_length,
    struct sockaddr_storage *control_address,
    struct sockaddr_storage *src_address,
    void *endpoint)
{
    if (AERON_THREADING_MODE_SHARED == conductor_proxy->threading_mode)
    {
        aeron_command_create_publication_image_t cmd =
            {
                .base = { .func = aeron_driver_conductor_on_create_publication_image, .item = NULL },
                .session_id = session_id,
                .stream_id = stream_id,
                .initial_term_id = initial_term_id,
                .active_term_id = active_term_id,
                .term_offset = term_offset,
                .term_length = term_length,
                .mtu_length = mtu_length,
                .endpoint = endpoint
            };

        memcpy(&cmd.control_address, control_address, sizeof(struct sockaddr_storage));
        memcpy(&cmd.src_address, src_address, sizeof(struct sockaddr_storage));

        aeron_driver_conductor_on_create_publication_image(conductor_proxy->conductor, &cmd);
    }
    else
    {
        aeron_command_create_publication_image_t *cmd;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_create_publication_image_t)) < 0)
        {
            aeron_counter_ordered_increment(conductor_proxy->fail_counter, 1);
            return;
        }

        cmd->base.func = aeron_driver_conductor_on_create_publication_image;
        cmd->base.item = NULL;

        cmd->session_id = session_id;
        cmd->stream_id = stream_id;
        cmd->initial_term_id = initial_term_id;
        cmd->active_term_id = active_term_id;
        cmd->term_offset = term_offset;
        cmd->term_length = term_length;
        cmd->mtu_length = mtu_length;
        cmd->endpoint = endpoint;
        memcpy(&cmd->control_address, control_address, sizeof(struct sockaddr_storage));
        memcpy(&cmd->src_address, src_address, sizeof(struct sockaddr_storage));

        aeron_driver_conductor_proxy_offer(conductor_proxy, cmd);
    }
}

void aeron_driver_conductor_proxy_on_linger_buffer(
    aeron_driver_conductor_proxy_t *conductor_proxy, uint8_t *buffer)
{
    if (AERON_THREADING_MODE_SHARED == conductor_proxy->threading_mode)
    {
        aeron_free(buffer);
    }
    else
    {
        aeron_command_base_t *cmd;

        if (aeron_alloc((void **)&cmd, sizeof(aeron_command_base_t)) < 0)
        {
            aeron_counter_ordered_increment(conductor_proxy->fail_counter, 1);
            return;
        }

        cmd->func = aeron_driver_conductor_on_linger_buffer;
        cmd->item = buffer;

        aeron_driver_conductor_proxy_offer(conductor_proxy, cmd);
    }
}

void aeron_command_on_delete_cmd(void *clientd, void *cmd)
{
    aeron_command_base_t *command = (aeron_command_base_t *)cmd;

    aeron_free(command->item);
    aeron_free(cmd);
}
