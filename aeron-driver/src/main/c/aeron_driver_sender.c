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

#include <media/aeron_send_channel_endpoint.h>
#include "aeron_driver_sender.h"

int aeron_driver_sender_init(
    aeron_driver_sender_t *sender, aeron_driver_context_t *context, aeron_system_counters_t *system_counters)
{
    sender->context = context;
    sender->sender_proxy.sender = sender;
    sender->sender_proxy.command_queue = &context->sender_command_queue;
    sender->sender_proxy.fail_counter =
        aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_SENDER_PROXY_FAILS);
    sender->sender_proxy.threading_mode = context->threading_mode;
    return 0;
}

void aeron_driver_sender_on_command(void *clientd, volatile void *item)
{
    aeron_command_base_t *cmd = (aeron_command_base_t *)item;

    cmd->func(clientd, cmd);
}

int aeron_driver_sender_do_work(void *clientd)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    int work_count = 0;

    work_count +=
        aeron_spsc_concurrent_array_queue_drain(sender->sender_proxy.command_queue, aeron_driver_sender_on_command, sender, 10);

    return work_count;
}

void aeron_driver_sender_on_close(void *clientd)
{

}

void aeron_driver_sender_on_register_endpoint(void *clientd, void *command)
{
    aeron_driver_sender_t *sender = (aeron_driver_sender_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)command;
    aeron_send_channel_endpoint_t *endpoint = (aeron_send_channel_endpoint_t *)cmd->item;

    /* TODO: recycle cmd by sending to conductor as on_cmd_free */
}
