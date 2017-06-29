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

#include <sched.h>
#include "concurrent/aeron_counters_manager.h"
#include "aeron_driver_receiver_proxy.h"

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


