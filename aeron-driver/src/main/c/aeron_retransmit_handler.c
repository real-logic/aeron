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

#include <string.h>
#include "concurrent/aeron_counters_manager.h"
#include "protocol/aeron_udp_protocol.h"
#include "util/aeron_error.h"
#include "aeron_retransmit_handler.h"

int aeron_retransmit_handler_init(
    aeron_retransmit_handler_t *handler,
    int64_t *invalid_packets_counter,
    int64_t linger_timeout_ns)
{
    if (aeron_int64_to_ptr_hash_map_init(
        &handler->active_retransmits_map, 8, AERON_INT64_TO_PTR_HASH_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "could not init retransmit handler map: %s", strerror(errcode));
        return -1;
    }

    handler->invalid_packets_counter = invalid_packets_counter;
    handler->linger_timeout_ns = linger_timeout_ns;

    for (size_t i = 0; i < AERON_RETRANSMIT_HANDLER_MAX_RETRANSMITS; i++)
    {
        handler->retransmit_action_pool[i].state = AERON_RETRANSMIT_ACTION_STATE_INACTIVE;
    }

    return 0;
}

int aeron_retransmit_handler_close(aeron_retransmit_handler_t *handler)
{
    aeron_int64_to_ptr_hash_map_delete(&handler->active_retransmits_map);
    return 0;
}

bool aeron_retransmit_handler_is_invalid(aeron_retransmit_handler_t *handler, int32_t term_offset, size_t term_length)
{
    const bool is_invalid = (term_offset > ((int32_t)(term_length - AERON_DATA_HEADER_LENGTH))) || (term_offset < 0);

    if (is_invalid)
    {
        aeron_counter_increment(handler->invalid_packets_counter, 1);
    }

    return is_invalid;
}

aeron_retransmit_action_t *aeron_retransmit_handler_assign_action(aeron_retransmit_handler_t *handler)
{
    for (size_t i = 0; i < AERON_RETRANSMIT_HANDLER_MAX_RETRANSMITS; i++)
    {
        if (AERON_RETRANSMIT_ACTION_STATE_INACTIVE == handler->retransmit_action_pool[i].state)
        {
            return &handler->retransmit_action_pool[i];
        }
    }

    return NULL;
}

int aeron_retransmit_handler_on_nak(
    aeron_retransmit_handler_t *handler,
    int32_t term_id,
    int32_t term_offset,
    size_t length,
    size_t term_length,
    int64_t now_ns,
    aeron_retransmit_handler_resend_func_t resend,
    void *resend_clientd)
{
    int result = 0;

    if (!aeron_retransmit_handler_is_invalid(handler, term_offset, term_length))
    {
        const int64_t key = aeron_int64_to_ptr_hash_map_compound_key(term_id, term_offset);

        if (NULL == aeron_int64_to_ptr_hash_map_get(&handler->active_retransmits_map, key) &&
            handler->active_retransmits_map.size < AERON_RETRANSMIT_HANDLER_MAX_RETRANSMITS)
        {
            aeron_retransmit_action_t *action = aeron_retransmit_handler_assign_action(handler);

            if (NULL == action)
            {
                aeron_set_err(EINVAL, "%s", "could not assign retransmit action");
                return -1;
            }

            const size_t term_length_left = term_length - term_offset;

            action->term_id = term_id;
            action->term_offset = term_offset;
            action->length = length < term_length_left ? length : term_length_left;

            result = resend(resend_clientd, term_id, term_offset, action->length);
            action->state = AERON_RETRANSMIT_ACTION_STATE_LINGERING;
            action->expire_ns = now_ns + handler->linger_timeout_ns;

            if (aeron_int64_to_ptr_hash_map_put(&handler->active_retransmits_map, key, action) < 0)
            {
                int errcode = errno;

                aeron_set_err(errcode, "could not put retransmit handler map: %s", strerror(errcode));
                return -1;
            }
        }
    }

    return result;
}

int aeron_retransmit_handler_process_timeouts(
    aeron_retransmit_handler_t *handler,
    int64_t now_ns)
{
    int result = 0;
    size_t num_active_actions = handler->active_retransmits_map.size;

    if (num_active_actions > 0)
    {
        for (size_t i = 0; i < AERON_RETRANSMIT_HANDLER_MAX_RETRANSMITS && num_active_actions > 0; i++)
        {
            aeron_retransmit_action_t *action = &handler->retransmit_action_pool[i];

            if (AERON_RETRANSMIT_ACTION_STATE_LINGERING == action->state)
            {
                if (now_ns > action->expire_ns)
                {
                    const int64_t key = aeron_int64_to_ptr_hash_map_compound_key(action->term_id, action->term_offset);

                    action->state = AERON_RETRANSMIT_ACTION_STATE_INACTIVE;
                    aeron_int64_to_ptr_hash_map_remove(&handler->active_retransmits_map, key);
                    result++;
                }

                num_active_actions--;
            }
        }
    }

    return result;
}
