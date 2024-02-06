/*
 * Copyright 2014-2024 Real Logic Limited.
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

#include <string.h>
#include "concurrent/aeron_counters_manager.h"
#include "protocol/aeron_udp_protocol.h"
#include "util/aeron_error.h"
#include "aeron_retransmit_handler.h"

aeron_retransmit_action_t *aeron_retransmit_handler_scan_for_available_retransmit(
    aeron_retransmit_handler_t *handler,
    int32_t term_id,
    int32_t term_offset,
    size_t length);

int aeron_retransmit_handler_init(
    aeron_retransmit_handler_t *handler,
    int64_t *invalid_packets_counter,
    uint64_t delay_timeout_ns,
    uint64_t linger_timeout_ns)
{
    handler->invalid_packets_counter = invalid_packets_counter;
    handler->delay_timeout_ns = delay_timeout_ns;
    handler->linger_timeout_ns = linger_timeout_ns;

    for (size_t i = 0; i < AERON_RETRANSMIT_HANDLER_MAX_RETRANSMITS; i++)
    {
        handler->retransmit_action_pool[i].state = AERON_RETRANSMIT_ACTION_STATE_INACTIVE;
    }

    return 0;
}

int aeron_retransmit_handler_close(aeron_retransmit_handler_t *handler)
{
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
        aeron_retransmit_action_t *action = aeron_retransmit_handler_scan_for_available_retransmit(handler, term_id, term_offset, length);

        if (action != NULL)
        {
            const size_t term_length_left = term_length - term_offset;

            action->term_id = term_id;
            action->term_offset = term_offset;
            action->length = length < term_length_left ? length : term_length_left;

            if (0 == handler->delay_timeout_ns)
            {
                result = resend(resend_clientd, term_id, term_offset, action->length);
                action->state = AERON_RETRANSMIT_ACTION_STATE_LINGERING;
                action->expiry_ns = now_ns + (int64_t)handler->linger_timeout_ns;
            }
            else
            {
                action->state = AERON_RETRANSMIT_ACTION_STATE_DELAYED;
                action->expiry_ns = now_ns + (int64_t)handler->delay_timeout_ns;
            }
        }
    }

    return result;
}

int aeron_retransmit_handler_process_timeouts(
    aeron_retransmit_handler_t *handler,
    int64_t now_ns,
    aeron_retransmit_handler_resend_func_t resend,
    void *resend_clientd)
{
    int result = 0;

    for (size_t i = 0; i < AERON_RETRANSMIT_HANDLER_MAX_RETRANSMITS; i++)
    {
        aeron_retransmit_action_t *action = &handler->retransmit_action_pool[i];

        if (AERON_RETRANSMIT_ACTION_STATE_DELAYED == action->state)
        {
            if (now_ns > action->expiry_ns)
            {
                result = resend(resend_clientd, action->term_id, action->term_offset, action->length);
                action->state = AERON_RETRANSMIT_ACTION_STATE_LINGERING;
                action->expiry_ns = now_ns + (int64_t)handler->linger_timeout_ns;
                result++;
            }
        }
        else if (AERON_RETRANSMIT_ACTION_STATE_LINGERING == action->state)
        {
            if (now_ns > action->expiry_ns)
            {
                action->state = AERON_RETRANSMIT_ACTION_STATE_INACTIVE;
                result++;
            }
        }
    }

    return result;
}

aeron_retransmit_action_t *aeron_retransmit_handler_scan_for_available_retransmit(
    aeron_retransmit_handler_t *handler,
    int32_t term_id,
    int32_t term_offset,
    size_t length)
{
    aeron_retransmit_action_t *available_action = NULL;
    for (size_t i = 0; i < AERON_RETRANSMIT_HANDLER_MAX_RETRANSMITS; i++)
    {
        aeron_retransmit_action_t *action = &handler->retransmit_action_pool[i];

        switch (action->state)
        {
            case AERON_RETRANSMIT_ACTION_STATE_INACTIVE:
                if (NULL == available_action)
                {
                    available_action = action;
                }
                break;

            case AERON_RETRANSMIT_ACTION_STATE_DELAYED:
            case AERON_RETRANSMIT_ACTION_STATE_LINGERING:
                if (action->term_id == term_id &&
                    action->term_offset <= term_offset &&
                    term_offset + length <= action->term_offset + action->length)
                {
                    return NULL;
                }
                break;
        }
    }

    if (NULL != available_action)
    {
        return available_action;
    }

    // TODO aeron err??
    return NULL;
}
