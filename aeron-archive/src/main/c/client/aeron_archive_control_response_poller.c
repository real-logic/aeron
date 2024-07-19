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

#include "aeron_archive.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

#include "c/aeron_archive_client/messageHeader.h"
#include "c/aeron_archive_client/controlResponse.h"
#include "c/aeron_archive_client/challenge.h"
#include "c/aeron_archive_client/recordingSignalEvent.h"

#define AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_ERROR_MESSAGE_MAX_LEN 10000 // TODO

struct aeron_archive_control_response_poller_stct
{
    aeron_subscription_t *subscription;
    int fragment_limit;
    aeron_controlled_fragment_assembler_t *fragment_assembler;

    int64_t control_session_id;
    int64_t correlation_id;
    int64_t relevant_id;
    int64_t recording_id;
    int64_t subscription_id;
    int64_t position;

    int32_t recording_signal_code;
    int32_t version;

    char error_message[AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_ERROR_MESSAGE_MAX_LEN];

    int code_value;

    bool is_poll_complete;
    bool is_code_ok;
    bool is_code_error;
    bool is_control_response;
    bool was_challenged;
    bool is_recording_signal;
};

void aeron_archive_control_response_poller_reset(aeron_archive_control_response_poller_t *poller);

aeron_controlled_fragment_handler_action_t aeron_archive_control_response_poller_on_fragment(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header);

/* *************** */

int aeron_archive_control_response_poller_create(
    aeron_archive_control_response_poller_t **poller,
    aeron_subscription_t *subscription,
    int fragment_limit)
{
    aeron_archive_control_response_poller_t *_poller = NULL;

    if (aeron_alloc((void **)&_poller, sizeof(aeron_archive_control_response_poller_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_control_response_poller_t");
        return -1;
    }

    _poller->subscription = subscription;
    _poller->fragment_limit = fragment_limit;

    if (aeron_controlled_fragment_assembler_create(
        &_poller->fragment_assembler,
        aeron_archive_control_response_poller_on_fragment,
        _poller) < 0)
    {
        AERON_APPEND_ERR("%s", "aeron_fragment_assembler_create\n");
        return -1;
    }

    aeron_archive_control_response_poller_reset(_poller);

    *poller = _poller;

    return 0;
}

int aeron_archive_control_response_poller_poll(aeron_archive_control_response_poller_t *poller)
{
    if (poller->is_poll_complete)
    {
        aeron_archive_control_response_poller_reset(poller);
    }

    return aeron_subscription_controlled_poll(
        poller->subscription,
        aeron_controlled_fragment_assembler_handler,
        poller->fragment_assembler,
        poller->fragment_limit);
}

aeron_subscription_t *aeron_archive_control_response_poller_get_subscription(aeron_archive_control_response_poller_t *poller)
{
    return poller->subscription;
}

bool aeron_archive_control_response_poller_is_poll_complete(aeron_archive_control_response_poller_t *poller)
{
    return poller->is_poll_complete;
}

bool aeron_archive_control_response_poller_was_challenged(aeron_archive_control_response_poller_t *poller)
{
    return poller->was_challenged;
}

bool aeron_archive_control_response_poller_is_code_ok(aeron_archive_control_response_poller_t *poller)
{
    return poller->is_code_ok;
}

bool aeron_archive_control_response_poller_is_code_error(aeron_archive_control_response_poller_t *poller)
{
    return poller->is_code_error;
}

int64_t aeron_archive_control_response_poller_correlation_id(aeron_archive_control_response_poller_t  *poller)
{
    return poller->correlation_id;
}

int64_t aeron_archive_control_response_poller_control_session_id(aeron_archive_control_response_poller_t  *poller)
{
    return poller->control_session_id;
}

int64_t aeron_archive_control_response_poller_relevant_id(aeron_archive_control_response_poller_t  *poller)
{
    return poller->relevant_id;
}

int32_t aeron_archive_control_response_poller_version(aeron_archive_control_response_poller_t  *poller)
{
    return poller->version;
}

char *aeron_archive_control_response_poller_error_message(aeron_archive_control_response_poller_t  *poller)
{
    return poller->error_message;
}

/* *************** */

void aeron_archive_control_response_poller_reset(aeron_archive_control_response_poller_t *poller)
{
    poller->control_session_id = AERON_NULL_VALUE;
    poller->correlation_id = AERON_NULL_VALUE;
    poller->relevant_id = AERON_NULL_VALUE;
    poller->recording_id = AERON_NULL_VALUE;
    poller->subscription_id = AERON_NULL_VALUE;
    poller->position = AERON_NULL_VALUE;

    poller->recording_signal_code = INT32_MIN;
    poller->version = 0;

    memset(poller->error_message, 0, AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_ERROR_MESSAGE_MAX_LEN);

    poller->code_value = -1;

    poller->is_poll_complete = false;
    poller->is_code_ok = false;
    poller->is_code_error = false;
    poller->is_control_response = false;
    poller->was_challenged = false;
    poller->is_recording_signal = false;
}

aeron_controlled_fragment_handler_action_t aeron_archive_control_response_poller_on_fragment(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    aeron_archive_control_response_poller_t *poller = (aeron_archive_control_response_poller_t *)clientd;

    if (poller->is_poll_complete)
    {
        return AERON_ACTION_ABORT;
    }

    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_messageHeader_wrap(
        &hdr,
        (char *)buffer,
        0,
        aeron_archive_client_messageHeader_sbe_schema_version(),
        length);

    uint16_t schema_id = aeron_archive_client_messageHeader_schemaId(&hdr);

    if (schema_id != aeron_archive_client_messageHeader_sbe_schema_id())
    {
        // TODO
    }

    uint16_t template_id = aeron_archive_client_messageHeader_templateId(&hdr);

    switch(template_id)
    {
        case AERON_ARCHIVE_CLIENT_CONTROL_RESPONSE_SBE_TEMPLATE_ID:
        {
            struct aeron_archive_client_controlResponse control_response;

            aeron_archive_client_controlResponse_wrap_for_decode(
                &control_response,
                (char *)buffer,
                aeron_archive_client_messageHeader_encoded_length(),
                aeron_archive_client_controlResponse_sbe_block_length(),
                aeron_archive_client_controlResponse_sbe_schema_version(),
                length);

            poller->control_session_id = aeron_archive_client_controlResponse_controlSessionId(&control_response);
            poller->correlation_id = aeron_archive_client_controlResponse_correlationId(&control_response);
            poller->relevant_id = aeron_archive_client_controlResponse_relevantId(&control_response);
            poller->version = aeron_archive_client_controlResponse_version(&control_response);

            if (!aeron_archive_client_controlResponse_code(&control_response, &poller->code_value))
            {
                // TODO
            }

            poller->is_code_error = poller->code_value == aeron_archive_client_controlResponseCode_ERROR;
            poller->is_code_ok = poller->code_value == aeron_archive_client_controlResponseCode_OK;

            aeron_archive_client_controlResponse_get_errorMessage(
                &control_response,
                poller->error_message,
                AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_ERROR_MESSAGE_MAX_LEN);

            poller->is_control_response = true;
            poller->is_poll_complete = true;

            return AERON_ACTION_BREAK;
        }
        case AERON_ARCHIVE_CLIENT_CHALLENGE_SBE_TEMPLATE_ID:
            return AERON_ACTION_BREAK;
        case AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_EVENT_SBE_TEMPLATE_ID:
            return AERON_ACTION_BREAK;
        default:
            // do nothing
    }

    return AERON_ACTION_CONTINUE;
}
