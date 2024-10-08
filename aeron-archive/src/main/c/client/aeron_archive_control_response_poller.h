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

#ifndef AERON_C_ARCHIVE_CONTROL_RESPONSE_POLLER_H
#define AERON_C_ARCHIVE_CONTROL_RESPONSE_POLLER_H

#include "aeron_archive.h"

#include "aeronc.h"

#define AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_FRAGMENT_LIMIT_DEFAULT 10

typedef struct aeron_archive_control_response_poller_stct
{
    aeron_subscription_t *subscription;
    int fragment_limit;
    aeron_controlled_fragment_assembler_t *fragment_assembler;
    bool error_on_fragment;

    int64_t control_session_id;
    int64_t correlation_id;
    int64_t relevant_id;
    int64_t recording_id;
    int64_t subscription_id;
    int64_t position;

    int32_t recording_signal_code;
    int32_t version;

    char *error_message;
    uint32_t error_message_malloced_len;

    char *encoded_challenge_buffer;
    uint32_t encoded_challenge_buffer_malloced_len;

    aeron_archive_encoded_credentials_t encoded_challenge;

    int code_value;

    bool is_poll_complete;
    bool is_code_ok;
    bool is_code_error;
    bool is_control_response;
    bool was_challenged;
    bool is_recording_signal;
}
aeron_archive_control_response_poller_t;

int aeron_archive_control_response_poller_create(
    aeron_archive_control_response_poller_t **poller,
    aeron_subscription_t *subscription,
    int fragment_limit);

int aeron_archive_control_response_poller_close(aeron_archive_control_response_poller_t *poller);

int aeron_archive_control_response_poller_poll(aeron_archive_control_response_poller_t *poller);

#endif // AERON_C_ARCHIVE_CONTROL_RESPONSE_POLLER_H
