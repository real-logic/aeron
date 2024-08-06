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

#ifndef AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_H
#define AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_H

#include "aeron_archive.h"

#include "aeronc.h"

#define AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_FRAGMENT_LIMIT_DEFAULT 10

typedef struct aeron_archive_control_response_poller_stct aeron_archive_control_response_poller_t;

int aeron_archive_control_response_poller_create(
    aeron_archive_control_response_poller_t **poller,
    aeron_subscription_t *subscription,
    int fragment_limit);

int aeron_archive_control_response_poller_close(aeron_archive_control_response_poller_t *poller);

aeron_subscription_t *aeron_archive_control_response_poller_get_subscription(aeron_archive_control_response_poller_t *poller);

int aeron_archive_control_response_poller_poll(aeron_archive_control_response_poller_t *poller);

bool aeron_archive_control_response_poller_is_poll_complete(aeron_archive_control_response_poller_t *poller);

bool aeron_archive_control_response_poller_is_recording_signal(aeron_archive_control_response_poller_t *poller);

bool aeron_archive_control_response_poller_was_challenged(aeron_archive_control_response_poller_t *poller);

bool aeron_archive_control_response_poller_is_code_ok(aeron_archive_control_response_poller_t *poller);

bool aeron_archive_control_response_poller_is_code_error(aeron_archive_control_response_poller_t *poller);

int aeron_archive_control_response_poller_code_value(aeron_archive_control_response_poller_t *poller);

int64_t aeron_archive_control_response_poller_correlation_id(aeron_archive_control_response_poller_t *poller);

int64_t aeron_archive_control_response_poller_control_session_id(aeron_archive_control_response_poller_t *poller);

int64_t aeron_archive_control_response_poller_relevant_id(aeron_archive_control_response_poller_t *poller);

int32_t aeron_archive_control_response_poller_version(aeron_archive_control_response_poller_t *poller);

char *aeron_archive_control_response_poller_error_message(aeron_archive_control_response_poller_t *poller);

aeron_archive_encoded_credentials_t *aeron_archive_control_response_poller_encoded_challenge(aeron_archive_control_response_poller_t *poller);

#endif // AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_H
