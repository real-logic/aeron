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

#ifndef AERON_ARCHIVE_PROXY_H
#define AERON_ARCHIVE_PROXY_H

#include "aeron_archive.h"
#include "aeronc.h"
#include "aeron_common.h"

#define AERON_ARCHIVE_PROXY_RETRY_ATTEMPTS_DEFAULT (3)

typedef struct aeron_archive_proxy_stct aeron_archive_proxy_t;

int aeron_archive_proxy_create(
    aeron_archive_proxy_t **archive_proxy,
    aeron_archive_context_t *ctx,
    aeron_exclusive_publication_t *exclusive_publication,
    int retry_attempts);

int aeron_archive_proxy_close(aeron_archive_proxy_t *archive_proxy);

bool aeron_archive_proxy_try_connect(
    aeron_archive_proxy_t *archive_proxy,
    const char *control_response_channel,
    int32_t control_response_stream_id,
    aeron_archive_encoded_credentials_t *encoded_credentials,
    int64_t correlation_id);

bool aeron_archive_proxy_archive_id(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t control_session_id);

bool aeron_archive_proxy_close_session(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id);

bool aeron_archive_proxy_start_recording(
    aeron_archive_proxy_t *archive_proxy,
    const char *recording_channel,
    int32_t recording_stream_id,
    bool localSource,
    int64_t correlation_id,
    int64_t control_session_id);

bool aeron_archive_proxy_get_recording_position(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t recording_id);

bool aeron_archive_proxy_get_stop_position(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t recording_id);

bool aeron_archive_proxy_get_max_recorded_position(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t recording_id);

bool aeron_archive_proxy_stop_recording(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t subscription_id);

bool aeron_archive_proxy_find_last_matching_recording(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t min_recording_id,
    const char *channel_fragment,
    int32_t stream_id,
    int32_t session_id);

bool aeron_archive_proxy_list_recording(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t recording_id);

#endif //AERON_ARCHIVE_PROXY_H
