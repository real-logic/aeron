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

#ifndef AERON_ARCHIVE_H
#define AERON_ARCHIVE_H

#include "aeronc.h"
#include "aeron_common.h"

#include <stdio.h> // TODO remove this

typedef struct aeron_archive_stct aeron_archive_t;
typedef struct aeron_archive_context_stct aeron_archive_context_t;
typedef struct aeron_archive_async_connect_stct aeron_archive_async_connect_t;

typedef struct aeron_archive_replay_params_stct
{
    int32_t bounding_limit_counter_id;
    int32_t file_io_max_length;
    int64_t position;
    int64_t length;
    int64_t replay_token;
    int64_t subscription_registration_id;
}
aeron_archive_replay_params_t;

typedef struct aeron_archive_encoded_credentials_stct
{
    const char *data;
    uint32_t length;
}
aeron_archive_encoded_credentials_t;

typedef aeron_archive_encoded_credentials_t *(*aeron_archive_credentials_encoded_credentials_supplier_func_t)(void *clientd);
typedef aeron_archive_encoded_credentials_t *(*aeron_archive_credentials_challenge_supplier_func_t)(
    aeron_archive_encoded_credentials_t *encoded_challenge,
    void *clientd);
typedef void (*aeron_archive_credentials_free_func_t)(
    aeron_archive_encoded_credentials_t *credentials,
    void *clientd);

typedef void (*aeron_archive_recording_descriptor_consumer_func_t)(
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t start_timestamp,
    int64_t stop_timestamp,
    int64_t start_position,
    int64_t stop_position,
    int32_t initial_term_id,
    int32_t segment_file_length,
    int32_t term_buffer_length,
    int32_t mtu_length,
    int32_t session_id,
    int32_t stream_id,
    const char *stripped_channel,
    const char *original_channel,
    const char *source_identity,
    void *clientd);

typedef void (*aeron_archive_recording_subscription_descriptor_consumer_func_t)(
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t subscription_id,
    int32_t stream_id,
    const char *stripped_channel,
    void *clientd);

typedef enum aeron_archive_source_location_en
{
    AERON_ARCHIVE_SOURCE_LOCATION_LOCAL = 0,
    AERON_ARCHIVE_SOURCE_LOCATION_REMOTE = 1
}
aeron_archive_source_location_t;

int aeron_archive_context_init(aeron_archive_context_t **ctx);
int aeron_archive_context_close(aeron_archive_context_t *ctx);

int aeron_archive_context_set_message_timeout_ns(aeron_archive_context_t *ctx, int64_t message_timeout_ns);
int aeron_archive_context_set_idle_strategy(
    aeron_archive_context_t *ctx,
    aeron_idle_strategy_func_t idle_strategy_func,
    void *idle_strategy_state);
int aeron_archive_context_set_credentials_supplier(
    aeron_archive_context_t *ctx,
    aeron_archive_credentials_encoded_credentials_supplier_func_t encoded_credentials,
    aeron_archive_credentials_challenge_supplier_func_t  on_challenge,
    aeron_archive_credentials_free_func_t on_free,
    void *clientd);

int aeron_archive_async_connect(aeron_archive_async_connect_t **async, aeron_archive_context_t *ctx);
int aeron_archive_async_connect_poll(aeron_archive_t **aeron_archive, aeron_archive_async_connect_t *async);

int aeron_archive_connect(aeron_archive_t **aeron_archive, aeron_archive_context_t *ctx);

int aeron_archive_close(aeron_archive_t *aeron_archive);

int aeron_archive_start_recording(
    int64_t *subscription_id_p,
    aeron_archive_t *aeron_archive,
    const char *recording_channel,
    int32_t recording_stream_id,
    aeron_archive_source_location_t source_location);

int aeron_archive_get_recording_position(
    int64_t *recording_position_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id);

int aeron_archive_get_stop_position(
    int64_t *stop_position_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id);

int aeron_archive_get_max_recorded_position(
    int64_t *max_recorded_position_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id);

int aeron_archive_stop_recording(
    aeron_archive_t *aeron_archive,
    int64_t subscription_id);

int aeron_archive_find_last_matching_recording(
    int64_t *recording_id_p,
    aeron_archive_t *aeron_archive,
    int64_t min_recording_id,
    const char *channel_fragment,
    int32_t stream_id,
    int32_t session_id);

int aeron_archive_list_recording(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    aeron_archive_recording_descriptor_consumer_func_t recording_descriptor_consumer,
    void *recording_descriptor_consumer_clientd);

int aeron_archive_start_replay(
    int64_t *replay_session_id_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params);

int aeron_archive_replay(
    aeron_subscription_t **subscription_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params);

int aeron_archive_truncate_recording(
    int64_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    int64_t position);

int aeron_archive_stop_replay(
    aeron_archive_t *aeron_archive,
    int64_t replay_session_id);

int aeron_archive_list_recording_subscriptions(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    int32_t pseudo_index,
    int32_t subscription_count,
    const char *channel_fragment,
    int32_t stream_id,
    bool apply_stream_id,
    aeron_archive_recording_subscription_descriptor_consumer_func_t recording_subscription_descriptor_consumer,
    void *recording_subscription_descriptor_consumer_clientd);

aeron_t *aeron_archive_get_aeron(aeron_archive_t *aeron_archive);
int64_t aeron_archive_get_archive_id(aeron_archive_t *aeron_archive);
aeron_subscription_t *aeron_archive_get_control_response_subscription(aeron_archive_t *aeron_archive);

int aeron_archive_replay_params_init(aeron_archive_replay_params_t *params);

int32_t aeron_archive_recording_pos_find_counter_id_by_recording_id(aeron_counters_reader_t *counters_reader, int64_t recording_id);
int32_t aeron_archive_recording_pos_find_counter_id_by_session_id(aeron_counters_reader_t *counters_reader, int32_t session_id);
int64_t aeron_archive_recording_pos_get_recording_id(aeron_counters_reader_t *counters_reader, int32_t counter_id);
int aeron_archive_recording_pos_get_source_identity(aeron_counters_reader_t *counters_reader, int32_t counter_id, const char *dst, int32_t *len_p);
int aeron_archive_recording_pos_is_active(aeron_counters_reader_t *counters_reader, int32_t counter_id, int64_t recording_id, bool *is_active);

#endif //AERON_ARCHIVE_H
