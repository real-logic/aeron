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

#ifdef __cplusplus
extern "C"
{
#endif

#include "aeronc.h"
#include "aeron_common.h"

#define ARCHIVE_ERROR_CODE_GENERIC (0)
#define ARCHIVE_ERROR_CODE_ACTIVE_LISTING (1)
#define ARCHIVE_ERROR_CODE_ACTIVE_RECORDING (2)
#define ARCHIVE_ERROR_CODE_ACTIVE_SUBSCRIPTION (3)
#define ARCHIVE_ERROR_CODE_UNKNOWN_SUBSCRIPTION (4)
#define ARCHIVE_ERROR_CODE_UNKNOWN_RECORDING (5)
#define ARCHIVE_ERROR_CODE_UNKNOWN_REPLAY (6)
#define ARCHIVE_ERROR_CODE_MAX_REPLAYS (7)
#define ARCHIVE_ERROR_CODE_MAX_RECORDINGS (8)
#define ARCHIVE_ERROR_CODE_INVALID_EXTENSION (9)
#define ARCHIVE_ERROR_CODE_AUTHENTICATION_REJECTED (10)
#define ARCHIVE_ERROR_CODE_STORAGE_SPACE (11)
#define ARCHIVE_ERROR_CODE_UNKNOWN_REPLICATION (12)
#define ARCHIVE_ERROR_CODE_UNAUTHORISED_ACTION (13)

typedef struct aeron_archive_stct aeron_archive_t;
typedef struct aeron_archive_context_stct aeron_archive_context_t;
typedef struct aeron_archive_async_connect_stct aeron_archive_async_connect_t;

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

typedef void (*aeron_archive_delegating_invoker_func_t)(void *clientd);

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

int aeron_archive_replay_params_init(aeron_archive_replay_params_t *params);

typedef struct aeron_archive_replication_params_stct
{
    int64_t stop_position;
    int64_t dst_recording_id;
    const char *live_destination;
    const char *replication_channel;
    const char *src_response_channel;
    int64_t channel_tag_id;
    int64_t subscription_tag_id;
    int32_t file_io_max_length;
    int32_t replication_session_id;
    aeron_archive_encoded_credentials_t *encoded_credentials;
}
aeron_archive_replication_params_t;

int aeron_archive_replication_params_init(aeron_archive_replication_params_t *params);

typedef struct aeron_archive_recording_descriptor_stct
{
    int64_t control_session_id;
    int64_t correlation_id;
    int64_t recording_id;
    int64_t start_timestamp;
    int64_t stop_timestamp;
    int64_t start_position;
    int64_t stop_position;
    int32_t initial_term_id;
    int32_t segment_file_length;
    int32_t term_buffer_length;
    int32_t mtu_length;
    int32_t session_id;
    int32_t stream_id;
    char *stripped_channel;
    size_t stripped_channel_length;
    char *original_channel;
    size_t original_channel_length;
    char *source_identity;
    size_t source_identity_length;
}
aeron_archive_recording_descriptor_t;

typedef void (*aeron_archive_recording_descriptor_consumer_func_t)(
    aeron_archive_recording_descriptor_t *recording_descriptor,
    void *clientd);

typedef struct aeron_archive_recording_subscription_descriptor_stct
{
    int64_t control_session_id;
    int64_t correlation_id;
    int64_t subscription_id;
    int32_t stream_id;
    char *stripped_channel;
    size_t stripped_channel_length;
}
aeron_archive_recording_subscription_descriptor_t;

typedef void (*aeron_archive_recording_subscription_descriptor_consumer_func_t)(
    aeron_archive_recording_subscription_descriptor_t *recording_subscription_descriptor,
    void *clientd);

typedef struct aeron_archive_recording_signal_stct
{
    int64_t control_session_id;
    int64_t recording_id;
    int64_t subscription_id;
    int64_t position;
    int32_t recording_signal_code;
}
aeron_archive_recording_signal_t;

typedef void (*aeron_archive_recording_signal_consumer_func_t)(
    aeron_archive_recording_signal_t *recording_signal,
    void *clientd);

typedef enum aeron_archive_source_location_en
{
    AERON_ARCHIVE_SOURCE_LOCATION_LOCAL = 0,
    AERON_ARCHIVE_SOURCE_LOCATION_REMOTE = 1
}
aeron_archive_source_location_t;

typedef enum aeron_archive_client_recording_signal_en
{
    AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_START = INT32_C(0),
    AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_STOP = INT32_C(1),
    AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_EXTEND = INT32_C(2),
    AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_REPLICATE = INT32_C(3),
    AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_MERGE = INT32_C(4),
    AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_SYNC = INT32_C(5),
    AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_DELETE = INT32_C(6),
    AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_REPLICATE_END = INT32_C(7),
    AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_NULL_VALUE = INT32_MIN
}
aeron_archive_client_recording_signal_t;

/* context */

int aeron_archive_context_init(aeron_archive_context_t **ctx);
int aeron_archive_context_close(aeron_archive_context_t *ctx);

int aeron_archive_context_set_aeron(aeron_archive_context_t *ctx, aeron_t *aeron);
aeron_t *aeron_archive_context_get_aeron(aeron_archive_context_t *ctx);

int aeron_archive_context_set_owns_aeron_client(aeron_archive_context_t *ctx, bool owns_aeron_client);
bool aeron_archive_context_get_owns_aeron_client(aeron_archive_context_t *ctx);

int aeron_archive_context_set_aeron_directory_name(aeron_archive_context_t *ctx, const char *aeron_directory_name);
const char *aeron_archive_context_get_aeron_directory_name(aeron_archive_context_t *ctx);

int aeron_archive_context_set_control_request_channel(aeron_archive_context_t *ctx, const char *control_request_channel);
const char *aeron_archive_context_get_control_request_channel(aeron_archive_context_t *ctx);

int aeron_archive_context_set_control_request_stream_id(aeron_archive_context_t *ctx, int32_t control_request_stream_id);
int32_t aeron_archive_context_get_control_request_stream_id(aeron_archive_context_t *ctx);

int aeron_archive_context_set_control_response_channel(aeron_archive_context_t *ctx, const char *control_response_channel);
const char *aeron_archive_context_get_control_response_channel(aeron_archive_context_t *ctx);

int aeron_archive_context_set_control_response_stream_id(aeron_archive_context_t *ctx, int32_t control_response_stream_id);
int32_t aeron_archive_context_get_control_response_stream_id(aeron_archive_context_t *ctx);

int aeron_archive_context_set_message_timeout_ns(aeron_archive_context_t *ctx, int64_t message_timeout_ns);
int64_t aeron_archive_context_get_message_timeout_ns(aeron_archive_context_t *ctx);

int aeron_archive_context_set_idle_strategy(
    aeron_archive_context_t *ctx,
    aeron_idle_strategy_func_t idle_strategy_func,
    void *idle_strategy_state);

int aeron_archive_context_set_credentials_supplier(
    aeron_archive_context_t *ctx,
    aeron_archive_credentials_encoded_credentials_supplier_func_t encoded_credentials,
    aeron_archive_credentials_challenge_supplier_func_t on_challenge,
    aeron_archive_credentials_free_func_t on_free,
    void *clientd);

int aeron_archive_context_set_recording_signal_consumer(
    aeron_archive_context_t *ctx,
    aeron_archive_recording_signal_consumer_func_t on_recording_signal,
    void *clientd);

int aeron_archive_context_set_error_handler(
    aeron_archive_context_t *ctx,
    aeron_error_handler_t error_handler,
    void *clientd);

int aeron_archive_context_set_delegating_invoker(
    aeron_archive_context_t *ctx,
    aeron_archive_delegating_invoker_func_t delegating_invoker_func,
    void *clientd);

/* client */

int aeron_archive_async_connect(aeron_archive_async_connect_t **async, aeron_archive_context_t *ctx);
int aeron_archive_async_connect_poll(aeron_archive_t **aeron_archive, aeron_archive_async_connect_t *async);

int aeron_archive_connect(aeron_archive_t **aeron_archive, aeron_archive_context_t *ctx);

int aeron_archive_close(aeron_archive_t *aeron_archive);

aeron_archive_context_t *aeron_archive_get_archive_context(aeron_archive_t *aeron_archive);
aeron_archive_context_t *aeron_archive_get_and_own_archive_context(aeron_archive_t *aeron_archive);

int64_t aeron_archive_get_archive_id(aeron_archive_t *aeron_archive);

aeron_subscription_t *aeron_archive_get_control_response_subscription(aeron_archive_t *aeron_archive);
aeron_subscription_t *aeron_archive_get_and_own_control_response_subscription(aeron_archive_t *aeron_archive);

int aeron_archive_poll_for_recording_signals(int32_t *count_p, aeron_archive_t *aeron_archive);
int aeron_archive_poll_for_error_response(aeron_archive_t *aeron_archive, char *buffer, size_t buffer_length);
int aeron_archive_check_for_error_response(aeron_archive_t *aeron_archive);

int aeron_archive_add_recorded_publication(
    aeron_publication_t **publication_p,
    aeron_archive_t *aeron_archive,
    const char *channel,
    int32_t stream_id);

int aeron_archive_add_recorded_exclusive_publication(
    aeron_exclusive_publication_t **exclusive_publication_p,
    aeron_archive_t *aeron_archive,
    const char *channel,
    int32_t stream_id);

int aeron_archive_start_recording(
    int64_t *subscription_id_p,
    aeron_archive_t *aeron_archive,
    const char *recording_channel,
    int32_t recording_stream_id,
    aeron_archive_source_location_t source_location,
    bool auto_stop);

int aeron_archive_get_recording_position(
    int64_t *recording_position_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id);

int aeron_archive_get_start_position(
    int64_t *start_position_p,
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

int aeron_archive_stop_recording_subscription(
    aeron_archive_t *aeron_archive,
    int64_t subscription_id);

int aeron_archive_try_stop_recording_subscription(
    bool *stopped_p,
    aeron_archive_t *aeron_archive,
    int64_t subscription_id);

int aeron_archive_stop_recording_channel_and_stream(
    aeron_archive_t *aeron_archive,
    const char *channel,
    int32_t stream_id);

int aeron_archive_try_stop_recording_channel_and_stream(
    bool *stopped_p,
    aeron_archive_t *aeron_archive,
    const char *channel,
    int32_t stream_id);

int aeron_archive_try_stop_recording_by_identity(
    bool *stopped_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id);

int aeron_archive_stop_recording_publication(
    aeron_archive_t *aeron_archive,
    aeron_publication_t *publication);

int aeron_archive_stop_recording_exclusive_publication(
    aeron_archive_t *aeron_archive,
    aeron_exclusive_publication_t *exclusive_publication);

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

int aeron_archive_list_recordings(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t from_recording_id,
    int32_t record_count,
    aeron_archive_recording_descriptor_consumer_func_t recording_descriptor_consumer,
    void *recording_descriptor_consumer_clientd);

int aeron_archive_list_recordings_for_uri(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t from_recording_id,
    int32_t record_count,
    const char *channel_fragment,
    int32_t stream_id,
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

int aeron_archive_stop_all_replays(
    aeron_archive_t *aeron_archive,
    int64_t recording_id);

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

int aeron_archive_purge_recording(
    int64_t *deleted_segments_count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id);

int aeron_archive_extend_recording(
    int64_t *subscription_id_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *recording_channel,
    int32_t recording_stream_id,
    aeron_archive_source_location_t source_location,
    bool auto_stop);

int aeron_archive_replicate(
    int64_t *replication_id_p,
    aeron_archive_t *aeron_archive,
    int64_t src_recording_id,
    int32_t src_control_stream_id,
    const char *src_control_channel,
    aeron_archive_replication_params_t *params);

int aeron_archive_stop_replication(
    aeron_archive_t *aeron_archive,
    int64_t replication_id);

int aeron_archive_try_stop_replication(
    bool *stopped_p,
    aeron_archive_t *aeron_archive,
    int64_t replication_id);

int aeron_archive_detach_segments(
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    int64_t new_start_position);

int aeron_archive_delete_detached_segments(
    int64_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id);

int aeron_archive_purge_segments(
    int64_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    int64_t new_start_position);

int aeron_archive_attach_segments(
    int64_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id);

int aeron_archive_migrate_segments(
    int64_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t src_recording_id,
    int64_t dst_recording_id);

int64_t aeron_archive_segment_file_base_position(
    int64_t start_position,
    int64_t position,
    int32_t term_buffer_length,
    int32_t segment_file_length);

int32_t aeron_archive_recording_pos_find_counter_id_by_recording_id(aeron_counters_reader_t *counters_reader, int64_t recording_id);
int32_t aeron_archive_recording_pos_find_counter_id_by_session_id(aeron_counters_reader_t *counters_reader, int32_t session_id);
int64_t aeron_archive_recording_pos_get_recording_id(aeron_counters_reader_t *counters_reader, int32_t counter_id);
int aeron_archive_recording_pos_get_source_identity(aeron_counters_reader_t *counters_reader, int32_t counter_id, const char *dst, size_t *len_p);
int aeron_archive_recording_pos_is_active(bool *is_active, aeron_counters_reader_t *counters_reader, int32_t counter_id, int64_t recording_id);

/* replay/merge */

typedef struct aeron_archive_replay_merge_stct aeron_archive_replay_merge_t;

#define REPLAY_MERGE_PROGRESS_TIMEOUT_DEFAULT_MS (5 * 1000)

int aeron_archive_replay_merge_init(
    aeron_archive_replay_merge_t **replay_merge,
    aeron_subscription_t *subscription,
    aeron_archive_t *aeron_archive,
    const char *replay_channel,
    const char *replay_destination,
    const char *live_destination,
    int64_t recording_id,
    int64_t start_position,
    long long epoch_clock,
    int64_t merge_progress_timeout_ms);
int aeron_archive_replay_merge_close(aeron_archive_replay_merge_t *replay_merge);

int aeron_archive_replay_merge_do_work(int *work_count_p, aeron_archive_replay_merge_t *replay_merge);
int aeron_archive_replay_merge_poll(
    aeron_archive_replay_merge_t *replay_merge,
    aeron_fragment_handler_t handler,
    void *clientd,
    int fragment_limit);
aeron_image_t *aeron_archive_replay_merge_image(aeron_archive_replay_merge_t *replay_merge);
bool aeron_archive_replay_merge_is_merged(aeron_archive_replay_merge_t *replay_merge);
bool aeron_archive_replay_merge_has_failed(aeron_archive_replay_merge_t *replay_merge);
bool aeron_archive_replay_merge_is_live_added(aeron_archive_replay_merge_t *replay_merge);

#ifdef __cplusplus
}
#endif

#endif //AERON_ARCHIVE_H
