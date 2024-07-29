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

#include <sys/errno.h>

#include "aeron_archive.h"
#include "aeron_archive_context.h"
#include "aeron_archive_client.h"

#include "aeronc.h"
#include "aeron_alloc.h"
#include "aeron_agent.h"
#include "util/aeron_error.h"
#include "uri/aeron_uri.h"

#define ENSURE_NOT_REENTRANT_CHECK_RETURN(_aa, _rc) \
do { \
    if ((_aa)->is_in_callback) \
    { \
        AERON_SET_ERR(-1, "%s", "client cannot be invoked within callback"); \
        return (_rc); \
    } \
} while (0)

struct aeron_archive_stct
{
    aeron_archive_context_t *ctx;
    aeron_mutex_t lock;
    aeron_archive_proxy_t *archive_proxy;
    aeron_subscription_t *subscription; // shared by various pollers
    aeron_archive_control_response_poller_t *control_response_poller;
    aeron_archive_recording_descriptor_poller_t *recording_descriptor_poller;
    aeron_archive_recording_subscription_descriptor_poller_t *recording_subscription_descriptor_poller;
    aeron_t *aeron;
    int64_t control_session_id;
    int64_t archive_id;
    bool is_in_callback;
};

int aeron_archive_poll_for_response(
    int64_t *relevant_id,
    aeron_archive_t *aeron_archive,
    const char *operation_name,
    int64_t correlation_id);

int aeron_archive_poll_for_descriptors(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    const char *operation_name,
    int64_t correlation_id,
    int32_t record_count,
    aeron_archive_recording_descriptor_consumer_func_t recording_descriptor_consumer,
    void *recording_descriptor_consumer_clientd);

int aeron_archive_poll_for_subscription_descriptors(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    const char *operation_name,
    int64_t correlation_id,
    int32_t subscription_count,
    aeron_archive_recording_subscription_descriptor_consumer_func_t recording_subscription_descriptor_consumer,
    void *recording_subscription_descriptor_consumer_clientd);

/* **************** */

int aeron_archive_connect(aeron_archive_t **aeron_archive, aeron_archive_context_t *ctx /*, TODO my-cool-idle-strategy */)
{
    aeron_archive_async_connect_t *async;

    // TODO ensure *aeron_archive == NULL

    const uint64_t idle_duration_ns = UINT64_C(1000) * UINT64_C(1000); /* 1ms */

    if (aeron_archive_async_connect(&async, ctx) < 0)
    {
        // TODO
        return -1;
    }

    if (aeron_archive_async_connect_poll(aeron_archive, async) < 0)
    {
        // TODO
        return -1;
    }

    while (NULL == *aeron_archive)
    {
        aeron_idle_strategy_sleeping_idle((void *)&idle_duration_ns, 0);

        if (aeron_archive_async_connect_poll(aeron_archive, async) < 0)
        {
            // TODO
            return -1;
        }

        // _poll returned either 0 or 1
        // 0 leaves *aeron_archive as NULL, which is basically 'continue'
        // 1 sets *aeron_archive, which is basically a 'break'
        // if we sit in this loop for 'too long', eventually _poll will return -1
    }

    return 0;
}

int aeron_archive_create(
    aeron_archive_t **aeron_archive,
    aeron_archive_context_t *ctx,
    aeron_archive_proxy_t *archive_proxy,
    aeron_subscription_t *subscription,
    aeron_archive_control_response_poller_t *control_response_poller,
    aeron_archive_recording_descriptor_poller_t *recording_descriptor_poller,
    aeron_archive_recording_subscription_descriptor_poller_t *recording_subscription_descriptor_poller,
    aeron_t *aeron,
    int64_t control_session_id,
    int64_t archive_id)
{
    aeron_archive_t *_aeron_archive = NULL;

    if (aeron_alloc((void **)&_aeron_archive, sizeof(aeron_archive_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_t");
        return -1;
    }

    _aeron_archive->ctx = ctx;

    aeron_mutex_init(&_aeron_archive->lock, NULL);

    _aeron_archive->archive_proxy = archive_proxy;
    _aeron_archive->subscription = subscription;
    _aeron_archive->control_response_poller = control_response_poller;
    _aeron_archive->recording_descriptor_poller = recording_descriptor_poller;
    _aeron_archive->recording_subscription_descriptor_poller = recording_subscription_descriptor_poller;
    _aeron_archive->aeron = aeron;
    _aeron_archive->control_session_id = control_session_id;
    _aeron_archive->archive_id = archive_id;
    _aeron_archive->is_in_callback = false;

    *aeron_archive = _aeron_archive;
    return 0;
}

int aeron_archive_close(aeron_archive_t *aeron_archive)
{
    aeron_archive_proxy_close(aeron_archive->archive_proxy);
    aeron_archive->archive_proxy = NULL;

    aeron_archive_control_response_poller_close(aeron_archive->control_response_poller);
    aeron_archive->control_response_poller = NULL;

    aeron_archive_recording_descriptor_poller_close(aeron_archive->recording_descriptor_poller);
    aeron_archive->recording_descriptor_poller = NULL;

    aeron_archive_recording_subscription_descriptor_poller_close(aeron_archive->recording_subscription_descriptor_poller);
    aeron_archive->recording_subscription_descriptor_poller = NULL;

    aeron_subscription_close(aeron_archive->subscription, NULL, NULL);
    aeron_archive->subscription = NULL;

    aeron_mutex_destroy(&aeron_archive->lock);

    aeron_free(aeron_archive);

    return 0;
}

void aeron_archive_idle(aeron_archive_t *aeron_archive)
{
    aeron_archive_context_idle(aeron_archive->ctx);
}

int aeron_archive_start_recording(
    int64_t *subscription_id_p,
    aeron_archive_t *aeron_archive,
    const char *recording_channel,
    int32_t recording_stream_id,
    aeron_archive_source_location_t source_location)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_next_correlation_id(aeron_archive->aeron);

    if (!aeron_archive_proxy_start_recording(
        aeron_archive->archive_proxy,
        recording_channel,
        recording_stream_id,
        source_location == AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        correlation_id,
        aeron_archive->control_session_id))
    {
        aeron_mutex_unlock(&aeron_archive->lock);
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    int rc = aeron_archive_poll_for_response(
        subscription_id_p,
        aeron_archive,
        "AeronArchive::startRecording",
        correlation_id);

    aeron_mutex_unlock(&aeron_archive->lock);

    return rc;
}

int aeron_archive_get_recording_position(
    int64_t *recording_position_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_next_correlation_id(aeron_archive->aeron);

    if (!aeron_archive_proxy_get_recording_position(
        aeron_archive->archive_proxy,
        aeron_archive->control_session_id,
        correlation_id,
        recording_id))
    {
        aeron_mutex_unlock(&aeron_archive->lock);
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    int rc = aeron_archive_poll_for_response(
        recording_position_p,
        aeron_archive,
        "AeronArchive::getRecordingPosition",
        correlation_id);

    aeron_mutex_unlock(&aeron_archive->lock);

    return rc;
}

int aeron_archive_get_stop_position(
    int64_t *stop_position_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_next_correlation_id(aeron_archive->aeron);

    if (!aeron_archive_proxy_get_stop_position(
        aeron_archive->archive_proxy,
        aeron_archive->control_session_id,
        correlation_id,
        recording_id))
    {
        aeron_mutex_unlock(&aeron_archive->lock);
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    int rc = aeron_archive_poll_for_response(
        stop_position_p,
        aeron_archive,
        "AeronArchive::getStopPosition",
        correlation_id);

    aeron_mutex_unlock(&aeron_archive->lock);

    return rc;
}

int aeron_archive_get_max_recorded_position(
    int64_t *max_recorded_position_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_next_correlation_id(aeron_archive->aeron);

    if (!aeron_archive_proxy_get_max_recorded_position(
        aeron_archive->archive_proxy,
        aeron_archive->control_session_id,
        correlation_id,
        recording_id))
    {
        aeron_mutex_unlock(&aeron_archive->lock);
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    int rc = aeron_archive_poll_for_response(
        max_recorded_position_p,
        aeron_archive,
        "AeronArchive::getMaxRecordedPosition",
        correlation_id);

    aeron_mutex_unlock(&aeron_archive->lock);

    return rc;
}

int aeron_archive_stop_recording(
    aeron_archive_t *aeron_archive,
    int64_t subscription_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_next_correlation_id(aeron_archive->aeron);

    if (!aeron_archive_proxy_stop_recording(
        aeron_archive->archive_proxy,
        aeron_archive->control_session_id,
        correlation_id,
        subscription_id))
    {
        aeron_mutex_unlock(&aeron_archive->lock);
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    int rc = aeron_archive_poll_for_response(
        NULL,
        aeron_archive,
        "AeronArchive::stopRecording",
        correlation_id);

    aeron_mutex_unlock(&aeron_archive->lock);

    return rc;
}

int aeron_archive_find_last_matching_recording(
    int64_t *recording_id_p,
    aeron_archive_t *aeron_archive,
    int64_t min_recording_id,
    const char *channel_fragment,
    int32_t stream_id,
    int32_t session_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_next_correlation_id(aeron_archive->aeron);

    if (!aeron_archive_proxy_find_last_matching_recording(
        aeron_archive->archive_proxy,
        aeron_archive->control_session_id,
        correlation_id,
        min_recording_id,
        channel_fragment,
        stream_id,
        session_id))
    {
        aeron_mutex_unlock(&aeron_archive->lock);
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    int rc = aeron_archive_poll_for_response(
        recording_id_p,
        aeron_archive,
        "AeronArchive::findLastMatchingRecording",
        correlation_id);

    aeron_mutex_unlock(&aeron_archive->lock);

    return rc;
}

int aeron_archive_list_recording(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    aeron_archive_recording_descriptor_consumer_func_t recording_descriptor_consumer,
    void *recording_descriptor_consumer_clientd)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_next_correlation_id(aeron_archive->aeron);

    if (!aeron_archive_proxy_list_recording(
        aeron_archive->archive_proxy,
        aeron_archive->control_session_id,
        correlation_id,
        recording_id))
    {
        aeron_mutex_unlock(&aeron_archive->lock);
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    aeron_archive->is_in_callback = true;

    int rc = aeron_archive_poll_for_descriptors(
        count_p,
        aeron_archive,
        "AeronArchive::listRecording",
        correlation_id,
        1,
        recording_descriptor_consumer,
        recording_descriptor_consumer_clientd);

    aeron_archive->is_in_callback = false;

    aeron_mutex_unlock(&aeron_archive->lock);

    return rc;
}

int aeron_archive_start_replay(
    int64_t *replay_session_id_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    // TODO check replay channel for control mode response
    // ... but for now assume it's not present

    int64_t correlation_id = aeron_next_correlation_id(aeron_archive->aeron);

    if (!aeron_archive_proxy_replay(
        aeron_archive->archive_proxy,
        aeron_archive->control_session_id,
        correlation_id,
        recording_id,
        replay_channel,
        replay_stream_id,
        params))
    {
        aeron_mutex_unlock(&aeron_archive->lock);
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    int rc = aeron_archive_poll_for_response(
        replay_session_id_p,
        aeron_archive,
        "AeronArchive::startReplay",
        correlation_id);

    aeron_mutex_unlock(&aeron_archive->lock);

    return rc;
}

int aeron_archive_replay(
    aeron_subscription_t **subscription_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    // TODO check replay channel for control mode response
    // ... but for now assume it's not present

    int64_t correlation_id = aeron_next_correlation_id(aeron_archive->aeron);

    if (!aeron_archive_proxy_replay(
        aeron_archive->archive_proxy,
        aeron_archive->control_session_id,
        correlation_id,
        recording_id,
        replay_channel,
        replay_stream_id,
        params))
    {
        aeron_mutex_unlock(&aeron_archive->lock);
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    int64_t replay_session_id;

    if (aeron_archive_poll_for_response(
        &replay_session_id,
        aeron_archive,
        "AeronArchive::replay",
        correlation_id) < 0)
    {
        aeron_mutex_unlock(&aeron_archive->lock);
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    char replay_channel_with_sid[AERON_MAX_PATH + 1];
    memset(replay_channel_with_sid, '\0', AERON_MAX_PATH + 1);
    {
        aeron_uri_string_put_int64(
            replay_channel,
            strlen(replay_channel),
            replay_channel_with_sid,
            AERON_MAX_PATH + 1,
            AERON_URI_SESSION_ID_KEY,
            (int32_t)replay_session_id);
    }

    memset(replay_channel_with_sid, '\0', AERON_MAX_PATH + 1);
    {
        aeron_uri_t uri;

        aeron_uri_parse(strlen(replay_channel), replay_channel, &uri);
        aeron_uri_put_int64(&uri, AERON_URI_SESSION_ID_KEY, (int32_t)replay_session_id);
        aeron_uri_sprint(&uri, replay_channel_with_sid, AERON_MAX_PATH + 1);
        aeron_uri_close(&uri);

    }

    aeron_async_add_subscription_t *async_add_subscription;
    aeron_subscription_t *subscription = NULL;

    if (aeron_async_add_subscription(
        &async_add_subscription,
        aeron_archive->aeron,
        replay_channel_with_sid,
        replay_stream_id,
        NULL,
        NULL,
        NULL,
        NULL) < 0)
    {
        aeron_mutex_unlock(&aeron_archive->lock);
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (aeron_async_add_subscription_poll(&subscription, async_add_subscription) < 0)
    {
        aeron_mutex_unlock(&aeron_archive->lock);
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    while (NULL == subscription)
    {
        aeron_archive_idle(aeron_archive);

        if (aeron_async_add_subscription_poll(&subscription, async_add_subscription) < 0)
        {
            aeron_mutex_unlock(&aeron_archive->lock);
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
    }

    *subscription_p = subscription;

    aeron_mutex_unlock(&aeron_archive->lock);

    return 0;
}

int aeron_archive_truncate_recording(
    int64_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    int64_t position)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_next_correlation_id(aeron_archive->aeron);

    if (!aeron_archive_proxy_truncate_recording(
        aeron_archive->archive_proxy,
        aeron_archive->control_session_id,
        correlation_id,
        recording_id,
        position))
    {
        aeron_mutex_unlock(&aeron_archive->lock);
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    int rc = aeron_archive_poll_for_response(
        count_p,
        aeron_archive,
        "AeronArchive::truncateRecording",
        correlation_id);

    aeron_mutex_unlock(&aeron_archive->lock);

    return rc;
}

int aeron_archive_stop_replay(
    aeron_archive_t *aeron_archive,
    int64_t replay_session_id)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_next_correlation_id(aeron_archive->aeron);

    if (!aeron_archive_proxy_stop_replay(
        aeron_archive->archive_proxy,
        aeron_archive->control_session_id,
        correlation_id,
        replay_session_id))
    {
        aeron_mutex_unlock(&aeron_archive->lock);
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    int rc = aeron_archive_poll_for_response(
        NULL,
        aeron_archive,
        "AeronArchive::stopReplay",
        correlation_id);

    aeron_mutex_unlock(&aeron_archive->lock);

    return rc;
}

int aeron_archive_list_recording_subscriptions(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    int32_t pseudo_index,
    int32_t subscription_count,
    const char *channel_fragment,
    int32_t stream_id,
    bool apply_stream_id,
    aeron_archive_recording_subscription_descriptor_consumer_func_t recording_subscription_descriptor_consumer,
    void *recording_subscription_descriptor_consumer_clientd)
{
    ENSURE_NOT_REENTRANT_CHECK_RETURN(aeron_archive, -1);
    aeron_mutex_lock(&aeron_archive->lock);

    int64_t correlation_id = aeron_next_correlation_id(aeron_archive->aeron);

    if (!aeron_archive_proxy_list_recording_subscriptions(
        aeron_archive->archive_proxy,
        aeron_archive->control_session_id,
        correlation_id,
        pseudo_index,
        subscription_count,
        channel_fragment,
        stream_id,
        apply_stream_id))
    {
        aeron_mutex_unlock(&aeron_archive->lock);
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    aeron_archive->is_in_callback = true;

    int rc = aeron_archive_poll_for_subscription_descriptors(
        count_p,
        aeron_archive,
        "AeronArchive::listRecordingSubscriptions",
        correlation_id,
        subscription_count,
        recording_subscription_descriptor_consumer,
        recording_subscription_descriptor_consumer_clientd);

    aeron_archive->is_in_callback = false;

    aeron_mutex_unlock(&aeron_archive->lock);

    return rc;
}

aeron_t *aeron_archive_get_aeron(aeron_archive_t *aeron_archive)
{
    return aeron_archive->aeron;
}

int64_t aeron_archive_get_archive_id(aeron_archive_t *aeron_archive)
{
    return aeron_archive->archive_id;
}

aeron_subscription_t *aeron_archive_get_control_response_subscription(aeron_archive_t *aeron_archive)
{
    return aeron_archive->subscription;
}

/* **************** */

int aeron_archive_poll_next_response(
    aeron_archive_t *aeron_archive,
    const char *operation_name,
    int64_t correlation_id,
    int64_t deadline_ns)
{
    aeron_archive_control_response_poller_t *poller = aeron_archive->control_response_poller;

    while (true)
    {
        int fragments = aeron_archive_control_response_poller_poll(poller);

        if (aeron_archive_control_response_poller_is_poll_complete(poller))
        {
            if (aeron_archive_control_response_poller_is_recording_signal(poller) &&
                aeron_archive_control_response_poller_control_session_id(poller) == aeron_archive->control_session_id)
            {
                // TODO dispatch recording signal???
                continue;
            }

            break;
        }

        if (fragments > 0)
        {
            continue;
        }

        {
            aeron_subscription_t *subscription;

            subscription = aeron_archive_control_response_poller_get_subscription(poller);
            if (!aeron_subscription_is_connected(subscription))
            {
                AERON_SET_ERR(-1, "%s", "subscription to archive is not connected");
                return -1;
            }
        }

        if (aeron_nano_clock() > deadline_ns)
        {
            AERON_SET_ERR(ETIMEDOUT, "%s awaiting response - correlationId=%llu", operation_name, correlation_id);
            return -1;
        }

        aeron_archive_idle(aeron_archive);

        // TODO invoke aeron client???
    }

    return 0;
}

int aeron_archive_poll_for_response(
    int64_t *relevant_id_p,
    aeron_archive_t *aeron_archive,
    const char *operation_name,
    int64_t correlation_id)
{
    aeron_archive_control_response_poller_t *poller = aeron_archive->control_response_poller;

    int64_t deadline_ns = aeron_nano_clock() + aeron_archive->ctx->message_timeout_ns;

    while (true)
    {
        if (aeron_archive_poll_next_response(
            aeron_archive,
            operation_name,
            correlation_id,
            deadline_ns) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        // make sure the session id matches
        if (aeron_archive_control_response_poller_control_session_id(poller) != aeron_archive->control_session_id)
        {
            // TODO invoke aeron client??
            continue;
        }

        if (aeron_archive_control_response_poller_is_code_error(poller))
        {
            if (aeron_archive_control_response_poller_correlation_id(poller) == correlation_id)
            {
                // got an error, and the correlation ids match
                AERON_SET_ERR(
                    -1,
                    "response for correlationId=%llu, error: %s",
                    correlation_id,
                    aeron_archive_control_response_poller_error_message(poller));
                return -1;
            }
            /* // TODO
            else if (error_handler(aeron_archive->ctx) != NULL)
            {

            }
             */
        }
        else if (aeron_archive_control_response_poller_correlation_id(poller) == correlation_id)
        {
            if (!aeron_archive_control_response_poller_is_code_ok(poller))
            {
                AERON_SET_ERR(-1, "unexpected response code: %i", aeron_archive_control_response_poller_code_value(poller));
                return -1;
            }

            if (NULL != relevant_id_p)
            {
                *relevant_id_p = aeron_archive_control_response_poller_relevant_id(poller);
            }

            return 0;
        }
    }
}

int aeron_archive_poll_for_descriptors(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    const char *operation_name,
    int64_t correlation_id,
    int32_t record_count,
    aeron_archive_recording_descriptor_consumer_func_t recording_descriptor_consumer,
    void *recording_descriptor_consumer_clientd)
{
    aeron_archive_recording_descriptor_poller_t *poller = aeron_archive->recording_descriptor_poller;

    int32_t existing_remain_count = record_count;

    int64_t deadline_ns = aeron_nano_clock() + aeron_archive->ctx->message_timeout_ns;

    aeron_archive_recording_descriptor_poller_reset(
        poller,
        correlation_id,
        record_count,
        recording_descriptor_consumer,
        recording_descriptor_consumer_clientd);

    while (true)
    {
        const int fragments = aeron_archive_recording_descriptor_poller_poll(poller);

        const int32_t remaining_record_count = aeron_archive_recording_descriptor_poller_remaining_record_count(poller);

        if (aeron_archive_recording_descriptor_poller_is_dispatch_complete(poller))
        {
            *count_p = record_count - remaining_record_count;

            return 0;
        }

        if (remaining_record_count != existing_remain_count)
        {
            existing_remain_count = remaining_record_count;

            deadline_ns = aeron_nano_clock() + aeron_archive->ctx->message_timeout_ns;
        }

        // TODO invoke aeron client

        if (fragments > 0)
        {
            continue;
        }

        if (!aeron_subscription_is_connected(aeron_archive->subscription))
        {
            // TODO
            return -1;
        }

        if (aeron_nano_clock() > deadline_ns)
        {
            AERON_SET_ERR(ETIMEDOUT, "%s awaiting recording descriptors - correlationId=%llu", operation_name, correlation_id);
            return -1;
        }

        aeron_archive_idle(aeron_archive);
    }
}

int aeron_archive_poll_for_subscription_descriptors(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    const char *operation_name,
    int64_t correlation_id,
    int32_t subscription_count,
    aeron_archive_recording_subscription_descriptor_consumer_func_t recording_subscription_descriptor_consumer,
    void *recording_subscription_descriptor_consumer_clientd)
{
    aeron_archive_recording_subscription_descriptor_poller_t *poller = aeron_archive->recording_subscription_descriptor_poller;

    int32_t existing_remain_count = subscription_count;

    int64_t deadline_ns = aeron_nano_clock() + aeron_archive->ctx->message_timeout_ns;

    aeron_archive_recording_subscription_descriptor_poller_reset(
        poller,
        correlation_id,
        subscription_count,
        recording_subscription_descriptor_consumer,
        recording_subscription_descriptor_consumer_clientd);

    while (true)
    {
        const int fragments = aeron_archive_recording_subscription_descriptor_poller_poll(poller);

        const int32_t remaining_subscription_count = aeron_archive_recording_subscription_descriptor_poller_remaining_subscription_count(poller);

        if (aeron_archive_recording_subscription_descriptor_poller_is_dispatch_complete(poller))
        {
            *count_p = subscription_count - remaining_subscription_count;

            return 0;
        }

        if (remaining_subscription_count != existing_remain_count)
        {
            existing_remain_count = remaining_subscription_count;

            deadline_ns = aeron_nano_clock() + aeron_archive->ctx->message_timeout_ns;
        }

        // TODO invoke aeron client

        if (fragments > 0)
        {
            continue;
        }

        if (!aeron_subscription_is_connected(aeron_archive->subscription))
        {
            // TODO
            return -1;
        }

        if (aeron_nano_clock() > deadline_ns)
        {
            AERON_SET_ERR(ETIMEDOUT, "%s awaiting recording descriptors - correlationId=%llu", operation_name, correlation_id);
            return -1;
        }

        aeron_archive_idle(aeron_archive);
    }
}
