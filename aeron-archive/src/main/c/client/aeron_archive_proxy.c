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

#include <errno.h>

#include "aeron_archive.h"
#include "aeron_archive_context.h"
#include "aeron_archive_proxy.h"
#include "aeron_archive_configuration.h"
#include "aeron_archive_replay_params.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

#include "c/aeron_archive_client/authConnectRequest.h"
#include "c/aeron_archive_client/archiveIdRequest.h"
#include "c/aeron_archive_client/closeSessionRequest.h"
#include "c/aeron_archive_client/startRecordingRequest.h"
#include "c/aeron_archive_client/recordingPositionRequest.h"
#include "c/aeron_archive_client/stopPositionRequest.h"
#include "c/aeron_archive_client/maxRecordedPositionRequest.h"
#include "c/aeron_archive_client/stopRecordingSubscriptionRequest.h"
#include "c/aeron_archive_client/findLastMatchingRecordingRequest.h"
#include "c/aeron_archive_client/listRecordingRequest.h"
#include "c/aeron_archive_client/boundedReplayRequest.h"
#include "c/aeron_archive_client/replayRequest.h"
#include "c/aeron_archive_client/truncateRecordingRequest.h"
#include "c/aeron_archive_client/stopReplayRequest.h"
#include "c/aeron_archive_client/listRecordingSubscriptionsRequest.h"

#define AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH (8 * 1024)

struct aeron_archive_proxy_stct
{
    aeron_archive_context_t *ctx;
    aeron_exclusive_publication_t *exclusive_publication;
    int retry_attempts;
    // TODO why bake a buffer into the archive_proxy_t?  Couldn't/shouldn't we just toss it on the stack?
    // This seems odd to me.  ... but that's how it was done in the C++ implementation...
    uint8_t buffer[AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH];
};

int64_t aeron_archive_proxy_offer_once(aeron_archive_proxy_t *archive_proxy, size_t length);

bool aeron_archive_proxy_offer(
    aeron_archive_proxy_t *archive_proxy,
    size_t length);

/* **************** */

int aeron_archive_proxy_create(
    aeron_archive_proxy_t **archive_proxy,
    aeron_archive_context_t *ctx,
    aeron_exclusive_publication_t *exclusive_publication,
    int retry_attempts)
{
    aeron_archive_proxy_t *_archive_proxy = NULL;

    if (aeron_alloc((void **)&_archive_proxy, sizeof(aeron_archive_proxy_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_proxy_t");
        return -1;
    }

    _archive_proxy->ctx = ctx;
    _archive_proxy->exclusive_publication = exclusive_publication;
    _archive_proxy->retry_attempts = retry_attempts;

    *archive_proxy = _archive_proxy;

    return 0;
}

int aeron_archive_proxy_close(aeron_archive_proxy_t *archive_proxy)
{
    aeron_exclusive_publication_close(archive_proxy->exclusive_publication, NULL, NULL);
    archive_proxy->exclusive_publication = NULL;

    aeron_free(archive_proxy);

    return 0;
}

bool aeron_archive_proxy_try_connect(
    aeron_archive_proxy_t *archive_proxy,
    const char *control_response_channel,
    int32_t control_response_stream_id,
    aeron_archive_encoded_credentials_t *encoded_credentials,
    int64_t correlation_id)
{
    struct aeron_archive_client_authConnectRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_authConnectRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_authConnectRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_authConnectRequest_set_responseStreamId(&codec, control_response_stream_id);
    aeron_archive_client_authConnectRequest_set_version(&codec, aeron_archive_semantic_version());
    aeron_archive_client_authConnectRequest_put_responseChannel(
        &codec,
        control_response_channel,
        strlen(control_response_channel));
    aeron_archive_client_authConnectRequest_put_encodedCredentials(
        &codec,
        encoded_credentials->data,
        encoded_credentials->length);

    return aeron_archive_proxy_offer_once(
        archive_proxy,
        aeron_archive_client_authConnectRequest_encoded_length(&codec)) > 0;
}

bool aeron_archive_proxy_archive_id(
    aeron_archive_proxy_t *archive_proxy,
    int64_t correlation_id,
    int64_t control_session_id)
{
    struct aeron_archive_client_archiveIdRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_archiveIdRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_archiveIdRequest_set_controlSessionId(&codec, control_session_id);
    aeron_archive_client_archiveIdRequest_set_correlationId(&codec, correlation_id);

    return aeron_archive_proxy_offer_once(
        archive_proxy,
        aeron_archive_client_archiveIdRequest_encoded_length(&codec)) > 0;
}

bool aeron_archive_proxy_close_session(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id)
{
    struct aeron_archive_client_closeSessionRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_closeSessionRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_closeSessionRequest_set_controlSessionId(&codec, control_session_id);


    return aeron_archive_proxy_offer_once(
        archive_proxy,
        aeron_archive_client_closeSessionRequest_encoded_length(&codec)) > 0;
}

bool aeron_archive_proxy_start_recording(
    aeron_archive_proxy_t *archive_proxy,
    const char *recording_channel,
    int32_t recording_stream_id,
    bool localSource,
    int64_t correlation_id,
    int64_t control_session_id)
{
    struct aeron_archive_client_startRecordingRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_startRecordingRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_startRecordingRequest_set_controlSessionId(&codec, control_session_id);
    aeron_archive_client_startRecordingRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_startRecordingRequest_set_streamId(&codec, recording_stream_id);
    aeron_archive_client_startRecordingRequest_set_sourceLocation(
        &codec,
        localSource ? aeron_archive_client_sourceLocation_LOCAL : aeron_archive_client_sourceLocation_REMOTE);
    aeron_archive_client_startRecordingRequest_put_channel(
        &codec,
        recording_channel,
        strlen(recording_channel));

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_startRecordingRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_get_recording_position(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t recording_id)
{
    struct aeron_archive_client_recordingPositionRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_recordingPositionRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_recordingPositionRequest_set_controlSessionId(&codec, control_session_id);
    aeron_archive_client_recordingPositionRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_recordingPositionRequest_set_recordingId(&codec, recording_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_recordingPositionRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_get_stop_position(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t recording_id)
{
    struct aeron_archive_client_stopPositionRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_stopPositionRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_stopPositionRequest_set_controlSessionId(&codec, control_session_id);
    aeron_archive_client_stopPositionRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_stopPositionRequest_set_recordingId(&codec, recording_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_stopPositionRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_get_max_recorded_position(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t recording_id)
{
    struct aeron_archive_client_maxRecordedPositionRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_maxRecordedPositionRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_maxRecordedPositionRequest_set_controlSessionId(&codec, control_session_id);
    aeron_archive_client_maxRecordedPositionRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_maxRecordedPositionRequest_set_recordingId(&codec, recording_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_maxRecordedPositionRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_stop_recording(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t subscription_id)
{
    struct aeron_archive_client_stopRecordingSubscriptionRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_stopRecordingSubscriptionRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_stopRecordingSubscriptionRequest_set_controlSessionId(&codec, control_session_id);
    aeron_archive_client_stopRecordingSubscriptionRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_stopRecordingSubscriptionRequest_set_subscriptionId(&codec, subscription_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_stopRecordingSubscriptionRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_find_last_matching_recording(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t min_recording_id,
    const char *channel_fragment,
    int32_t stream_id,
    int32_t session_id)
{
    struct aeron_archive_client_findLastMatchingRecordingRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_findLastMatchingRecordingRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_findLastMatchingRecordingRequest_set_controlSessionId(&codec, control_session_id);
    aeron_archive_client_findLastMatchingRecordingRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_findLastMatchingRecordingRequest_set_minRecordingId(&codec, min_recording_id);
    aeron_archive_client_findLastMatchingRecordingRequest_set_sessionId(&codec, session_id);
    aeron_archive_client_findLastMatchingRecordingRequest_set_streamId(&codec, stream_id);
    aeron_archive_client_findLastMatchingRecordingRequest_put_channel(
        &codec,
        channel_fragment,
        strlen(channel_fragment));

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_findLastMatchingRecordingRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_list_recording(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t recording_id)
{
    struct aeron_archive_client_listRecordingRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_listRecordingRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_listRecordingRequest_set_controlSessionId(&codec, control_session_id);
    aeron_archive_client_listRecordingRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_listRecordingRequest_set_recordingId(&codec, recording_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_listRecordingRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_replay(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params)
{
    size_t length;
    struct aeron_archive_client_messageHeader hdr;

    if (aeron_archive_replay_params_is_bounded(params))
    {
        struct aeron_archive_client_boundedReplayRequest codec;

        aeron_archive_client_boundedReplayRequest_wrap_and_apply_header(
            &codec,
            (char *)archive_proxy->buffer,
            0,
            AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
            &hdr);
        aeron_archive_client_boundedReplayRequest_set_controlSessionId(&codec, control_session_id);
        aeron_archive_client_boundedReplayRequest_set_correlationId(&codec, correlation_id);
        aeron_archive_client_boundedReplayRequest_set_recordingId(&codec, recording_id);
        aeron_archive_client_boundedReplayRequest_set_position(&codec, params->position);
        aeron_archive_client_boundedReplayRequest_set_length(&codec, params->length);
        aeron_archive_client_boundedReplayRequest_set_limitCounterId(&codec, params->bounding_limit_counter_id);
        aeron_archive_client_boundedReplayRequest_set_replayStreamId(&codec, replay_stream_id);
        aeron_archive_client_boundedReplayRequest_set_fileIoMaxLength(&codec, params->file_io_max_length);
        aeron_archive_client_boundedReplayRequest_set_replayToken(&codec, params->replay_token);
        aeron_archive_client_boundedReplayRequest_put_replayChannel(
            &codec,
           replay_channel,
            strlen(replay_channel));

        length = aeron_archive_client_boundedReplayRequest_encoded_length(&codec);
    }
    else
    {
        struct aeron_archive_client_replayRequest codec;

        aeron_archive_client_replayRequest_wrap_and_apply_header(
            &codec,
            (char *)archive_proxy->buffer,
            0,
            AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
            &hdr);
        aeron_archive_client_replayRequest_set_controlSessionId(&codec, control_session_id);
        aeron_archive_client_replayRequest_set_correlationId(&codec, correlation_id);
        aeron_archive_client_replayRequest_set_recordingId(&codec, recording_id);
        aeron_archive_client_replayRequest_set_position(&codec, params->position);
        aeron_archive_client_replayRequest_set_length(&codec, params->length);
        aeron_archive_client_replayRequest_set_replayStreamId(&codec, replay_stream_id);
        aeron_archive_client_replayRequest_set_fileIoMaxLength(&codec, params->file_io_max_length);
        aeron_archive_client_replayRequest_set_replayToken(&codec, params->replay_token);
        aeron_archive_client_replayRequest_put_replayChannel(
            &codec,
            replay_channel,
            strlen(replay_channel));

        length = aeron_archive_client_replayRequest_encoded_length(&codec);
    }

    return aeron_archive_proxy_offer(archive_proxy, length);
}

bool aeron_archive_proxy_truncate_recording(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t position)
{
    struct aeron_archive_client_truncateRecordingRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_truncateRecordingRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_truncateRecordingRequest_set_controlSessionId(&codec, control_session_id);
    aeron_archive_client_truncateRecordingRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_truncateRecordingRequest_set_recordingId(&codec, recording_id);
    aeron_archive_client_truncateRecordingRequest_set_position(&codec, position);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_truncateRecordingRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_stop_replay(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t replay_session_id)
{
    struct aeron_archive_client_stopReplayRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_stopReplayRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_stopReplayRequest_set_controlSessionId(&codec, control_session_id);
    aeron_archive_client_stopReplayRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_stopReplayRequest_set_replaySessionId(&codec, replay_session_id);

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_stopReplayRequest_encoded_length(&codec));
}

bool aeron_archive_proxy_list_recording_subscriptions(
    aeron_archive_proxy_t *archive_proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int32_t pseudo_index,
    int32_t subscription_count,
    const char *channel_fragment,
    int32_t stream_id,
    bool apply_stream_id)
{
    struct aeron_archive_client_listRecordingSubscriptionsRequest codec;
    struct aeron_archive_client_messageHeader hdr;

    aeron_archive_client_listRecordingSubscriptionsRequest_wrap_and_apply_header(
        &codec,
        (char *)archive_proxy->buffer,
        0,
        AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH,
        &hdr);
    aeron_archive_client_listRecordingSubscriptionsRequest_set_controlSessionId(&codec, control_session_id);
    aeron_archive_client_listRecordingSubscriptionsRequest_set_correlationId(&codec, correlation_id);
    aeron_archive_client_listRecordingSubscriptionsRequest_set_pseudoIndex(&codec, pseudo_index);
    aeron_archive_client_listRecordingSubscriptionsRequest_set_subscriptionCount(&codec, subscription_count);
    aeron_archive_client_listRecordingSubscriptionsRequest_set_applyStreamId(
        &codec,
        apply_stream_id ? aeron_archive_client_booleanType_TRUE : aeron_archive_client_booleanType_FALSE);
    aeron_archive_client_listRecordingSubscriptionsRequest_set_streamId(&codec, stream_id);
    aeron_archive_client_listRecordingSubscriptionsRequest_put_channel(
        &codec,
        channel_fragment,
        strlen(channel_fragment));

    return aeron_archive_proxy_offer(
        archive_proxy,
        aeron_archive_client_listRecordingSubscriptionsRequest_encoded_length(&codec));
}

/* ************* */

// The length here must NOT include the messageHeader encoded length
int64_t aeron_archive_proxy_offer_once(aeron_archive_proxy_t *archive_proxy, size_t length)
{
    /*
    fprintf(stderr, "length :: %i\n", length);
    for (uint64_t i = 0; i < length; i++)
    {
        fprintf(stderr, "[%llu] '%x' '%c'\n", i, archive_proxy->buffer[i], archive_proxy->buffer[i]);
    }
     */
    return aeron_exclusive_publication_offer(
        archive_proxy->exclusive_publication,
        archive_proxy->buffer,
        aeron_archive_client_messageHeader_encoded_length() + length,
        NULL,
        NULL);
}

// The length here must NOT include the messageHeader encoded length
bool aeron_archive_proxy_offer(
    aeron_archive_proxy_t *archive_proxy,
    size_t length)
{
    int attempts = archive_proxy->retry_attempts;

    while (true)
    {
        int64_t result = aeron_archive_proxy_offer_once(archive_proxy, length);

        if (result > 0)
        {
            return true;
        }

        if (AERON_PUBLICATION_CLOSED == result ||
            AERON_PUBLICATION_NOT_CONNECTED == result ||
            AERON_PUBLICATION_MAX_POSITION_EXCEEDED == result)
        {
            AERON_APPEND_ERR("%s", "");
            return false;
        }

        if (--attempts <= 0)
        {
            AERON_SET_ERR(-1, "%s", "too many retries");
            return false;
        }

        aeron_archive_context_idle(archive_proxy->ctx);
    }
}
