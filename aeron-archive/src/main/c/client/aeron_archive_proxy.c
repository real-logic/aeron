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
#include "aeron_archive_proxy.h"
#include "aeron_archive_configuration.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

#include "c/aeron_archive_client/authConnectRequest.h"
#include "c/aeron_archive_client/archiveIdRequest.h"
#include "c/aeron_archive_client/closeSessionRequest.h"

#define AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH (8 * 1024)

struct aeron_archive_proxy_stct
{
    aeron_exclusive_publication_t *exclusive_publication;
    int retry_attempts;
    uint8_t buffer[AERON_ARCHIVE_PROXY_REQUEST_BUFFER_LENGTH];
};

bool aeron_archive_proxy_offer(aeron_archive_proxy_t *archive_proxy, size_t length);

int aeron_archive_proxy_create(aeron_archive_proxy_t **archive_proxy, aeron_exclusive_publication_t *exclusive_publication, int retry_attempts)
{
    aeron_archive_proxy_t *_archive_proxy = NULL;

    if (aeron_alloc((void **)&_archive_proxy, sizeof(aeron_archive_proxy_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_proxy_t");
        return -1;
    }

    _archive_proxy->exclusive_publication = exclusive_publication;
    _archive_proxy->retry_attempts = retry_attempts;

    *archive_proxy = _archive_proxy;

    return 0;
}

bool aeron_archive_proxy_try_connect(
    aeron_archive_proxy_t *archive_proxy,
    const char *control_response_channel,
    int32_t control_response_stream_id,
    void *encoded_credentials,
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
    aeron_archive_client_authConnectRequest_put_encodedCredentials(&codec, "admin:admin", 11); // TODO

    return aeron_archive_proxy_offer(archive_proxy,
        aeron_archive_client_messageHeader_encoded_length() +
        aeron_archive_client_authConnectRequest_sbe_position(&codec));
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

    return aeron_archive_proxy_offer(archive_proxy,
        aeron_archive_client_messageHeader_encoded_length() +
        aeron_archive_client_archiveIdRequest_sbe_position(&codec));
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

    return aeron_archive_proxy_offer(archive_proxy,
        aeron_archive_client_messageHeader_encoded_length() +
        aeron_archive_client_closeSessionRequest_sbe_position(&codec));
}

/* ************* */

bool aeron_archive_proxy_offer(aeron_archive_proxy_t *archive_proxy, size_t length)
{
    fprintf(stderr, "length :: %i\n", length);
    for (uint64_t i = 0; i < length; i++)
    {
        fprintf(stderr, "[%llu] '%x' '%c'\n", i, archive_proxy->buffer[i], archive_proxy->buffer[i]);
    }

    int64_t rc = aeron_exclusive_publication_offer(
        archive_proxy->exclusive_publication,
        archive_proxy->buffer,
        length,
        NULL,
        NULL);

    fprintf(stderr, "ex pub offer :: %llu\n", rc);

    if (rc > 0)
    {
        return true;
    }

    // TODO do something with rc

    return false;
}
