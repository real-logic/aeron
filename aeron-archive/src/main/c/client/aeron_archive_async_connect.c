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
#include "aeron_archive_client.h"
#include "aeron_archive_configuration.h"
#include "aeron_archive_context.h"
#include "aeron_archive_control_response_poller.h"
#include "aeron_archive_recording_descriptor_poller.h"
#include "aeron_archive_recording_subscription_descriptor_poller.h"
#include "aeron_archive_proxy.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

typedef enum aeron_archive_async_connect_state_en
{
    ADD_PUBLICATION = 0,
    AWAIT_PUBLICATION_CONNECTED = 1,
    SEND_CONNECT_REQUEST = 2,
    AWAIT_SUBSCRIPTION_CONNECTED = 3,
    AWAIT_CONNECT_RESPONSE = 4,
    SEND_ARCHIVE_ID_REQUEST = 5,
    AWAIT_ARCHIVE_ID_RESPONSE = 6,
    DONE = 7,
    SEND_CHALLENGE_RESPONSE = 8,
    AWAIT_CHALLENGE_RESPONSE = 9
}
aeron_archive_async_connect_state_t;

struct aeron_archive_async_connect_stct
{
    aeron_archive_async_connect_state_t state;
    aeron_archive_context_t *ctx;
    aeron_t *aeron;
    aeron_async_add_subscription_t *async_add_subscription;
    int64_t subscription_id;
    aeron_subscription_t *subscription;
    aeron_async_add_exclusive_publication_t *async_add_exclusive_publication;
    int64_t exclusive_publication_id;
    aeron_exclusive_publication_t *exclusive_publication;
    int64_t deadline_ns;
    aeron_archive_proxy_t *archive_proxy;
    aeron_archive_control_response_poller_t *control_response_poller;
    int64_t correlation_id;
    int64_t control_session_id;
};

int aeron_archive_check_and_setup_response_channel(aeron_archive_context_t *ctx, int64_t subscription_id);

int aeron_archive_async_connect_transition_to_done(aeron_archive_t **aeron_archive, aeron_archive_async_connect_t *async, int64_t archive_id);

int aeron_archive_async_connect_delete(aeron_archive_async_connect_t *async);

/* *********************** */

int aeron_archive_async_connect(aeron_archive_async_connect_t **async, aeron_archive_context_t *ctx)
{
    *async = NULL;

    if (aeron_archive_context_conclude(ctx) < 0)
    {
        return -1;
    }

    aeron_t *aeron = NULL;
    aeron_async_add_subscription_t *async_add_subscription = NULL;
    aeron_async_add_exclusive_publication_t *async_add_exclusive_publication = NULL;

    aeron = ctx->aeron;

    if (aeron_async_add_subscription(
        &async_add_subscription,
        aeron,
        ctx->control_response_channel,
        ctx->control_response_stream_id,
        NULL,
        NULL,
        NULL,
        NULL) < 0)
    {
        // TODO
        return -1;
    }

    int64_t subscription_id = aeron_async_add_subscription_get_registration_id(async_add_subscription);

    aeron_archive_check_and_setup_response_channel(ctx, subscription_id);

    if (aeron_async_add_exclusive_publication(
        &async_add_exclusive_publication,
        aeron,
        ctx->control_request_channel,
        ctx->control_request_stream_id) < 0)
    {
        // TODO
        return -1;
    }

    int64_t exclusive_publication_id = aeron_async_add_exclusive_exclusive_publication_get_registration_id(async_add_exclusive_publication);

    aeron_archive_async_connect_t *_async = NULL;

    if (aeron_alloc((void **)&_async, sizeof(aeron_archive_async_connect_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_async_connect_t");
        return -1;
    }

    _async->state = ADD_PUBLICATION;
    _async->ctx = ctx;
    _async->aeron = aeron;
    _async->async_add_subscription = async_add_subscription;
    _async->subscription_id = subscription_id;
    _async->subscription = NULL;
    _async->async_add_exclusive_publication = async_add_exclusive_publication;
    _async->exclusive_publication_id = exclusive_publication_id;
    _async->exclusive_publication = NULL;
    _async->deadline_ns = aeron_nano_clock() + ctx->message_timeout_ns;
    _async->archive_proxy = NULL;
    _async->control_response_poller = NULL;
    _async->correlation_id = AERON_NULL_VALUE;
    _async->control_session_id = AERON_NULL_VALUE;

    *async = _async;

    return 0;
}

int aeron_archive_async_connect_poll(aeron_archive_t **aeron_archive, aeron_archive_async_connect_t *async)
{
    if (aeron_nano_clock() > async->deadline_ns)
    {
        // TODO timeout
        return -1;
    }

    if (ADD_PUBLICATION == async->state)
    {
        if (NULL == async->exclusive_publication)
        {
            int rc = aeron_async_add_exclusive_publication_poll(
                &async->exclusive_publication,
                async->async_add_exclusive_publication);

            if (rc == 0)
            {
                // try again
            }
            else if (rc == 1)
            {
                // success - exclusive_publication should now be set
                async->async_add_exclusive_publication = NULL;
            }
            else
            {
                // error
                async->async_add_exclusive_publication = NULL;
                return -1;
            }
        }

        if (NULL != async->exclusive_publication && NULL == async->archive_proxy)
        {
            if (aeron_archive_proxy_create(
                &async->archive_proxy,
                async->ctx,
                async->exclusive_publication,
                AERON_ARCHIVE_PROXY_RETRY_ATTEMPTS_DEFAULT) < 0)
            {
                // TODO
                return -1;
            }
        }

        if (NULL == async->subscription)
        {
            int rc = aeron_async_add_subscription_poll(
                &async->subscription,
                async->async_add_subscription);

            if (rc == 0)
            {
                // try again
            }
            else if (rc == 1)
            {
                // success - subscription should now be set
                async->async_add_subscription = NULL;
            }
            else
            {
                // error
                async->async_add_subscription = NULL;
                return -1;
            }
        }

        if (NULL != async->subscription && NULL == async->control_response_poller)
        {
            if (aeron_archive_control_response_poller_create(
                &async->control_response_poller,
                async->subscription,
                AERON_ARCHIVE_CONTROL_RESPONSE_POLLER_FRAGMENT_LIMIT_DEFAULT) < 0)
            {
                // TODO
                return -1;
            }
        }

        if (NULL != async->archive_proxy && NULL != async->control_response_poller)
        {
            async->state = AWAIT_PUBLICATION_CONNECTED;
        }
    }

    if (AWAIT_PUBLICATION_CONNECTED == async->state)
    {
        if (aeron_exclusive_publication_is_connected(async->exclusive_publication))
        {
            async->state = SEND_CONNECT_REQUEST;
        }
        else
        {
            return 0;
        }
    }

    if (SEND_CONNECT_REQUEST == async->state)
    {
        char control_response_channel[AERON_MAX_PATH] = { 0 };

        if (aeron_subscription_try_resolve_channel_endpoint_port(async->subscription, control_response_channel, sizeof(control_response_channel)) < 0)
        {
            // TODO
            return -1;
        }

        if ('\0' == control_response_channel[0])
        {
            return 0;
        }

        aeron_archive_encoded_credentials_t *encoded_credentials = aeron_archive_credentials_supplier_encoded_credentials(&async->ctx->credentials_supplier);

        async->correlation_id = aeron_next_correlation_id(async->aeron);

        if (!aeron_archive_proxy_try_connect(
            async->archive_proxy,
            control_response_channel,
            async->ctx->control_response_stream_id,
            encoded_credentials,
            async->correlation_id))
        {
            aeron_archive_credentials_supplier_on_free(&async->ctx->credentials_supplier, encoded_credentials);
            return 0;
        }

        aeron_archive_credentials_supplier_on_free(&async->ctx->credentials_supplier, encoded_credentials);
        async->state = AWAIT_SUBSCRIPTION_CONNECTED;
    }

    if (AWAIT_SUBSCRIPTION_CONNECTED == async->state)
    {
        if (aeron_subscription_is_connected(async->subscription))
        {
            async->state = AWAIT_CONNECT_RESPONSE;
        }
        else
        {
            return 0;
        }
    }

    if (SEND_ARCHIVE_ID_REQUEST == async->state)
    {
        if (!aeron_archive_proxy_archive_id(
            async->archive_proxy,
            async->correlation_id,
            async->control_session_id))
        {
            return 0;
        }

        async->state = AWAIT_ARCHIVE_ID_RESPONSE;
    }

    if (SEND_CHALLENGE_RESPONSE == async->state)
    {
        // TODO

        async->state = AWAIT_CHALLENGE_RESPONSE;
    }

    if (NULL != async->control_response_poller)
    {
        aeron_archive_control_response_poller_poll(async->control_response_poller);

        if (aeron_archive_control_response_poller_is_poll_complete(async->control_response_poller) &&
            aeron_archive_control_response_poller_correlation_id(async->control_response_poller) == async->correlation_id)
        {
            async->control_session_id = aeron_archive_control_response_poller_control_session_id(async->control_response_poller);

            if (aeron_archive_control_response_poller_was_challenged(async->control_response_poller))
            {
                // TODO
                return -1;
            }
            else
            {
                if (!aeron_archive_control_response_poller_is_code_ok(async->control_response_poller))
                {
                    if (aeron_archive_control_response_poller_is_code_error(async->control_response_poller))
                    {
                        aeron_archive_proxy_close_session(async->archive_proxy, async->control_session_id);
                        // TODO
                        return -1;
                    }

                    aeron_archive_proxy_close_session(async->archive_proxy, async->control_session_id);
                    // TODO
                    return -1;
                }

                if (AWAIT_CONNECT_RESPONSE == async->state)
                {
                    int32_t archive_protocol_version = aeron_archive_control_response_poller_version(async->control_response_poller);

                    if (archive_protocol_version < aeron_archive_protocol_version_with_archive_id())
                    {
                        return aeron_archive_async_connect_transition_to_done(aeron_archive, async, AERON_NULL_VALUE);
                    }
                    else
                    {
                        async->correlation_id = aeron_next_correlation_id(async->aeron);
                        async->state = SEND_ARCHIVE_ID_REQUEST;
                    }
                }
                else if (AWAIT_ARCHIVE_ID_RESPONSE == async->state)
                {
                    int64_t archive_id = aeron_archive_control_response_poller_relevant_id(async->control_response_poller);

                    return aeron_archive_async_connect_transition_to_done(aeron_archive, async, archive_id);
                }
                else
                {
                    // TODO
                    return -1;
                }
            }
        }
    }

    return 0;

    /*
cleanup: // TODO add cleanup: section that frees up the async
    return -1;
     */
}

/* *********************** */

int aeron_archive_check_and_setup_response_channel(aeron_archive_context_t *ctx, int64_t subscription_id)
{
    // TODO
    return 0;
}

int aeron_archive_async_connect_transition_to_done(aeron_archive_t **aeron_archive, aeron_archive_async_connect_t *async, int64_t archive_id)
{
    aeron_archive_recording_descriptor_poller_t *recording_descriptor_poller;

    if (aeron_archive_recording_descriptor_poller_create(
        &recording_descriptor_poller,
        async->ctx,
        async->subscription,
        async->control_session_id,
        AERON_ARCHIVE_RECORDING_DESCRIPTOR_POLLER_FRAGMENT_LIMIT_DEFAULT) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    aeron_archive_recording_subscription_descriptor_poller_t *recording_subscription_descriptor_poller;

    if (aeron_archive_recording_subscription_descriptor_poller_create(
        &recording_subscription_descriptor_poller,
        async->ctx,
        async->subscription,
        async->control_session_id,
        AERON_ARCHIVE_RECORDING_SUBSCRIPTION_DESCRIPTOR_POLLER_FRAGMENT_LIMIT_DEFAULT) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        // TODO free up the recording_descriptor
        return -1;
    }

    int rc = aeron_archive_create(
        aeron_archive,
        async->ctx,
        async->archive_proxy,
        async->subscription,
        async->control_response_poller,
        recording_descriptor_poller,
        recording_subscription_descriptor_poller,
        async->aeron,
        async->control_session_id,
        archive_id);

    if (rc == 0)
    {
        /*
         * What's returned here is what's returned from aeron_archive_async_connect_poll().
         * '0' means 'try again'.
         * '1' means 'success' and then it's expected that aeron_archive is now valid.
         * If aeron_archive_create() returns 0, then the aeron_archive should be valid.  So return 1.
         */
        rc = 1;

        // TODO NULL out things like archive proxy and poller and such so they're not deleted when the async is deleted
    }

    aeron_archive_async_connect_delete(async);

    return rc;
}

int aeron_archive_async_connect_delete(aeron_archive_async_connect_t *async)
{
    // TODO check for non-NULL fields and free things up accordingly

    aeron_free(async);

    return 0;
}
