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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <stdlib.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <inttypes.h>

#if !defined(_MSC_VER)
#include <unistd.h>
#endif

#include "aeronc.h"
#include "concurrent/aeron_atomic.h"
#include "collections/aeron_int64_to_ptr_hash_map.h"
#include "util/aeron_strutil.h"
#include "aeron_agent.h"
#include "aeron_alloc.h"

#include "../samples_configuration.h"
#include "../sample_util.h"

typedef struct response_channel_info_t_stct
{
    aeron_image_t *image;
    aeron_subscription_t *subscription;
    aeron_async_add_publication_t *async_add_pub;
    aeron_publication_t *publication;
    aeron_fragment_assembler_t *fragment_assembler;
}
response_channel_info_t;

const char usage_str[] =
    "[-h][-v][-c request-uri][-d response-uri][-p prefix][-r response-stream-id][-s request-stream-id]\n"
    "    -h                       help\n"
    "    -v                       show version and exit\n"
    "    -c request-uri           use request channel specified in uri\n"
    "    -d response-uri          use response control channel specified in uri\n"
    "    -p prefix                aeron.dir location specified as prefix\n"
    "    -r response-stream-id    response stream-id to use\n"
    "    -s request-stream-id     request stream-id to use\n"
    ;

volatile bool running = true;

aeron_t *aeron = NULL;
const char *response_control_channel = DEFAULT_RESPONSE_CONTROL_CHANNEL;
int32_t response_stream_id = DEFAULT_RESPONSE_STREAM_ID;

aeron_int64_to_ptr_hash_map_t response_channel_info_map;
aeron_mutex_t info_lock;

void sigint_handler(int signal)
{
    AERON_PUT_ORDERED(running, false);
}

inline bool is_running(void)
{
    bool result;
    AERON_GET_VOLATILE(result, running);
    return result;
}

void poll_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    response_channel_info_t *response_channel_info = (response_channel_info_t *)clientd;
    aeron_subscription_constants_t subscription_constants;
    aeron_header_values_t header_values;

    if (aeron_subscription_constants(response_channel_info->subscription, &subscription_constants) < 0)
    {
        fprintf(stderr, "could not get subscription constants: %s\n", aeron_errmsg());
        return;
    }

    aeron_header_values(header, &header_values);

    printf(
        "Message to stream %" PRId32 " from session %" PRId32 " (%" PRIu64 " bytes) <<%.*s>>\n",
        subscription_constants.stream_id,
        header_values.frame.session_id,
        (uint64_t)length,
        (int)length,
        buffer);

    if (NULL != response_channel_info->publication)
    {
        char message[256] = { 0 };
        int message_len;

        message_len = SNPRINTF(message, sizeof(message) - 1, "responding to message: %.*s", (int)length, buffer);

        int64_t result = aeron_publication_offer(response_channel_info->publication, (const uint8_t *)message, message_len, NULL, NULL);

        if (result > 0)
        {
            printf("response sent!\n");
        }
        else if (AERON_PUBLICATION_BACK_PRESSURED == result)
        {
            printf("Offer failed due to back pressure\n");
        }
        else if (AERON_PUBLICATION_NOT_CONNECTED == result)
        {
            printf("Offer failed because publisher is not connected to a subscriber\n");
        }
        else if (AERON_PUBLICATION_ADMIN_ACTION == result)
        {
            printf("Offer failed because of an administration action in the system\n");
        }
        else if (AERON_PUBLICATION_CLOSED == result)
        {
            printf("Offer failed because publication is closed\n");
        }
        else
        {
            printf("Offer failed due to unknown reason %" PRId64 "\n", result);
        }
    }
}

void handle_available_image(void *clientd, aeron_subscription_t *subscription, aeron_image_t *image)
{
    response_channel_info_t *response_channel_info = NULL;
    aeron_image_constants_t constants;

    print_available_image(NULL, subscription, image);

    if (aeron_image_constants(image, &constants) < 0)
    {
        fprintf(stderr, "aeron_image_constants: %s\n", aeron_errmsg());
        return;
    }

    aeron_alloc((void **)&response_channel_info, sizeof(response_channel_info_t));

    response_channel_info->image = image;
    response_channel_info->subscription = subscription;

    {
        char _channel_buf[AERON_MAX_PATH] = { 0 };

        SNPRINTF(_channel_buf, sizeof(_channel_buf) - 1, "%s|control-mode=response|response-correlation-id=%" PRIi64, response_control_channel, constants.correlation_id);

        printf("Responding on channel %s on Stream ID %" PRId32 "\n", _channel_buf, response_stream_id);

        if (aeron_async_add_publication(&response_channel_info->async_add_pub, aeron, _channel_buf, response_stream_id) < 0)
        {
            fprintf(stderr, "aeron_async_add_publication: %s\n", aeron_errmsg());
            return;
        }
    }

    response_channel_info->publication = NULL;

    if (aeron_fragment_assembler_create(&response_channel_info->fragment_assembler, poll_handler, response_channel_info) < 0)
    {
        fprintf(stderr, "aeron_fragment_assembler_create: %s\n", aeron_errmsg());
        return;
    }

    aeron_mutex_lock(&info_lock);
    if (aeron_int64_to_ptr_hash_map_put(&response_channel_info_map, constants.correlation_id, response_channel_info) < 0)
    {
        fprintf(stderr, "aeron_int64_to_ptr_hash_map_put: %s\n", aeron_errmsg());
    }
    aeron_mutex_unlock(&info_lock);
}

void handle_unavailable_image(void *clientd, aeron_subscription_t *subscription, aeron_image_t *image)
{
    response_channel_info_t *response_channel_info = NULL;
    aeron_image_constants_t constants;

    print_unavailable_image(NULL, subscription, image);

    if (aeron_image_constants(image, &constants) < 0)
    {
        fprintf(stderr, "aeron_image_constants: %s\n", aeron_errmsg());
        return;
    }

    aeron_mutex_lock(&info_lock);
    response_channel_info = aeron_int64_to_ptr_hash_map_remove(&response_channel_info_map, constants.correlation_id);
    aeron_mutex_unlock(&info_lock);

    if (NULL != response_channel_info)
    {
        aeron_publication_close(response_channel_info->publication, NULL, NULL);
        aeron_fragment_assembler_delete(response_channel_info->fragment_assembler);
        aeron_free(response_channel_info);
    }
}

void process_response_channel_info(void *clientd, int64_t key, void *value)
{
    response_channel_info_t *response_channel_info = (response_channel_info_t *)value;

    if (NULL != response_channel_info->async_add_pub)
    {
        int rc;

        rc = aeron_async_add_publication_poll(&response_channel_info->publication, response_channel_info->async_add_pub);

        if (rc == 0)
        {
            return; // still waiting
        }

        if (rc < 0)
        {
            fprintf(stderr, "aeron_async_add_publication_poll: %s\n", aeron_errmsg());
        }

        // if we're here, _poll returned either 1 or -1.  Either way, we're done with the async_add_pub
        response_channel_info->async_add_pub = NULL;
    }

    int fragments_read = aeron_image_poll(
        response_channel_info->image,
        aeron_fragment_assembler_handler,
        response_channel_info->fragment_assembler,
        DEFAULT_FRAGMENT_COUNT_LIMIT);

    if (fragments_read < 0)
    {
        fprintf(stderr, "aeron_image_poll: %s\n", aeron_errmsg());
    }
    else
    {
        int *total_fragments_read = (int *)clientd;

        *total_fragments_read += fragments_read;
    }
}

int main(int argc, char **argv)
{
    int status = EXIT_FAILURE, opt;

    aeron_context_t *context = NULL;
    const char *aeron_dir = NULL;

    aeron_async_add_subscription_t *async = NULL;
    aeron_subscription_t *subscription = NULL;
    const char *request_channel = DEFAULT_REQUEST_CHANNEL;
    int32_t request_stream_id = DEFAULT_REQUEST_STREAM_ID;

    const uint64_t idle_duration_ns = UINT64_C(1000) * UINT64_C(1000); /* 1ms */

    while ((opt = getopt(argc, argv, "hvc:d:p:r:s:")) != -1)
    {
        switch (opt)
        {
            case 'c':
            {
                request_channel = optarg;
                break;
            }

            case 'd':
            {
                response_control_channel = optarg;
                break;
            }

            case 'p':
            {
                aeron_dir = optarg;
                break;
            }

            case 'r':
            {
                response_stream_id = (int32_t)strtoul(optarg, NULL, 0);
                break;
            }

            case 's':
            {
                request_stream_id = (int32_t)strtoul(optarg, NULL, 0);
                break;
            }

            case 'v':
            {
                printf(
                    "%s <%s> major %d minor %d patch %d git %s\n",
                    argv[0],
                    aeron_version_full(),
                    aeron_version_major(),
                    aeron_version_minor(),
                    aeron_version_patch(),
                    aeron_version_gitsha());
                exit(EXIT_SUCCESS);
            }

            case 'h':
            default:
                fprintf(stderr, "Usage: %s %s", argv[0], usage_str);
                exit(status);
        }
    }

    signal(SIGINT, sigint_handler);

    if (aeron_int64_to_ptr_hash_map_init(&response_channel_info_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        fprintf(stderr, "aeron_int64_to_ptr_hash_map_init: %s\n", aeron_errmsg());
        goto cleanup;
    }

    aeron_mutex_init(&info_lock, NULL);

    printf("Subscribing to channel %s on Stream ID %" PRId32 "\n", request_channel, request_stream_id);

    if (aeron_context_init(&context) < 0)
    {
        fprintf(stderr, "aeron_context_init: %s\n", aeron_errmsg());
        goto cleanup;
    }

    if (NULL != aeron_dir)
    {
        if (aeron_context_set_dir(context, aeron_dir) < 0)
        {
            fprintf(stderr, "aeron_context_set_dir: %s\n", aeron_errmsg());
            goto cleanup;
        }
    }

    if (aeron_init(&aeron, context) < 0)
    {
        fprintf(stderr, "aeron_init: %s\n", aeron_errmsg());
        goto cleanup;
    }

    if (aeron_start(aeron) < 0)
    {
        fprintf(stderr, "aeron_start: %s\n", aeron_errmsg());
        goto cleanup;
    }

    if (aeron_async_add_subscription(
        &async,
        aeron,
        request_channel,
        request_stream_id,
        handle_available_image,
        NULL,
        handle_unavailable_image,
        NULL) < 0)
    {
        fprintf(stderr, "aeron_async_add_subscription: %s\n", aeron_errmsg());
        goto cleanup;
    }

    while (NULL == subscription)
    {
        if (aeron_async_add_subscription_poll(&subscription, async) < 0)
        {
            fprintf(stderr, "aeron_async_add_subscription_poll: %s\n", aeron_errmsg());
            goto cleanup;
        }

        sched_yield();
    }

    printf("Subscription channel status %" PRIu64 "\n", aeron_subscription_channel_status(subscription));

    while (is_running())
    {
        int total_fragments_read = 0;

        aeron_mutex_lock(&info_lock);
        aeron_int64_to_ptr_hash_map_for_each(&response_channel_info_map, process_response_channel_info, &total_fragments_read);
        aeron_mutex_unlock(&info_lock);

        aeron_idle_strategy_sleeping_idle((void *)&idle_duration_ns, total_fragments_read);
    }

    printf("Shutting down...\n");
    status = EXIT_SUCCESS;

cleanup:
    aeron_subscription_close(subscription, NULL, NULL);
    aeron_close(aeron);
    aeron_context_close(context);
    aeron_int64_to_ptr_hash_map_delete(&response_channel_info_map);
    aeron_mutex_destroy(&info_lock);

    return status;
}

extern bool is_running(void);
