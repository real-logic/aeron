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
#include "util/aeron_parse_util.h"
#include "aeron_agent.h"

#include "../samples_configuration.h"
#include "../sample_util.h"

const char usage_str[] =
    "[-h][-v][-c request-uri][-d response-uri][-l linger][-m messages][-p prefix][-r response-stream-id][-s request-stream-id]\n"
    "    -h                       help\n"
    "    -v                       show version and exit\n"
    "    -c request-uri           use request channel specified in uri\n"
    "    -d response-uri          use response control channel specified in uri\n"
    "    -l linger                linger at end of publishing for linger seconds\n"
    "    -m messages              number of messages to send\n"
    "    -p prefix                aeron.dir location specified as prefix\n"
    "    -r response-stream-id    response stream-id to use\n"
    "    -s request-stream-id     request stream-id to use\n"
    ;

volatile bool running = true;

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
    aeron_subscription_t *subscription = (aeron_subscription_t *)clientd;
    aeron_subscription_constants_t subscription_constants;
    aeron_header_values_t header_values;

    if (aeron_subscription_constants(subscription, &subscription_constants) < 0)
    {
        fprintf(stderr, "could not get subscription constants: %s\n", aeron_errmsg());
        return;
    }

    aeron_header_values(header, &header_values);

    printf(
        "Response to stream %" PRId32 " from session %" PRId32 " (%" PRIu64 " bytes) <<%.*s>>\n",
        subscription_constants.stream_id,
        header_values.frame.session_id,
        (uint64_t)length,
        (int)length,
        buffer);
}

int main(int argc, char **argv)
{
    char small_message[256] = { 0 };
    const char *message;
    int message_len;

    int status = EXIT_FAILURE, opt;

    aeron_context_t *context = NULL;
    const char *aeron_dir = NULL;
    aeron_t *aeron = NULL;

    aeron_async_add_subscription_t *async_add_sub = NULL;
    aeron_subscription_t *subscription = NULL;
    aeron_fragment_assembler_t *fragment_assembler = NULL;
    const char *response_control_channel = DEFAULT_RESPONSE_CONTROL_CHANNEL;
    int32_t response_stream_id = DEFAULT_RESPONSE_STREAM_ID;
    int64_t subscriber_registration_id;

    aeron_async_add_publication_t *async_add_pub = NULL;
    aeron_publication_t *publication = NULL;
    const char *request_channel = DEFAULT_REQUEST_CHANNEL;
    int32_t request_stream_id = DEFAULT_REQUEST_STREAM_ID;

    uint64_t linger_ns = DEFAULT_LINGER_TIMEOUT_MS * UINT64_C(1000) * UINT64_C(1000);
    uint64_t messages = DEFAULT_NUMBER_OF_MESSAGES;

    while ((opt = getopt(argc, argv, "hvc:d:l:m:p:r:s:")) != -1)
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

            case 'l':
            {
                if (aeron_parse_duration_ns(optarg, &linger_ns) < 0)
                {
                    fprintf(stderr, "malformed linger %s: %s\n", optarg, aeron_errmsg());
                    exit(status);
                }
                break;
            }

            case 'm':
            {
                if (aeron_parse_size64(optarg, &messages) < 0)
                {
                    fprintf(stderr, "malformed number of messages %s: %s\n", optarg, aeron_errmsg());
                    exit(status);
                }
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

    {
        char _response_channel_buf[256] = { 0 };

        SNPRINTF(_response_channel_buf, sizeof(_response_channel_buf) - 1, "%s|control-mode=response", response_control_channel);

        printf("Subscribing to response channel %s on Stream ID %" PRId32 "\n", _response_channel_buf, response_stream_id);

        if (aeron_async_add_subscription(
            &async_add_sub,
            aeron,
            _response_channel_buf,
            response_stream_id,
            print_available_image,
            NULL,
            print_unavailable_image,
            NULL) < 0)
        {
            fprintf(stderr, "aeron_async_add_subscription: %s\n", aeron_errmsg());
            goto cleanup;
        }
    }

    while (NULL == subscription)
    {
        if (aeron_async_add_subscription_poll(&subscription, async_add_sub) < 0)
        {
            fprintf(stderr, "aeron_async_add_subscription_poll: %s\n", aeron_errmsg());
            goto cleanup;
        }

        sched_yield();
    }

    printf("Subscription channel status %" PRIu64 "\n", aeron_subscription_channel_status(subscription));

    if (aeron_fragment_assembler_create(&fragment_assembler, poll_handler, subscription) < 0)
    {
        fprintf(stderr, "aeron_fragment_assembler_create: %s\n", aeron_errmsg());
        goto cleanup;
    }

    {
        aeron_subscription_constants_t constants;

        if (aeron_subscription_constants(subscription, &constants) < 0)
        {
            fprintf(stderr, "aeron_subscription_constants: %s\n", aeron_errmsg());
            goto cleanup;
        }

        subscriber_registration_id = constants.registration_id;
    }

    {
        char _channel_buf[256] = { 0 };

        SNPRINTF(_channel_buf, sizeof(_channel_buf) - 1, "%s|response-correlation-id=%" PRIi64, request_channel, subscriber_registration_id);

        printf("Publishing to channel %s on Stream ID %" PRId32 "\n", _channel_buf, request_stream_id);

        if (aeron_async_add_publication(&async_add_pub, aeron, _channel_buf, request_stream_id) < 0)
        {
            fprintf(stderr, "aeron_async_add_publication: %s\n", aeron_errmsg());
            goto cleanup;
        }
    }

    while (NULL == publication)
    {
        if (aeron_async_add_publication_poll(&publication, async_add_pub) < 0)
        {
            fprintf(stderr, "aeron_async_add_publication_poll: %s\n", aeron_errmsg());
            goto cleanup;
        }

        sched_yield();
    }

    printf("Publication channel status %" PRIu64 "\n", aeron_publication_channel_status(publication));

    for (size_t i = 0; i < messages && is_running(); i++)
    {
        message_len = SNPRINTF(small_message, sizeof(small_message) - 1, "Hello World! %" PRIu64, (uint64_t)i);
        message = small_message;
        printf("offering %" PRIu64 "/%" PRIu64 " - ", (uint64_t)i, (uint64_t)messages);
        fflush(stdout);

        int64_t result = aeron_publication_offer(publication, (const uint8_t *)message, message_len, NULL, NULL);

        if (result > 0)
        {
            printf("yay!\n");
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

        if (!aeron_publication_is_connected(publication))
        {
            printf("No active subscribers detected\n");
        }

        aeron_nano_sleep(1000ul * 1000ul * 1000ul);

        int fragments_read = aeron_subscription_poll(
            subscription, aeron_fragment_assembler_handler, fragment_assembler, DEFAULT_FRAGMENT_COUNT_LIMIT);

        if (fragments_read < 0)
        {
            fprintf(stderr, "aeron_subscription_poll: %s\n", aeron_errmsg());
            goto cleanup;
        }
    }

    printf("Done sending.\n");

    if (linger_ns > 0)
    {
        printf("Lingering for %" PRIu64 " nanoseconds\n", linger_ns);
        aeron_nano_sleep(linger_ns);
    }

cleanup:
    aeron_subscription_close(subscription, NULL, NULL);
    aeron_publication_close(publication, NULL, NULL);
    aeron_close(aeron);
    aeron_context_close(context);
    aeron_fragment_assembler_delete(fragment_assembler);

    return status;
}

extern bool is_running(void);
