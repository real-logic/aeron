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
#include "util/aeron_strutil.h"
#include "util/aeron_parse_util.h"
#include "aeron_agent.h"

#include "samples_configuration.h"
#include "sample_util.h"

const char usage_str[] =
    "[-h][-v][-c uri][-p prefix][-s stream-id]\n"
    "    -h               help\n"
    "    -v               show version and exit\n"
    "    -c uri           use channel specified in uri\n"
    "    -p prefix        aeron.dir location specified as prefix\n"
    "    -s stream-id     stream-id to use\n";

volatile bool running = true;

void sigint_handler(int signal)
{
    AERON_SET_RELEASE(running, false);
}

inline bool is_running(void)
{
    bool result;
    AERON_GET_ACQUIRE(result, running);
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
        "Message to stream %" PRId32 " from session %" PRId32 " (%" PRIu64 " bytes) <<%.*s>>\n",
        subscription_constants.stream_id,
        header_values.frame.session_id,
        (uint64_t)length,
        (int)length,
        buffer);
}

int main(int argc, char **argv)
{
    int status = EXIT_FAILURE, opt;
    aeron_context_t *context = NULL;
    aeron_t *aeron = NULL;
    aeron_async_add_subscription_t *async = NULL;
    aeron_subscription_t *subscription = NULL;
    aeron_fragment_assembler_t *fragment_assembler = NULL;
    const char *channel = DEFAULT_CHANNEL;
    const char *aeron_dir = NULL;
    const uint64_t idle_duration_ns = UINT64_C(1000) * UINT64_C(1000); /* 1ms */
    int32_t stream_id = DEFAULT_STREAM_ID;

    while ((opt = getopt(argc, argv, "hvc:p:s:")) != -1)
    {
        switch (opt)
        {
            case 'c':
            {
                channel = optarg;
                break;
            }

            case 'p':
            {
                aeron_dir = optarg;
                break;
            }

            case 's':
            {
                stream_id = (int32_t)strtoul(optarg, NULL, 0);
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

    printf("Subscribing to channel %s on Stream ID %" PRId32 "\n", channel, stream_id);

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
        channel,
        stream_id,
        print_available_image,
        NULL,
        print_unavailable_image,
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

    if (aeron_fragment_assembler_create(&fragment_assembler, poll_handler, subscription) < 0)
    {
        fprintf(stderr, "aeron_fragment_assembler_create: %s\n", aeron_errmsg());
        goto cleanup;
    }

    while (is_running())
    {
        int fragments_read = aeron_subscription_poll(
            subscription, aeron_fragment_assembler_handler, fragment_assembler, DEFAULT_FRAGMENT_COUNT_LIMIT);

        if (fragments_read < 0)
        {
            fprintf(stderr, "aeron_subscription_poll: %s\n", aeron_errmsg());
            goto cleanup;
        }

        aeron_idle_strategy_sleeping_idle((void *)&idle_duration_ns, fragments_read);
    }

    printf("Shutting down...\n");
    status = EXIT_SUCCESS;

cleanup:
    aeron_subscription_close(subscription, NULL, NULL);
    aeron_close(aeron);
    aeron_context_close(context);
    aeron_fragment_assembler_delete(fragment_assembler);

    return status;
}

extern bool is_running(void);
