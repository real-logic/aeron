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
#include <string.h>

#if !defined(_MSC_VER)
#include <unistd.h>
#endif

#include "aeronc.h"
#include "concurrent/aeron_atomic.h"
#include "util/aeron_strutil.h"
#include "aeron_agent.h"

#include "samples_configuration.h"
#include "sample_util.h"

const char usage_str[] =
    "[-h][-v][-C uri][-c uri][-p prefix][-S stream-id][-s stream-id]\n"
    "    -h               help\n"
    "    -v               show version and exit\n"
    "    -C uri           use channel specified in uri for pong channel\n"
    "    -c uri           use channel specified in uri for ping channel\n"
    "    -p prefix        aeron.dir location specified as prefix\n"
    "    -S stream-id     stream-id to use for pong channel\n"
    "    -s stream-id     stream-id to use for ping channel\n";

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

void ping_poll_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    aeron_exclusive_publication_t *publication = (aeron_exclusive_publication_t *)clientd;

    while (aeron_exclusive_publication_offer(publication, buffer, length, NULL, NULL) < 0)
    {
        aeron_idle_strategy_busy_spinning_idle(NULL, 0);
    }
}

int main(int argc, char **argv)
{
    int status = EXIT_FAILURE, opt;
    aeron_context_t *context = NULL;
    aeron_t *aeron = NULL;
    aeron_async_add_subscription_t *async_ping_sub = NULL;
    aeron_async_add_exclusive_publication_t *async_pong_pub = NULL;
    aeron_subscription_t *subscription = NULL;
    aeron_image_t *image = NULL;
    aeron_exclusive_publication_t *publication = NULL;
    aeron_image_fragment_assembler_t *fragment_assembler = NULL;
    const char *pong_channel = DEFAULT_PONG_CHANNEL;
    const char *ping_channel = DEFAULT_PING_CHANNEL;
    const char *aeron_dir = NULL;
    int32_t pong_stream_id = DEFAULT_PONG_STREAM_ID;
    int32_t ping_stream_id = DEFAULT_PING_STREAM_ID;

    while ((opt = getopt(argc, argv, "hvC:c:p:S:s:")) != -1)
    {
        switch (opt)
        {
            case 'C':
            {
                pong_channel = optarg;
                break;
            }

            case 'c':
            {
                ping_channel = optarg;
                break;
            }

            case 'p':
            {
                aeron_dir = optarg;
                break;
            }

            case 'S':
            {
                pong_stream_id = (int32_t)strtoul(optarg, NULL, 0);
                break;
            }

            case 's':
            {
                ping_stream_id = (int32_t)strtoul(optarg, NULL, 0);
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

    printf("Subscribing Ping at channel %s on Stream ID %" PRId32 "\n", ping_channel, ping_stream_id);
    printf("Publishing Pong at channel %s on Stream ID %" PRId32 "\n", pong_channel, pong_stream_id);

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
        &async_ping_sub,
        aeron,
        ping_channel,
        ping_stream_id,
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
        if (aeron_async_add_subscription_poll(&subscription, async_ping_sub) < 0)
        {
            fprintf(stderr, "aeron_async_add_subscription_poll: %s\n", aeron_errmsg());
            goto cleanup;
        }

        if (!is_running())
        {
            goto cleanup;
        }

        sched_yield();
    }

    printf("Subscription channel status %" PRIu64 "\n", aeron_subscription_channel_status(subscription));

    if (aeron_async_add_exclusive_publication(
        &async_pong_pub,
        aeron,
        pong_channel,
        pong_stream_id) < 0)
    {
        fprintf(stderr, "aeron_async_add_exclusive_publication: %s\n", aeron_errmsg());
        goto cleanup;
    }

    while (NULL == publication)
    {
        if (aeron_async_add_exclusive_publication_poll(&publication, async_pong_pub) < 0)
        {
            fprintf(stderr, "aeron_async_add_exclusive_publication_poll: %s\n", aeron_errmsg());
            goto cleanup;
        }

        if (!is_running())
        {
            goto cleanup;
        }

        sched_yield();
    }

    printf("Publication channel status %" PRIu64 "\n", aeron_exclusive_publication_channel_status(publication));

    while (!aeron_subscription_is_connected(subscription))
    {
        if (!is_running())
        {
            goto cleanup;
        }

        sched_yield();
    }

    if ((image = aeron_subscription_image_at_index(subscription, 0)) == NULL)
    {
        fprintf(stderr, "%s", "could not find image\n");
        goto cleanup;
    }

    if (aeron_image_fragment_assembler_create(&fragment_assembler, ping_poll_handler, publication) < 0)
    {
        fprintf(stderr, "aeron_image_fragment_assembler_create: %s\n", aeron_errmsg());
        goto cleanup;
    }

    while (is_running())
    {
        int fragments_read = aeron_image_poll(
            image, aeron_image_fragment_assembler_handler, fragment_assembler, DEFAULT_FRAGMENT_COUNT_LIMIT);

        if (fragments_read < 0)
        {
            fprintf(stderr, "aeron_image_poll: %s\n", aeron_errmsg());
            goto cleanup;
        }

        aeron_idle_strategy_busy_spinning_idle(NULL, fragments_read);
    }

    printf("Shutting down...\n");
    status = EXIT_SUCCESS;

cleanup:
    aeron_subscription_image_release(subscription, image);
    aeron_subscription_close(subscription, NULL, NULL);
    aeron_exclusive_publication_close(publication, NULL, NULL);
    aeron_close(aeron);
    aeron_context_close(context);
    aeron_image_fragment_assembler_delete(fragment_assembler);

    return status;
}

extern bool is_running(void);
