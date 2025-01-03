/*
 * Copyright 2014-2025 Real Logic Limited.
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

#include <hdr/hdr_histogram.h>

#include "aeronc.h"
#include "concurrent/aeron_atomic.h"
#include "util/aeron_strutil.h"
#include "util/aeron_parse_util.h"
#include "aeron_agent.h"

#include "samples_configuration.h"
#include "sample_util.h"
#include "aeron_alloc.h"

const char usage_str[] =
    "[-h][-v][-C uri][-c uri][-p prefix][-S stream-id][-s stream-id]\n"
    "    -h               help\n"
    "    -v               show version and exit\n"
    "    -C uri           use channel specified in uri for pong channel\n"
    "    -c uri           use channel specified in uri for ping channel\n"
    "    -L length        use message length of length bytes\n"
    "    -m messages      number of messages to send\n"
    "    -p prefix        aeron.dir location specified as prefix\n"
    "    -S stream-id     stream-id to use for pong channel\n"
    "    -s stream-id     stream-id to use for ping channel\n"
    "    -w messages      number of warm up messages to send\n";

typedef struct aeron_cping_pong_handler_data_stct
{
    size_t receive_count;
    struct hdr_histogram *histogram;
}
aeron_cping_pong_handler_data_t;

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

void pong_measuring_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    aeron_cping_pong_handler_data_t *data = (aeron_cping_pong_handler_data_t *)clientd;
    data->receive_count++;

    int64_t end_ns = aeron_nano_clock();
    int64_t start_ns;

    memcpy(&start_ns, buffer, sizeof(uint64_t));
    hdr_record_value(data->histogram, end_ns - start_ns);
}

void send_ping_and_receive_pong(
    aeron_exclusive_publication_t *publication,
    aeron_image_t *image,
    aeron_fragment_handler_t fragment_handler,
    void *poll_clientd,
    aeron_cping_pong_handler_data_t *pong_handler_data,
    uint64_t messages,
    const uint8_t *message,
    size_t message_length)
{
    pong_handler_data->receive_count = 0;

    int64_t *timestamp = (int64_t *)message;

    for (size_t i = 0; i < messages && is_running(); i++)
    {
        do
        {
            *timestamp = aeron_nano_clock();
        }
        while (aeron_exclusive_publication_offer(
            publication, message, message_length, NULL, NULL) < 0);

        while (pong_handler_data->receive_count < (i + 1))
        {
            if (aeron_image_poll(image, fragment_handler, poll_clientd, DEFAULT_FRAGMENT_COUNT_LIMIT) <= 0)
            {
                aeron_idle_strategy_busy_spinning_idle(NULL, 0);
            }
        }
    }
}

int main(int argc, char **argv)
{
    int status = EXIT_FAILURE, opt;
    aeron_context_t *context = NULL;
    aeron_t *aeron = NULL;
    aeron_async_add_subscription_t *async_pong_sub = NULL;
    aeron_async_add_exclusive_publication_t *async_ping_pub = NULL;
    aeron_subscription_t *subscription = NULL;
    aeron_image_t *image = NULL;
    aeron_exclusive_publication_t *publication = NULL;
    aeron_image_fragment_assembler_t *fragment_assembler = NULL;
    const char *pong_channel = DEFAULT_PONG_CHANNEL;
    const char *ping_channel = DEFAULT_PING_CHANNEL;
    const char *aeron_dir = NULL;
    uint64_t messages = DEFAULT_NUMBER_OF_MESSAGES;
    uint64_t message_length = DEFAULT_MESSAGE_LENGTH;
    uint64_t warm_up_messages = DEFAULT_NUMBER_OF_WARM_UP_MESSAGES;
    int32_t pong_stream_id = DEFAULT_PONG_STREAM_ID;
    int32_t ping_stream_id = DEFAULT_PING_STREAM_ID;
    uint8_t *message = NULL;
    aeron_cping_pong_handler_data_t *pong_handler_data = NULL;

    while ((opt = getopt(argc, argv, "hvC:c:L:m:p:S:s:w:")) != -1)
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

            case 'L':
            {
                if (aeron_parse_size64(optarg, &message_length) < 0)
                {
                    fprintf(stderr, "malformed message length %s: %s\n", optarg, aeron_errmsg());
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

            case 'w':
            {
                if (aeron_parse_size64(optarg, &warm_up_messages) < 0)
                {
                    fprintf(stderr, "malformed number of warm up messages %s: %s\n", optarg, aeron_errmsg());
                    exit(status);
                }
                break;
            }

            case 'h':
            default:
                fprintf(stderr, "Usage: %s %s", argv[0], usage_str);
                exit(status);
        }
    }

    signal(SIGINT, sigint_handler);

    printf("Publishing Ping at channel %s on Stream ID %" PRId32 "\n", ping_channel, ping_stream_id);
    printf("Subscribing Pong at channel %s on Stream ID %" PRId32 "\n", pong_channel, pong_stream_id);

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
        &async_pong_sub,
        aeron,
        pong_channel,
        pong_stream_id,
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
        if (aeron_async_add_subscription_poll(&subscription, async_pong_sub) < 0)
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
        &async_ping_pub,
        aeron,
        ping_channel,
        ping_stream_id) < 0)
    {
        fprintf(stderr, "aeron_async_add_exclusive_publication: %s\n", aeron_errmsg());
        goto cleanup;
    }

    while (NULL == publication)
    {
        if (aeron_async_add_exclusive_publication_poll(&publication, async_ping_pub) < 0)
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

    if (aeron_alloc((void**)&message, message_length) < 0)
    {
        fprintf(stderr, "could not allocate message buffer: %s\n", aeron_errmsg());
        goto cleanup;
    }
    memset(message, 0, message_length);

    if (aeron_alloc((void **)&pong_handler_data, sizeof(aeron_cping_pong_handler_data_t)) < 0)
    {
        fprintf(stderr, "could not allocate: %s\n", aeron_errmsg());
        goto cleanup;
    }

    if (hdr_init(1, 10 * 1000 * 1000 * INT64_C(1000), 3, &pong_handler_data->histogram) < 0)
    {
        fprintf(stderr, "hdr_init: %s\n", aeron_errmsg());
        goto cleanup;
    }

    if (aeron_image_fragment_assembler_create(&fragment_assembler, pong_measuring_handler, pong_handler_data) < 0)
    {
        fprintf(stderr, "aeron_image_fragment_assembler_create: %s\n", aeron_errmsg());
        goto cleanup;
    }

    printf("Warming up the media driver with %" PRIu64 " messages of length %" PRIu64 " bytes\n",
        warm_up_messages, message_length);

    int64_t start_warm_up_ns = aeron_nano_clock();

    send_ping_and_receive_pong(
        publication,
        image,
        aeron_image_fragment_assembler_handler,
        fragment_assembler,
        pong_handler_data,
        warm_up_messages,
        message,
        message_length);

    printf("Warm up complete in %" PRId64 "ns\n", aeron_nano_clock() - start_warm_up_ns);
    printf("Pinging %" PRIu64 " messages of length %" PRIu64 " bytes\n", messages, message_length);

    hdr_reset(pong_handler_data->histogram);

    send_ping_and_receive_pong(
        publication,
        image,
        aeron_image_fragment_assembler_handler,
        fragment_assembler,
        pong_handler_data,
        messages,
        message,
        message_length);

    hdr_percentiles_print(pong_handler_data->histogram, stdout, 5, 1000.0, CLASSIC);
    fflush(stdout);

    printf("Shutting down...\n");
    status = EXIT_SUCCESS;

cleanup:
    aeron_subscription_image_release(subscription, image);
    aeron_subscription_close(subscription, NULL, NULL);
    aeron_exclusive_publication_close(publication, NULL, NULL);
    aeron_close(aeron);
    aeron_context_close(context);
    aeron_image_fragment_assembler_delete(fragment_assembler);
    if (NULL != pong_handler_data)
    {
        hdr_close(pong_handler_data->histogram);
    }
    aeron_free(pong_handler_data);
    aeron_free(message);

    return status;
}

extern bool is_running(void);
