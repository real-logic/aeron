/*
 * Copyright 2014-2021 Real Logic Limited.
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
#include "util/aeron_strutil.h"
#include "aeron_agent.h"

#include "samples_configuration.h"

const char usage_str[] =
    "[-h][-v][-c uri][-l linger][-m messages][-p prefix][-s stream-id]\n"
    "    -h               help\n"
    "    -v               show version and exit\n"
    "    -c uri           use channel specified in uri\n"
    "    -l linger        linger at end of publishing for linger seconds\n"
    "    -m messages      number of messages to send\n"
    "    -p prefix        aeron.dir location specified as prefix\n"
    "    -s stream-id     stream-id to use\n";

volatile bool running = true;

void sigint_handler(int signal)
{
    AERON_PUT_ORDERED(running, false);
}

inline bool is_running()
{
    bool result;
    AERON_GET_VOLATILE(result, running);
    return result;
}

int main(int argc, char **argv)
{
    char message[256];
    int status = EXIT_FAILURE, opt;
    aeron_context_t *context = NULL;
    aeron_t *aeron = NULL;
    aeron_async_add_publication_t *async = NULL;
    aeron_publication_t *publication = NULL;
    const char *channel = DEFAULT_CHANNEL;
    const char *aeron_dir = NULL;
    uint64_t linger_ns = DEFAULT_LINGER_TIMEOUT_MS * 1000ul * 1000ul;
    uint64_t messages = DEFAULT_NUMBER_OF_MESSAGES;
    int32_t stream_id = DEFAULT_STREAM_ID;

    while ((opt = getopt(argc, argv, "hvc:l:m:p:s:")) != -1)
    {
        switch (opt)
        {
            case 'c':
            {
                channel = optarg;
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

            case 's':
            {
                stream_id = strtoul(optarg, NULL, 0);
                break;
            }

            case 'v':
            {
                printf("%s <%s> major %d minor %d patch %d\n",
                    argv[0], aeron_version_full(), aeron_version_major(), aeron_version_minor(), aeron_version_patch());
                exit(EXIT_SUCCESS);
            }

            case 'h':
            default:
                fprintf(stderr, "Usage: %s %s", argv[0], usage_str);
                exit(status);
        }
    }

    signal(SIGINT, sigint_handler);

    printf("Publishing to channel %s on Stream ID %" PRId32 "\n", channel, stream_id);

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

    if (aeron_async_add_publication(&async, aeron, channel, stream_id) < 0)
    {
        fprintf(stderr, "aeron_async_add_publication: %s\n", aeron_errmsg());
        goto cleanup;
    }

    while (NULL == publication)
    {
        if (aeron_async_add_publication_poll(&publication, async) < 0)
        {
            fprintf(stderr, "aeron_async_add_publication_poll: %s\n", aeron_errmsg());
            goto cleanup;
        }

        sched_yield();
    }

    printf("Publication channel status %" PRIu64 "\n", aeron_publication_channel_status(publication));

    for (size_t i = 0; i < messages && is_running(); i++)
    {
#if defined(_MSC_VER)
        const int message_len = sprintf_s(message, sizeof(message) - 1, "Hello World! %" PRIu32, (uint32_t)i);
#else
        const int message_len = snprintf(message, sizeof(message) - 1, "Hello World! %" PRIu32, (uint32_t)i);
#endif
        printf("offering %" PRIu32 "/%" PRIu32 " - ", (uint32_t)i, (uint32_t)messages);
        fflush(stdout);

        int64_t result = aeron_publication_offer(
            publication, (const uint8_t *)message, message_len, NULL, NULL);

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
    }

    printf("Done sending.\n");

    if (linger_ns > 0)
    {
        printf("Lingering for %" PRIu64 " nanoseconds\n", linger_ns);
        aeron_nano_sleep(linger_ns);
    }

cleanup:
    aeron_publication_close(publication, NULL, NULL);
    aeron_close(aeron);
    aeron_context_close(context);

    return status;
}

extern bool is_running();
