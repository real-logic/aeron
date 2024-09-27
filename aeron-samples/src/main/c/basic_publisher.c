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
#include "util/aeron_strutil.h"
#include "aeron_agent.h"

#include "samples_configuration.h"

const char usage_str[] =
    "[-h][-v][-c uri][-l linger][-m messages][-p prefix][-s stream-id]\n"
    "    -h               help\n"
    "    -v               show version and exit\n"
    "    -f               use a message that will fill the whole MTU\n"
    "    -c uri           use channel specified in uri\n"
    "    -l linger        linger at end of publishing for linger seconds\n"
    "    -m messages      number of messages to send\n"
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

int build_large_message(char *buf, size_t len)
{
    if (len < 2)
    {
        len = 2;
    }

    int limit = (int)len;
    buf[0] = '\n';
    len -= 1;

    char *current_buf = &buf[1];
    int line_counter = 0;

    while (limit > 0)
    {
        int written = snprintf(
            current_buf,
            limit,
            "[%3d] This is a line of text fill out a large message to test whether large MTUs cause issues...\n",
            line_counter);

        limit -= written;
        line_counter++;
        current_buf = &current_buf[written];
    }

    return (int)len;
}

int main(int argc, char **argv)
{
    char small_message[256] = { 0 };
    char large_message[8192] = { 0 };
    const char *message;
    int message_len = sizeof(small_message);
    int status = EXIT_FAILURE, opt;
    bool fill_mtu = false;
    aeron_context_t *context = NULL;
    aeron_t *aeron = NULL;
    aeron_async_add_publication_t *async = NULL;
    aeron_publication_t *publication = NULL;
    const char *channel = DEFAULT_CHANNEL;
    const char *aeron_dir = NULL;
    uint64_t linger_ns = DEFAULT_LINGER_TIMEOUT_MS * UINT64_C(1000) * UINT64_C(1000);
    uint64_t messages = DEFAULT_NUMBER_OF_MESSAGES;
    int32_t stream_id = DEFAULT_STREAM_ID;

    while ((opt = getopt(argc, argv, "hvfc:l:m:p:s:")) != -1)
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

            case 'f':
            {
                fill_mtu = true;
                break;
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

    if (fill_mtu)
    {
        aeron_publication_constants_t pub_constants = { 0 };
        aeron_publication_constants(publication, &pub_constants);
        message_len = build_large_message(large_message, pub_constants.max_payload_length);
        printf("Using message of %d bytes\n", message_len);
    }

    for (size_t i = 0; i < messages && is_running(); i++)
    {
        if (fill_mtu)
        {
            message = large_message;
        }
        else
        {
#if defined(_MSC_VER)
                message_len = sprintf_s(small_message, sizeof(small_message) - 1, "Hello World! %" PRIu64, (uint64_t)i);
#else
                message_len = snprintf(small_message, sizeof(small_message) - 1, "Hello World! %" PRIu64, (uint64_t)i);
#endif
            message = small_message;
        }
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

extern bool is_running(void);
