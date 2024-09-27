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
#include "util/aeron_parse_util.h"
#include "aeron_agent.h"
#include "aeron_alloc.h"
#include "sample_util.h"

#include "samples_configuration.h"

const char usage_str[] =
    "[-h][-v][-c uri][-L length][-l linger][-m messages][-p prefix][-s stream-id]\n"
    "    -h               help\n"
    "    -v               show version and exit\n"
    "    -c uri           use channel specified in uri\n"
    "    -L length        use message length of length bytes\n"
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

int main(int argc, char **argv)
{
    rate_reporter_t rate_reporter;
    int status = EXIT_FAILURE, opt;
    aeron_context_t *context = NULL;
    aeron_t *aeron = NULL;
    aeron_async_add_publication_t *async = NULL;
    aeron_publication_t *publication = NULL;
    const char *channel = DEFAULT_CHANNEL;
    const char *aeron_dir = NULL;
    uint8_t *message = NULL;
    uint64_t linger_ns = DEFAULT_LINGER_TIMEOUT_MS * UINT64_C(1000) * UINT64_C(1000);
    uint64_t messages = DEFAULT_NUMBER_OF_MESSAGES;
    uint64_t message_length = DEFAULT_MESSAGE_LENGTH;
    uint64_t back_pressure_count = 0, message_sent_count = 0;
    int64_t start_timestamp_ns, duration_ns;
    int32_t stream_id = DEFAULT_STREAM_ID;

    while ((opt = getopt(argc, argv, "hvc:L:l:m:p:s:")) != -1)
    {
        switch (opt)
        {
            case 'c':
            {
                channel = optarg;
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

            case 'h':
            default:
                fprintf(stderr, "Usage: %s %s", argv[0], usage_str);
                exit(status);
        }
    }

    signal(SIGINT, sigint_handler);

    if (aeron_alloc((void **)&message, message_length) < 0)
    {
        fprintf(stderr, "allocating message: %s\n", aeron_errmsg());
        goto cleanup;
    }

    memset(message, 0, message_length);

    printf("Streaming %" PRIu64 " messages of payload length %" PRIu64 " bytes to %s on stream id %" PRId32 "\n",
        messages, message_length, channel, stream_id);

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

    printf("Publication channel status %" PRId64 "\n", aeron_publication_channel_status(publication));

    if (rate_reporter_start(&rate_reporter, print_rate_report) < 0)
    {
        fprintf(stderr, "rate_reporter_start: %s\n", aeron_errmsg());
        goto cleanup;
    }

    start_timestamp_ns = aeron_nano_clock();
    for (uint64_t i = 0; i < messages && is_running(); i++)
    {
        *((uint64_t *)message) = i;
        while (aeron_publication_offer(publication, message, message_length, NULL, NULL) < 0)
        {
            ++back_pressure_count;

            if (!is_running())
            {
                break;
            }

            aeron_idle_strategy_busy_spinning_idle(NULL, 0);
        }

        ++message_sent_count;
        rate_reporter_on_message(&rate_reporter, message_length);
    }
    duration_ns = aeron_nano_clock() - start_timestamp_ns;

    printf("Done sending.\n");

    rate_reporter_halt(&rate_reporter);

    printf("Publisher back pressure ratio %g\n", (double)back_pressure_count / (double)message_sent_count);
    printf(
        "Total: %" PRId64 "ms, %.04g msgs/sec, %.04g bytes/sec, totals %" PRIu64 " messages %" PRIu64 " MB payloads\n",
        duration_ns / (1000 * 1000),
        ((double)messages * (double)(1000 * 1000 * 1000) / (double)duration_ns),
        ((double)(messages * message_length) * (double)(1000 * 1000 * 1000) / (double)duration_ns),
        messages,
        messages * message_length / (1024 * 1024));

    if (linger_ns > 0)
    {
        printf("Lingering for %" PRIu64 " nanoseconds\n", linger_ns);
        aeron_nano_sleep(linger_ns);
    }

    status = EXIT_SUCCESS;

cleanup:
    aeron_publication_close(publication, NULL, NULL);
    aeron_close(aeron);
    aeron_context_close(context);
    aeron_free(message);

    return status;
}

extern bool is_running(void);
