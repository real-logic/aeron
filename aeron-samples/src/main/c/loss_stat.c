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

#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <errno.h>

#ifndef _MSC_VER
#include <unistd.h>
#include <getopt.h>
#endif

#include "aeronc.h"
#include "aeron_common.h"
#include "util/aeron_strutil.h"

static const char *aeron_loss_stat_usage(void)
{
    return
        "    -h            Displays help information.\n"
        "    -d basePath   Base Path to shared memory. Default: /dev/shm/aeron-[user]\n"
        "    -t timeout    Number of milliseconds to wait to see if the driver metadata is available. Default: 1000\n";
}

static void aeron_loss_stat_print_error_and_usage(const char *message)
{
    fprintf(stderr, "%s\n%s", message, aeron_loss_stat_usage());
}

typedef struct aeron_loss_stat_settings_stct
{
    const char *base_path;
    int64_t timeout_ms;
}
aeron_loss_stat_settings_t;

static void aeron_loss_stat_print_observation(
    void *clientd,
    int64_t observation_count,
    int64_t total_bytes_lost,
    int64_t first_observation_timestamp,
    int64_t last_observation_timestamp,
    int32_t session_id,
    int32_t stream_id,
    const char *channel,
    int32_t channel_length,
    const char *source,
    int32_t source_length)
{
    char first_timestamp_str[AERON_FORMAT_DATE_MAX_LENGTH];
    char last_timestamp_str[AERON_FORMAT_DATE_MAX_LENGTH];
    aeron_format_date(first_timestamp_str, sizeof(first_timestamp_str), first_observation_timestamp);
    aeron_format_date(last_timestamp_str, sizeof(last_timestamp_str), last_observation_timestamp);
    printf(
        "%" PRId64 ",%" PRId64 ",%s,%s,%" PRId32 ",%" PRId32 ",%.*s,%.*s\n", 
        observation_count, 
        total_bytes_lost, 
        first_timestamp_str, 
        last_timestamp_str,
        session_id,
        stream_id,
        (int)channel_length,
        channel,
        (int)source_length,
        source);
}

int main(int argc, char **argv)
{
    char default_directory[AERON_MAX_PATH];
    aeron_default_path(default_directory, sizeof(default_directory));
    aeron_loss_stat_settings_t settings =
        {
            .base_path = default_directory,
            .timeout_ms = 1000
        };

    int opt;

    while ((opt = getopt(argc, argv, "d:t:h")) != -1)
    {
        switch (opt)
        {
            case 'd':
                settings.base_path = optarg;
                break;

            case 't':
            {
                errno = 0;
                char *endptr;
                settings.timeout_ms = strtoll(optarg, &endptr, 10);
                if (0 != errno || '\0' != endptr[0])
                {
                    aeron_loss_stat_print_error_and_usage("Invalid timeout");
                    return EXIT_FAILURE;
                }
                break;
            }

            case 'h':
                aeron_loss_stat_print_error_and_usage(argv[0]);
                return EXIT_SUCCESS;

            default:
                aeron_loss_stat_print_error_and_usage("Unknown option");
                return EXIT_FAILURE;
        }
    }

    aeron_cnc_t *aeron_cnc = NULL;

    if (aeron_cnc_init(&aeron_cnc, settings.base_path, settings.timeout_ms) < 0)
    {
        aeron_loss_stat_print_error_and_usage(aeron_errmsg());
        return EXIT_FAILURE;
    }

    int exit_result = EXIT_FAILURE;
    printf(
        "%s\n", 
        "OBSERVATION_COUNT, TOTAL_BYTES_LOST, FIRST_OBSERVATION,"
        "LAST_OBSERVATION, SESSION_ID, STREAM_ID, CHANNEL, SOURCE");
    int entries_read = aeron_cnc_loss_reporter_read(aeron_cnc, aeron_loss_stat_print_observation, NULL);

    if (entries_read < 0)
    {
        printf("%s\n", aeron_errmsg());
    }
    else
    {
        printf("%d entries read\n", entries_read);
        exit_result = EXIT_SUCCESS;
    }

    aeron_cnc_close(aeron_cnc);

    return exit_result;
}
