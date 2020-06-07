/*
 * Copyright 2014-2020 Real Logic Limited.
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

#include <ctype.h>
#include <time.h>
#include <stdio.h>
#include <inttypes.h>
#include <errno.h>
#include <string.h>

#define AERON_DLL_EXPORTS

#include "util/aeron_strutil.h"
#include "aeron_windows.h"

void aeron_format_date(char *str, size_t count, int64_t timestamp)
{
    char time_buffer[80];
    char msec_buffer[8];
    char tz_buffer[8];
    struct tm time;
    time_t just_seconds = timestamp / 1000;
    int64_t msec_after_sec = timestamp % 1000;

    localtime_r(&just_seconds, &time);

    strftime(time_buffer, sizeof(time_buffer) - 1, "%Y-%m-%d %H:%M:%S.", &time);
    snprintf(msec_buffer, sizeof(msec_buffer) - 1, "%03" PRId64, msec_after_sec);
    strftime(tz_buffer, sizeof(tz_buffer) - 1, "%z", &time);

    snprintf(str, count, "%s%s%s", time_buffer, msec_buffer, tz_buffer);
}

void aeron_format_to_hex(char *str, size_t str_length, uint8_t *data, size_t data_len)
{
    static char table[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    size_t j = 0;

    for (size_t i = 0; i < data_len && j < str_length; i++)
    {
        char c_high = table[(data[i] >> 4) & 0x0F];
        char c_low = table[data[i] & 0x0F];

        str[j++] = c_high;
        str[j++] = c_low;
    }

    str[j] = '\0';
}

int aeron_tokenise(char *input, const char delimiter, const int max_tokens, char **tokens)
{
    if (NULL == input)
    {
        return -EINVAL;
    }

    const size_t len = strlen(input);

    if (INT32_MAX < len)
    {
        return -EINVAL;
    }

    if (0 == len)
    {
        return 0;
    }

    int num_tokens = 0;

    for (int i = (int)len; --i != -1;)
    {
        if (delimiter == input[i])
        {
            input[i] = '\0';
        }

        if (0 == i && '\0' != input[i])
        {
            if (max_tokens <= num_tokens)
            {
                num_tokens = -ERANGE;
                break;
            }

            tokens[num_tokens] = &input[i];
            num_tokens++;
        }
        else if ('\0' == input[i] && '\0' != input[i + 1])
        {
            if (max_tokens <= num_tokens)
            {
                num_tokens = -ERANGE;
                break;
            }

            tokens[num_tokens] = &input[i + 1];
            num_tokens++;
        }
    }

    return num_tokens;
}

#if defined(_MSC_VER) && !defined(AERON_NO_GETOPT)

/* Taken from https://github.com/skeeto/getopt/blob/master/getopt.h */
/* A minimal POSIX getopt() implementation in ANSI C
 *
 * This is free and unencumbered software released into the public domain.
 *
 * This implementation supports the convention of resetting the option
 * parser by assigning optind to 0. This resets the internal state
 * appropriately.
 *
 * Ref: http://pubs.opengroup.org/onlinepubs/9699919799/functions/getopt.html
 */

static int opterr = 1;
static int optopt;
AERON_EXPORT int optind = 1;
AERON_EXPORT char *optarg = NULL;

int getopt(int argc, char * const argv[], const char *optstring)
{
    static int optpos = 1;
    const char *arg;
    (void)argc;

    /* Reset? */
    if (optind == 0) {
        optind = 1;
        optpos = 1;
    }

    arg = argv[optind];
    if (arg && strcmp(arg, "--") == 0) {
        optind++;
        return -1;
    } else if (!arg || arg[0] != '-' || !isalnum(arg[1])) {
        return -1;
    } else {
        const char *opt = strchr(optstring, arg[optpos]);
        optopt = arg[optpos];
        if (!opt) {
            if (opterr && *optstring != ':')
                fprintf(stderr, "%s: illegal option: %c\n", argv[0], optopt);
            return '?';
        } else if (opt[1] == ':') {
            if (arg[optpos + 1]) {
                optarg = (char *)arg + optpos + 1;
                optind++;
                optpos = 1;
                return optopt;
            } else if (argv[optind + 1]) {
                optarg = (char *)argv[optind + 1];
                optind += 2;
                optpos = 1;
                return optopt;
            } else {
                if (opterr && *optstring != ':')
                    fprintf(stderr, 
                            "%s: option requires an argument: %c\n", 
                            argv[0], optopt);
                return *optstring == ':' ? ':' : '?';
            }
        } else {
            if (!arg[++optpos]) {
                optind++;
                optpos = 1;
            }
            return optopt;
        }
    }
}

#endif

extern uint64_t aeron_fnv_64a_buf(uint8_t *buf, size_t len);
