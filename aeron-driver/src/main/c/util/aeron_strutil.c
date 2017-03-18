/*
 * Copyright 2014 - 2017 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <time.h>
#include <stdio.h>
#include <inttypes.h>
#include "util/aeron_strutil.h"

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


