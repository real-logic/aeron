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

#ifndef AERON_SAMPLE_UTIL_H
#define AERON_SAMPLE_UTIL_H

#include "aeronc.h"
#include "aeron_agent.h"
#include "util/aeron_bitutil.h"

void print_available_image(void *clientd, aeron_subscription_t *subscription, aeron_image_t *image);
void print_unavailable_image(void *clientd, aeron_subscription_t *subscription, aeron_image_t *image);

void print_rate_report(uint64_t duration_ns, double mps, double bps, uint64_t total_messages, uint64_t total_bytes);

typedef void (*on_rate_report_t)(
    uint64_t duration_ns, double mps, double bps, uint64_t total_messages, uint64_t total_bytes);

typedef struct rate_reporter_stct
{
    aeron_agent_runner_t runner;
    uint64_t idle_duration_ns;
    on_rate_report_t on_report;

    uint64_t last_total_bytes;
    uint64_t last_total_messages;
    int64_t last_timestamp_ns;

    struct polling_elements_stct
    {
        uint8_t pre_pad[AERON_CACHE_LINE_LENGTH * 2];
        volatile uint64_t total_bytes;
        volatile uint64_t total_messages;
        uint8_t post_pad[AERON_CACHE_LINE_LENGTH * 2];
    }
    polling_fields;
}
rate_reporter_t;

int rate_reporter_start(rate_reporter_t *reporter, on_rate_report_t on_report);
int rate_reporter_halt(rate_reporter_t *reporter);

inline void rate_reporter_poll_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    rate_reporter_t *reporter = (rate_reporter_t *)clientd;

    AERON_SET_RELEASE(reporter->polling_fields.total_bytes, reporter->polling_fields.total_bytes + length);
    AERON_SET_RELEASE(reporter->polling_fields.total_messages, reporter->polling_fields.total_messages + 1);
}

inline void rate_reporter_on_message(rate_reporter_t *reporter, size_t length)
{
    AERON_SET_RELEASE(reporter->polling_fields.total_bytes, reporter->polling_fields.total_bytes + length);
    AERON_SET_RELEASE(reporter->polling_fields.total_messages, reporter->polling_fields.total_messages + 1);
}

#if defined(_MSC_VER)
#define SNPRINTF(_buf, _len, _fmt, ...) sprintf_s(_buf, _len, _fmt, __VA_ARGS__)
#else
#define SNPRINTF(_buf, _len, _fmt, ...) snprintf(_buf, _len, _fmt, __VA_ARGS__)
#endif

#endif //AERON_SAMPLE_UTIL_H
