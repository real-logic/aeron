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

#include <stdio.h>
#include <inttypes.h>

#include "sample_util.h"

void print_available_image(void *clientd, aeron_subscription_t *subscription, aeron_image_t *image)
{
    aeron_subscription_constants_t subscription_constants;
    aeron_image_constants_t image_constants;

    if (aeron_subscription_constants(subscription, &subscription_constants) < 0 ||
        aeron_image_constants(image, &image_constants) < 0)
    {
        fprintf(stderr, "could not get subscription/image constants: %s\n", aeron_errmsg());
    }
    else
    {
        printf(
            "Available image on %s streamId=%" PRId32 " sessionId=%" PRId32 " mtu=%" PRId32 " term-length=%" PRId32 " from %s\n",
            subscription_constants.channel,
            subscription_constants.stream_id,
            image_constants.session_id,
            (int32_t)image_constants.mtu_length,
            (int32_t)image_constants.term_buffer_length,
            image_constants.source_identity);
    }
}

void print_unavailable_image(void *clientd, aeron_subscription_t *subscription, aeron_image_t *image)
{
    aeron_subscription_constants_t subscription_constants;
    aeron_image_constants_t image_constants;

    if (aeron_subscription_constants(subscription, &subscription_constants) < 0 ||
        aeron_image_constants(image, &image_constants) < 0)
    {
        fprintf(stderr, "could not get subscription/image constants: %s\n", aeron_errmsg());
    }
    else
    {
        printf(
            "Unavailable image on %s streamId=%" PRId32 " sessionId=%" PRId32 "\n",
            subscription_constants.channel,
            subscription_constants.stream_id,
            image_constants.session_id);
    }
}

void print_rate_report(uint64_t duration_ns, double mps, double bps, uint64_t total_messages, uint64_t total_bytes)
{
    printf("%" PRIu64 "ms, %.04g msgs/sec, %.04g bytes/sec, totals %" PRIu64 " messages %" PRIu64 " MB payloads\n",
        duration_ns / (UINT64_C(1000) * UINT64_C(1000)), mps, bps, total_messages, total_bytes / (UINT64_C(1024) * UINT64_C(1024)));
}

int rate_reporter_do_work(void *state)
{
    rate_reporter_t *reporter = (rate_reporter_t *)state;
    uint64_t current_total_bytes, current_total_messages;
    int64_t current_timestamp_ns, duration_ns;
    double mps, bps;

    AERON_GET_ACQUIRE(current_total_bytes, reporter->polling_fields.total_bytes);
    AERON_GET_ACQUIRE(current_total_messages, reporter->polling_fields.total_messages);
    current_timestamp_ns = aeron_nano_clock();
    duration_ns = current_timestamp_ns - reporter->last_timestamp_ns;
    mps = ((double)(current_total_messages - reporter->last_total_messages) *
        (double)reporter->idle_duration_ns) / (double)duration_ns;
    bps = ((double)(current_total_bytes - reporter->last_total_bytes) *
        (double)reporter->idle_duration_ns) / (double)duration_ns;

    reporter->on_report(duration_ns, mps, bps, current_total_messages, current_total_bytes);

    reporter->last_total_bytes = current_total_bytes;
    reporter->last_total_messages = current_total_messages;
    reporter->last_timestamp_ns = current_timestamp_ns;

    return 0;
}

int rate_reporter_start(rate_reporter_t *reporter, on_rate_report_t on_report)
{
    reporter->idle_duration_ns = UINT64_C(1000) * UINT64_C(1000) * UINT64_C(1000);
    reporter->on_report = on_report;
    reporter->last_total_bytes = 0;
    reporter->last_total_messages = 0;
    reporter->last_timestamp_ns = aeron_nano_clock();
    reporter->polling_fields.total_bytes = 0;
    reporter->polling_fields.total_messages = 0;

    if (aeron_agent_init(
        &reporter->runner,
        "rate reporter",
        reporter,
        NULL,
        NULL,
        rate_reporter_do_work,
        NULL,
        aeron_idle_strategy_sleeping_idle,
        &reporter->idle_duration_ns) < 0)
    {
        return -1;
    }

    if (aeron_agent_start(&reporter->runner) < 0)
    {
        return -1;
    }

    return 0;
}

int rate_reporter_halt(rate_reporter_t *reporter)
{
    aeron_agent_stop(&reporter->runner);
    aeron_agent_close(&reporter->runner);

    return 0;
}

extern void rate_reporter_poll_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header);
extern void rate_reporter_on_message(rate_reporter_t *reporter, size_t length);
