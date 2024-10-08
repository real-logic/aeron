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

#ifndef AERON_ARCHIVE_RECORDING_DESCRIPTOR_POLLER_H
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_POLLER_H

#include "aeron_archive.h"

#include "aeronc.h"

#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_POLLER_FRAGMENT_LIMIT_DEFAULT 10

typedef struct aeron_archive_recording_descriptor_poller_stct
{
    aeron_archive_context_t *ctx;
    aeron_subscription_t *subscription;
    int64_t control_session_id;

    int fragment_limit;
    aeron_controlled_fragment_assembler_t *fragment_assembler;
    bool error_on_fragment;

    int64_t correlation_id;
    int32_t remaining_record_count;
    aeron_archive_recording_descriptor_consumer_func_t recording_descriptor_consumer;
    void *recording_descriptor_consumer_clientd;

    bool is_dispatch_complete;
}
aeron_archive_recording_descriptor_poller_t;

int aeron_archive_recording_descriptor_poller_create(
    aeron_archive_recording_descriptor_poller_t **poller,
    aeron_archive_context_t *ctx,
    aeron_subscription_t *subscription,
    int64_t control_session_id,
    int fragment_limit);

int aeron_archive_recording_descriptor_poller_close(aeron_archive_recording_descriptor_poller_t *poller);

void aeron_archive_recording_descriptor_poller_reset(
    aeron_archive_recording_descriptor_poller_t *poller,
    int64_t correlation_id,
    int32_t record_count,
    aeron_archive_recording_descriptor_consumer_func_t recording_descriptor_consumer,
    void *recording_descriptor_consumer_clientd);

int aeron_archive_recording_descriptor_poller_poll(aeron_archive_recording_descriptor_poller_t *poller);

#endif // AERON_ARCHIVE_RECORDING_DESCRIPTOR_POLLER_H
