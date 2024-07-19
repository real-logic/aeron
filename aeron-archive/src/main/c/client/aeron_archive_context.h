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

#ifndef AERON_ARCHIVE_CLIENT_CONTEXT_H
#define AERON_ARCHIVE_CLIENT_CONTEXT_H

#include "aeron_archive.h"

int aeron_archive_context_conclude(aeron_archive_context_t *ctx);

aeron_t *aeron_archive_context_get_aeron(aeron_archive_context_t *ctx);

char *aeron_archive_context_get_control_request_channel(aeron_archive_context_t *ctx);
int32_t aeron_archive_context_get_control_request_stream_id(aeron_archive_context_t *ctx);
char *aeron_archive_context_get_control_response_channel(aeron_archive_context_t *ctx);
int32_t aeron_archive_context_get_control_response_stream_id(aeron_archive_context_t *ctx);

int64_t aeron_archive_context_get_message_timeout_ns(aeron_archive_context_t *ctx);
int aeron_archive_context_set_message_timeout_ns(aeron_archive_context_t *ctx, int64_t message_timeout_ns);

#endif //AERON_ARCHIVE_CLIENT_CONTEXT_H
