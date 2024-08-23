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

#ifndef AERON_ARCHIVE_RECORDING_SIGNAL_H
#define AERON_ARCHIVE_RECORDING_SIGNAL_H

#include "aeron_archive.h"

int aeron_archive_recording_signal_dispatch_buffer(aeron_archive_context_t *ctx, const uint8_t* buffer, size_t length);

void aeron_archive_recording_signal_dispatch_signal(aeron_archive_context_t *ctx, aeron_archive_recording_signal_t *signal);

#endif //AERON_ARCHIVE_RECORDING_SIGNAL_H
