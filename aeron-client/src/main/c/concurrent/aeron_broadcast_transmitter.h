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

#ifndef AERON_BROADCAST_TRANSMITTER_H
#define AERON_BROADCAST_TRANSMITTER_H

#include "util/aeron_bitutil.h"
#include "aeron_broadcast_descriptor.h"

typedef struct aeron_broadcast_transmitter_stct
{
    uint8_t *buffer;
    aeron_broadcast_descriptor_t *descriptor;
    size_t capacity;
    size_t max_message_length;
}
aeron_broadcast_transmitter_t;

int aeron_broadcast_transmitter_init(aeron_broadcast_transmitter_t *transmitter, void *buffer, size_t length);

int aeron_broadcast_transmitter_transmit(
    aeron_broadcast_transmitter_t *transmitter, int32_t msg_type_id, const void *msg, size_t length);

#endif //AERON_BROADCAST_TRANSMITTER_H
