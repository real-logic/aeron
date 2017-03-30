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

#ifndef AERON_AERON_DRIVER_AGENT_H
#define AERON_AERON_DRIVER_AGENT_H

#include "aeron_driver_conductor.h"
#include "command/aeron_control_protocol.h"

typedef void (*aeron_driver_conductor_on_command_t)(int32_t, const void *, size_t, void *);
typedef void (*aeron_driver_conductor_client_transmit_t)(aeron_driver_conductor_t *, int32_t, const void *, size_t);

#endif //AERON_AERON_DRIVER_AGENT_H
