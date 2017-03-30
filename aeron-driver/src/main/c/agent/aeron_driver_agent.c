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

#include <stdio.h>
#include <dlfcn.h>
#include "agent/aeron_driver_agent.h"

#define AERON_INTERCEPT_FUNC_RETURN_ON_ERROR(name,funcvar) \
do \
{ \
    if (NULL == funcvar) \
    { \
        if ((funcvar = (aeron_driver_conductor_on_command_t)dlsym( \
            RTLD_NEXT, name)) == NULL) \
        { \
            fprintf(stderr, "could not hook func <%s>: %s\n", name, dlerror()); \
            return; \
        } \
    } \
} \
while(0)

void aeron_driver_conductor_on_command(int32_t msg_type_id, const void *message, size_t length, void *clientd)
{
    static aeron_driver_conductor_on_command_t _original_func = NULL;

    AERON_INTERCEPT_FUNC_RETURN_ON_ERROR("aeron_driver_conductor_on_command", _original_func);

    /* TODO: add logging */
    _original_func(msg_type_id, message, length, clientd);
}
