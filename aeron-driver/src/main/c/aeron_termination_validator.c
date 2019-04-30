/*
 * Copyright 2014-2019 Real Logic Ltd.
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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <string.h>
#include "util/aeron_error.h"
#include "util/aeron_dlopen.h"
#include "aeron_termination_validator.h"

bool aeron_driver_termination_validator_default_allow(void *state, uint8_t *token_buffer, int32_t token_length)
{
    return true;
}

bool aeron_driver_termination_validator_default_deny(void *state, uint8_t *token_buffer, int32_t token_length)
{
    return false;
}

aeron_driver_termination_validator_func_t aeron_driver_termination_validator_load(const char *validator_name)
{
    aeron_driver_termination_validator_func_t func = NULL;

    if (strncmp(validator_name, "allow", sizeof("allow")) == 0)
    {
        return aeron_driver_termination_validator_load("aeron_driver_termination_validator_default_allow");
    }
    else if (strncmp(validator_name, "deny", sizeof("deny")) == 0)
    {
        return aeron_driver_termination_validator_load("aeron_driver_termination_validator_default_deny");
    }
    else
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
        if ((func = (aeron_driver_termination_validator_func_t) aeron_dlsym(RTLD_DEFAULT, validator_name)) == NULL)
        {
            aeron_set_err(
                EINVAL, "could not find termination validator %s: dlsym - %s", validator_name, aeron_dlerror());
            return NULL;
        }
#pragma GCC diagnostic pop
    }

    return func;
}
