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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <string.h>
#include "util/aeron_error.h"
#include "util/aeron_symbol_table.h"
#include "aeron_termination_validator.h"

static const aeron_symbol_table_func_t aeron_termination_validator_table[] =
    {
        {
            "allow",
            "aeron_driver_termination_validator_default_allow",
            (aeron_fptr_t)aeron_driver_termination_validator_default_allow
        },
        {
            "deny",
            "aeron_driver_termination_validator_default_deny",
            (aeron_fptr_t)aeron_driver_termination_validator_default_deny
        }
    };

static const size_t aeron_termination_validator_table_length =
    sizeof(aeron_termination_validator_table) / sizeof(aeron_symbol_table_func_t);

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
    return (aeron_driver_termination_validator_func_t)aeron_symbol_table_func_load(
        aeron_termination_validator_table,
        aeron_termination_validator_table_length,
        validator_name,
        "terminate validator");
}
