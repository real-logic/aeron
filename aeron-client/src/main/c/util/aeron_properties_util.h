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

#ifndef AERON_PROPERTIES_UTIL_H
#define AERON_PROPERTIES_UTIL_H

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

#define AERON_PROPERTIES_MAX_LENGTH 2048

typedef struct aeron_properties_parser_state_stct
{
    char property_str[AERON_PROPERTIES_MAX_LENGTH];
    size_t name_end;
    size_t value_end;
}
aeron_properties_parser_state_t;

typedef int (*aeron_properties_file_handler_func_t)(
    void *clientd, const char *property_name, const char *property_value);

inline void aeron_properties_parse_init(aeron_properties_parser_state_t *state)
{
    state->name_end = 0;
    state->value_end = 0;
}

int aeron_properties_parse_line(
    aeron_properties_parser_state_t *state,
    const char *line,
    size_t length,
    aeron_properties_file_handler_func_t handler,
    void *clientd);

int aeron_properties_parse_file(const char *filename, aeron_properties_file_handler_func_t handler, void *clientd);

int aeron_properties_setenv(const char *name, const char *value);

#define AERON_HTTP_PROPERTIES_TIMEOUT_NS (10 * 1000 * 1000 * 1000LL)

#endif //AERON_PROPERTIES_UTIL_H
