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

#ifndef AERON_PARSE_UTIL_H
#define AERON_PARSE_UTIL_H

#include <stdint.h>

#define AERON_MAX_HOST_LENGTH (384)
#define AERON_MAX_PORT_LENGTH (8)
#define AERON_MAX_PREFIX_LENGTH (8)

typedef struct aeron_parsed_address_stct
{
    char host[AERON_MAX_HOST_LENGTH];
    char port[AERON_MAX_PORT_LENGTH];
    int ip_version_hint;
}
aeron_parsed_address_t;

typedef struct aeron_parsed_interface_stct
{
    char host[AERON_MAX_HOST_LENGTH];
    char port[AERON_MAX_PORT_LENGTH];
    char prefix[AERON_MAX_PREFIX_LENGTH];
    int ip_version_hint;
}
aeron_parsed_interface_t;

int aeron_parse_size64(const char *str, uint64_t *result);

int aeron_parse_duration_ns(const char *str, uint64_t *result);

int aeron_address_split(const char *address_str, aeron_parsed_address_t *parsed_address);

int aeron_interface_split(const char *interface_str, aeron_parsed_interface_t *parsed_interface);

#endif //AERON_AERON_PROP_UTIL_H
