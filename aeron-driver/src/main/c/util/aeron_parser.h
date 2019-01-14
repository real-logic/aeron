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

#ifndef AERON_PARSER_H
#define AERON_PARSER_H
#include <stddef.h>

typedef struct
{
    size_t offset;
    size_t length;
} aeron_regex_matches_t;

int aeron_parse_ipv4(const char* text, aeron_regex_matches_t* matches, size_t max_matches_count);
int aeron_parse_ipv6(const char* text, aeron_regex_matches_t* matches, size_t max_matches_count);

#endif //AERON_PARSER_H
