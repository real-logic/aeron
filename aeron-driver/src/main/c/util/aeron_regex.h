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

#ifndef AERON_REGEX_H
#define AERON_REGEX_H

typedef void* aeron_regex_t;

typedef struct
{
    size_t offset;
    size_t length;
} aeron_regex_matches_t;

int aeron_regcomp(aeron_regex_t* output, const char* pattern, char* error, size_t error_max_size);

int aeron_regexec(aeron_regex_t regex, const char* text, aeron_regex_matches_t* matches, size_t max_matches_count, char* error, size_t max_error_size);

#endif //AERON_REGEX_H
