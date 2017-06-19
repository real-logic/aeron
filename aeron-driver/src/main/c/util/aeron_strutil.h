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

#ifndef AERON_AERON_STRUTIL_H
#define AERON_AERON_STRUTIL_H

#include <stdint.h>
#include <stddef.h>

void aeron_format_date(char *str, size_t count, int64_t timestamp);

#define AERON_FORMAT_HEX_LENGTH(b) ((2 * (b)) + 1)
void aeron_format_to_hex(char *str, size_t str_length, uint8_t *data, size_t data_len);

#endif //AERON_AERON_STRUTIL_H
