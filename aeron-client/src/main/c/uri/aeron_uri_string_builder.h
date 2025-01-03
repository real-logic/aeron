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

#ifndef AERON_URI_STRING_BUILDER_H
#define AERON_URI_STRING_BUILDER_H

#include "aeron_common.h"
#include "collections/aeron_str_to_ptr_hash_map.h"
#include "uri/aeron_uri.h"

typedef struct aeron_uri_string_builder_stct
{
    aeron_str_to_ptr_hash_map_t params;
    bool closed;
}
aeron_uri_string_builder_t;

#define AERON_URI_STRING_BUILDER_PREFIX_KEY "__prefix"
#define AERON_URI_STRING_BUILDER_MEDIA_KEY "__media"

int aeron_uri_string_builder_init_new(aeron_uri_string_builder_t *builder);
int aeron_uri_string_builder_init_on_string(aeron_uri_string_builder_t *builder, const char *uri);
/* TODO:
int aeron_uri_string_builder_init_on_uri(aeron_uri_string_builder_t *builder, aeron_uri_t *uri);
 */

int aeron_uri_string_builder_close(aeron_uri_string_builder_t *builder);

// a _put with a value of NULL will clear the entry
int aeron_uri_string_builder_put(aeron_uri_string_builder_t *builder, const char *key, const char *value);
int aeron_uri_string_builder_put_int32(aeron_uri_string_builder_t *builder, const char *key, int32_t value);
int aeron_uri_string_builder_put_int64(aeron_uri_string_builder_t *builder, const char *key, int64_t value);

const char *aeron_uri_string_builder_get(aeron_uri_string_builder_t *builder, const char *key);

int aeron_uri_string_builder_sprint(aeron_uri_string_builder_t *builder, char *buffer, size_t buffer_len);

int aeron_uri_string_builder_set_initial_position(
    aeron_uri_string_builder_t *builder,
    int64_t position,
    int32_t initial_term_id,
    int32_t term_length);

#endif //AERON_URI_STRING_BUILDER_H
