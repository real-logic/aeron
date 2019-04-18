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

#ifndef AERON_HTTP_UTIL_H
#define AERON_HTTP_UTIL_H

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

#include "aeron_socket.h"
#include "aeron_parse_util.h"

#define AERON_MAX_HTTP_USERINFO_LENGTH (384)
#define AERON_MAX_HTTP_PATH_AND_QUERY_LENGTH (512)

typedef struct aeron_http_parsed_url_stct
{
    char userinfo[AERON_MAX_HTTP_USERINFO_LENGTH];
    char host_and_port[AERON_MAX_HOST_LENGTH + 1 + AERON_MAX_PORT_LENGTH];
    char path_and_query[AERON_MAX_HTTP_PATH_AND_QUERY_LENGTH];
    struct sockaddr_storage address;
    int ip_version_hint;
}
aeron_http_parsed_url_t;

int aeron_http_parse_url(const char *url, aeron_http_parsed_url_t *parsed_url);

int aeron_http_retrieve(const char **content, const char *url, int64_t timeout_ns);

#endif //AERON_HTTP_UTIL_H
