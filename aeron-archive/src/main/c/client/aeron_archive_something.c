/*
 * Copyright 2014-2024 Real Logic Limited.
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
#ifdef HAVE_BSDSTDLIB_H
#include <bsd/stdlib.h>
#endif
#endif

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <inttypes.h>

#include "aeron_archive_something.h"

#include "c/aeron_archive_client/connectRequest.h"

int something(void)
{
    uint64_t buffer_length = 1000000;
    char buffer[buffer_length];
    char *channel_string = "some fancy channel";

    {
        struct aeron_archive_client_connectRequest codec;
        struct aeron_archive_client_messageHeader hdr;

        aeron_archive_client_connectRequest_wrap_and_apply_header(&codec, buffer, 0, buffer_length, &hdr);
        aeron_archive_client_connectRequest_set_correlationId(&codec, 123);
        aeron_archive_client_connectRequest_set_responseStreamId(&codec, 456);
        aeron_archive_client_connectRequest_set_version(&codec, 999);
        aeron_archive_client_connectRequest_put_responseChannel(&codec, channel_string, strlen(channel_string));

        for (uint64_t i = 0; i < aeron_archive_client_connectRequest_sbe_position(&codec); i++)
        {
            printf("[%llu] '%x' '%c'\n", i, buffer[i], buffer[i]);
        }
    }

    struct aeron_archive_client_messageHeader hdr2;
    aeron_archive_client_messageHeader_wrap(&hdr2, buffer, 0,
        aeron_archive_client_messageHeader_sbe_schema_version(), buffer_length);
    uint16_t templateId = aeron_archive_client_messageHeader_templateId(&hdr2);

    printf("template id :: %i\n", templateId);

    switch(templateId)
    {
        case AERON_ARCHIVE_CLIENT_CONNECT_REQUEST_SBE_TEMPLATE_ID:
        {
            struct aeron_archive_client_connectRequest codec2;
            aeron_archive_client_connectRequest_wrap_for_decode(
                &codec2,
                buffer,
                aeron_archive_client_messageHeader_encoded_length(),
                aeron_archive_client_connectRequest_sbe_block_length(),
                aeron_archive_client_connectRequest_sbe_schema_version(),
                buffer_length);

            uint64_t correlationId = aeron_archive_client_connectRequest_correlationId(&codec2);
            printf("correlation id :: %llu\n", correlationId);
            int32_t streamId = aeron_archive_client_connectRequest_responseStreamId(&codec2);
            printf("stream id :: %i\n", streamId);
            int32_t version = aeron_archive_client_connectRequest_version(&codec2);
            printf("version :: %i\n", version);

            int out_channel_length = 1000;
            char out_channel[out_channel_length];
            memset(out_channel, 0, out_channel_length);

            uint64_t l = aeron_archive_client_connectRequest_get_responseChannel(
                &codec2,
                out_channel,
                out_channel_length);
            printf("out channel :: %llu :: \"%s\"\n", l, out_channel);
            break;
        }

        default:
            printf("WHAT????\n");
    }


    return 123;
}
