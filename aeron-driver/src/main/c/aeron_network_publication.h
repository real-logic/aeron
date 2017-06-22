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

#ifndef AERON_AERON_NETWORK_PUBLICATION_H
#define AERON_AERON_NETWORK_PUBLICATION_H

#include "util/aeron_bitutil.h"
#include "util/aeron_fileutil.h"
#include "aeron_driver_common.h"

typedef enum aeron_network_publication_status_enum
{
    AERON_NETWORK_PUBLICATION_STATUS_ACTIVE,
    AERON_NETWORK_PUBLICATION_STATUS_INACTIVE,
    AERON_NETWORK_PUBLICATION_STATUS_LINGER
}
aeron_network_publication_status_t;

typedef struct aeron_send_channel_endpoint_stct aeron_send_channel_endpoint_t;

typedef struct aeron_network_publication_stct
{
    struct aeron_network_publication_conductor_fields_stct
    {
        aeron_driver_managed_resource_t managed_resource;
        aeron_subscribeable_t subscribeable;
        int64_t cleaning_position;
        int64_t trip_limit;
        int64_t consumer_position;
        int32_t refcnt;
        bool has_reached_end_of_life;
        aeron_network_publication_status_t status;
    }
    conductor_fields;

    uint8_t conductor_fields_pad[
        (2 * AERON_CACHE_LINE_LENGTH) - sizeof(struct aeron_network_publication_conductor_fields_stct)];

    aeron_mapped_raw_log_t mapped_raw_log;
    aeron_position_t pub_lmt_position;
    aeron_logbuffer_metadata_t *log_meta_data;
    aeron_send_channel_endpoint_t *endpoint;

    char *log_file_name;
    int64_t term_window_length;
    int64_t trip_gain;
    int64_t linger_timeout_ns;
    int32_t session_id;
    int32_t stream_id;
    int32_t initial_term_id;
    size_t log_file_name_length;
    size_t position_bits_to_shift;
    bool is_exclusive;
    aeron_map_raw_log_close_func_t map_raw_log_close_func;
}
aeron_network_publication_t;

int aeron_network_publication_send(aeron_network_publication_t *publication, int64_t now_ns);

#endif //AERON_AERON_NETWORK_PUBLICATION_H
