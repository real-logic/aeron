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

#ifndef AERON_AERON_PUBLICATION_IMAGE_H
#define AERON_AERON_PUBLICATION_IMAGE_H

#include "aeron_driver_common.h"
#include "media/aeron_receive_channel_endpoint.h"

typedef enum aeron_publication_image_status_enum
{
    AERON_PUBLICATION_IMAGE_STATUS_ACTIVE,
    AERON_PUBLICATION_IMAGE_STATUS_DRAINING,
    AERON_PUBLICATION_IMAGE_STATUS_LINGER,
    AERON_PUBLICATION_IMAGE_STATUS_CLOSING
}
aeron_publication_image_status_t;

typedef struct aeron_publication_image_stct
{
    struct aeron_network_publication_conductor_fields_stct
    {
        aeron_driver_managed_resource_t managed_resource;
        aeron_subscribeable_t subscribeable;
        int64_t clean_position;
        int64_t time_of_last_activity_ns;
        int32_t refcnt;
        bool has_reached_end_of_life;
        aeron_publication_image_status_t status;
    }
    conductor_fields;

    aeron_receive_channel_endpoint_t *endpoint;

    int64_t correlation_id;
    int32_t session_id;
    int32_t stream_id;
}
aeron_publication_image_t;

#endif //AERON_AERON_PUBLICATION_IMAGE_H
