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

#ifndef AERON_AERON_IPC_PUBLICATION_H
#define AERON_AERON_IPC_PUBLICATION_H

#include "aeron_driver_common.h"
#include "util/aeron_bitutil.h"

typedef struct aeron_ipc_publication_stct
{
    struct conductor_fields_stct
    {
        aeron_driver_managed_resource_t managed_resource;
        aeron_subscribeable_t subscribeable;
    }
    conductor_fields;

    uint8_t conductor_fields_pad[(2 * AERON_CACHE_LINE_LENGTH) - sizeof(struct conductor_fields_stct)];

    int32_t stream_id;
}
aeron_ipc_publication_t;

int aeron_ipc_publication_create(aeron_ipc_publication_t **publication, int32_t stream_id);

#endif //AERON_AERON_IPC_PUBLICATION_H
