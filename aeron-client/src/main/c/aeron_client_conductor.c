/*
 * Copyright 2014-2020 Real Logic Limited.
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

#include "aeron_client_conductor.h"

int aeron_client_conductor_init(aeron_client_conductor_t *conductor, aeron_context_t *context)
{
    return 0;
}

int aeron_client_conductor_do_work(aeron_client_conductor_t *conductor)
{
    return 0;
}

void aeron_client_conductor_on_close(aeron_client_conductor_t *conductor)
{
}

int64_t aeron_client_conductor_add_publication(aeron_t *client, const char *uri)
{
    return -1;
}

int aeron_client_conductor_find_publication(
    aeron_publication_t **publication, aeron_t *client, int64_t registration_id)
{
    return -1;
}
