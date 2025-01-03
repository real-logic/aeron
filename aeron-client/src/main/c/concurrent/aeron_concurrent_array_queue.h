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

#ifndef AERON_CONCURRENT_ARRAY_QUEUE_H
#define AERON_CONCURRENT_ARRAY_QUEUE_H

typedef enum aeron_queue_offer_result_stct
{
    AERON_OFFER_SUCCESS = 0,
    AERON_OFFER_ERROR = -2,
    AERON_OFFER_FULL = -1
}
aeron_queue_offer_result_t;

typedef void (*aeron_queue_drain_func_t)(void *clientd, void *item);

#endif //AERON_CONCURRENT_ARRAY_QUEUE_H
