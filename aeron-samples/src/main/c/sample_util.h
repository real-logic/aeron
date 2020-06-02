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

#ifndef AERON_SAMPLE_UTIL_H
#define AERON_SAMPLE_UTIL_H

#include "aeronc.h"

void print_available_image(void *clientd, aeron_subscription_t *subscription, aeron_image_t *image);
void print_unavailable_image(void *clientd, aeron_subscription_t *subscription, aeron_image_t *image);

#endif //AERON_SAMPLE_UTIL_H
