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

#include "aeron_driver_sender.h"

int aeron_driver_sender_init(aeron_driver_sender_t *sender, aeron_driver_context_t *context)
{
    sender->context = context;
    return 0;
}

int aeron_driver_sender_do_work(void *clientd)
{
    return 0;
}

void aeron_driver_sender_on_close(void *clientd)
{

}
