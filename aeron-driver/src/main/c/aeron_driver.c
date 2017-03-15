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

#include <stddef.h>
#include "aeronmd.h"
#include "aeron_alloc.h"

int aeron_driver_init(aeron_driver_t **driver, aeron_driver_context_t *context)
{
    aeron_driver_t *_driver = NULL;

    if (NULL == driver || NULL == context)
    {
        /* TODO: EINVAL */
        return -1;
    }

    if (aeron_alloc((void **)&_driver, sizeof(aeron_driver_t)) < 0)
    {
        return -1;
    }

    _driver->context = context;

    *driver = _driver;
    return 0;
}

int aeron_driver_start(aeron_driver_t *driver)
{
    return 0;
}

int aeron_driver_close(aeron_driver_t *driver)
{
    if (NULL == driver)
    {
        /* TODO: EINVAL */
        return -1;
    }

    aeron_free(driver);
    return 0;
}
