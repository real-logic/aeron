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

#include <errno.h>

#include "aeron_cnc.h"
#include "concurrent/aeron_thread.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

int aeron_cnc_init(aeron_cnc_t **aeron_cnc, const char *base_path, int64_t timeout_ms)
{
    aeron_cnc_t *_aeron_cnc;

    if (aeron_alloc((void **)&_aeron_cnc, sizeof(aeron_cnc_t)) < 0)
    {
        aeron_set_err_from_last_err_code("allocate aeron_cnc_t, %s:%d", __FILE__, __LINE__);
        return AERON_CNC_LOAD_FAILED;
    }

    strncpy(_aeron_cnc->base_path, base_path, sizeof(_aeron_cnc->base_path));

    int64_t deadline_ms = aeron_epoch_clock() + timeout_ms;
    do
    {
        aeron_cnc_load_result_t result = aeron_cnc_map_file_and_load_metadata(
            base_path, &_aeron_cnc->cnc_mmap, &_aeron_cnc->metadata);

        if (AERON_CNC_LOAD_SUCCESS == result)
        {
            break;
        }
        else if (AERON_CNC_LOAD_FAILED == result)
        {
            aeron_set_err_from_last_err_code("Failed to load aeron_cnc_t, %s:%d", __FILE__, __LINE__);
            goto error;
        }
        else
        {
            if (deadline_ms <= aeron_epoch_clock())
            {
                aeron_set_err(AERON_CLIENT_ERROR_DRIVER_TIMEOUT, "Timed out waiting for CnC file to become available");
                goto error;
            }

            aeron_micro_sleep(16 * 1000);
        }
    }
    while (true);

    *aeron_cnc = _aeron_cnc;
    return 0;

error:
    aeron_free(_aeron_cnc);
    return -1;
}

const char *aeron_cnc_filename(aeron_cnc_t *aeron_cnc)
{
    return aeron_cnc->base_path;
}

int aeron_cnc_constants(aeron_cnc_t *aeron_cnc, aeron_cnc_constants_t *constants)
{
    if (NULL == aeron_cnc || NULL == constants)
    {
        aeron_set_err(EINVAL, "Both aeron_cnc and constants must not be null");
        return -1;
    }

    memcpy(constants, aeron_cnc->metadata, sizeof(aeron_cnc_constants_t));

    return 0;
}

int64_t aeron_cnc_to_driver_heartbeat(aeron_cnc_t *aeron_cnc)
{
    uint8_t *to_driver_buffer = aeron_cnc_to_driver_buffer(aeron_cnc->metadata);
    aeron_mpsc_rb_t to_driver_rb = { 0 };
    if (aeron_mpsc_rb_init(&to_driver_rb, to_driver_buffer, aeron_cnc->metadata->to_driver_buffer_length) < 0)
    {
        return -1;
    }

    return aeron_mpsc_rb_consumer_heartbeat_time_value(&to_driver_rb);
}

void aeron_cnc_close(aeron_cnc_t *aeron_cnc)
{
    aeron_unmap(&aeron_cnc->cnc_mmap);
    aeron_free(aeron_cnc);
}

