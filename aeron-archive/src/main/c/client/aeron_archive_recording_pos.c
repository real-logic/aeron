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

#include "aeron_archive_recording_pos.h"
#include "util/aeron_error.h"
#include "concurrent/aeron_counters_manager.h"

#define AERON_ARCHIVE_RECORDING_POSITION_TYPE_ID 100

#pragma pack(push)
#pragma pack(4)
struct aeron_archive_recording_pos_key_defn
{
    int64_t recording_id;
    int32_t session_id;
    int32_t source_identity_length;
};
#pragma pack(pop)

int32_t aeron_archive_recording_pos_find_counter_id_by_session_id(aeron_counters_reader_t *counters_reader, int32_t session_id)
{
    for (int32_t i = 0, size = aeron_counters_reader_max_counter_id(counters_reader); i < size; i++)
    {
        int32_t counter_state;

        if (aeron_counters_reader_counter_state(counters_reader, i, &counter_state) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return AERON_NULL_COUNTER_ID;
        }

        if (AERON_COUNTER_RECORD_ALLOCATED == counter_state)
        {
            int32_t type_id;

            if (aeron_counters_reader_counter_type_id(counters_reader, i, &type_id) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                return AERON_NULL_COUNTER_ID;
            }

            if (AERON_ARCHIVE_RECORDING_POSITION_TYPE_ID == type_id)
            {
                struct aeron_archive_recording_pos_key_defn *key;

                if (aeron_counters_reader_metadata_key(counters_reader, i, (uint8_t **)&key) < 0)
                {
                    AERON_SET_ERR(-1, "unable to locate metadata key for counter %i", i);
                    return AERON_NULL_COUNTER_ID;
                }

                if (key->session_id == session_id)
                {
                    return i;
                }
            }
        }
        else if (AERON_COUNTER_RECORD_UNUSED == counter_state)
        {
            break;
        }
    }

    return AERON_NULL_COUNTER_ID;
}

int64_t aeron_archive_recording_pos_get_recording_id(aeron_counters_reader_t *counters_reader, int32_t counter_id)
{
    int32_t state, type_id;

    if (aeron_counters_reader_counter_state(counters_reader, counter_id, &state) < 0 ||
        aeron_counters_reader_counter_type_id(counters_reader, counter_id, &type_id) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return AERON_NULL_COUNTER_ID;
    }

    if (AERON_COUNTER_RECORD_ALLOCATED != state ||
        AERON_ARCHIVE_RECORDING_POSITION_TYPE_ID != type_id)
    {
        return AERON_NULL_COUNTER_ID;
    }

    struct aeron_archive_recording_pos_key_defn *key;

    if (aeron_counters_reader_metadata_key(counters_reader, counter_id, (uint8_t **)&key) < 0)
    {
        return AERON_NULL_COUNTER_ID;
    }

    return key->recording_id;
}
