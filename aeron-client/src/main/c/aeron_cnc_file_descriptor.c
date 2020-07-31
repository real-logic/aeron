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

#include "aeron_cnc_file_descriptor.h"

int32_t aeron_cnc_version_volatile(aeron_cnc_metadata_t *metadata)
{
    int32_t cnc_version;
    AERON_GET_VOLATILE(cnc_version, metadata->cnc_version);
    return cnc_version;
}

extern uint8_t *aeron_cnc_to_driver_buffer(aeron_cnc_metadata_t *metadata);
extern uint8_t *aeron_cnc_to_clients_buffer(aeron_cnc_metadata_t *metadata);
extern uint8_t *aeron_cnc_counters_metadata_buffer(aeron_cnc_metadata_t *metadata);
extern uint8_t *aeron_cnc_counters_values_buffer(aeron_cnc_metadata_t *metadata);
extern uint8_t *aeron_cnc_error_log_buffer(aeron_cnc_metadata_t *metadata);
extern size_t aeron_cnc_computed_length(size_t total_length_of_buffers, size_t alignment);
extern bool aeron_cnc_is_file_length_sufficient(aeron_mapped_file_t *cnc_mmap);
