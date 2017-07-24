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

#include "concurrent/aeron_logbuffer_descriptor.h"

extern int32_t aeron_logbuffer_term_offset(int64_t raw_tail, int32_t term_length);
extern int32_t aeron_logbuffer_term_id(int64_t raw_tail);
extern size_t aeron_logbuffer_index_by_position(int64_t position, size_t position_bits_to_shift);
extern size_t aeron_logbuffer_index_by_term(int32_t initial_term_id, int32_t active_term_id);
extern int64_t aeron_logbuffer_compute_position(
    int32_t active_term_id, int32_t term_offset, size_t position_bits_to_shift, int32_t initial_term_id);
extern int32_t aeron_logbuffer_compute_term_id_from_position(
    int64_t position, size_t position_bits_to_shift, int32_t initial_term_id);
extern int32_t aeron_logbuffer_compute_term_offset_from_position(int64_t position, size_t position_bits_to_shift);
extern void aeron_logbuffer_rotate_log(
    aeron_logbuffer_metadata_t *log_meta_data, size_t active_partition_index, int32_t term_id);
extern void aeron_logbuffer_fill_default_header(
    uint8_t *log_meta_data_buffer, int32_t session_id, int32_t stream_id, int32_t initial_term_id);
extern void aeron_logbuffer_apply_default_header(uint8_t *log_meta_data_buffer, uint8_t *buffer);
