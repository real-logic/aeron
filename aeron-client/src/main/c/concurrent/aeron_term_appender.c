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

#include "aeron_term_appender.h"

extern int64_t aeron_term_appender_get_and_add_raw_tail(volatile int64_t *addr, size_t aligned_length);
extern int aeron_term_appender_check_term(int32_t expected_term_id, int32_t term_id);

extern void aeron_term_appender_header_write(
    aeron_mapped_buffer_t *term_buffer,
    int32_t offset,
    size_t length,
    int32_t term_id,
    int32_t session_id,
    int32_t stream_id);

extern int32_t aeron_term_appender_handle_end_of_log_condition(
    aeron_mapped_buffer_t *term_buffer,
    int32_t term_offset,
    int32_t term_length,
    int32_t term_id,
    int32_t session_id,
    int32_t stream_id);

extern int32_t aeron_term_appender_claim(
    aeron_mapped_buffer_t *term_buffer,
    volatile int64_t *term_tail_counter,
    size_t length,
    aeron_buffer_claim_t *buffer_claim,
    int32_t active_term_id,
    int32_t session_id,
    int32_t stream_id);

extern int32_t aeron_term_appender_append_unfragmented_message(
    aeron_mapped_buffer_t *term_buffer,
    volatile int64_t *term_tail_counter,
    uint8_t *buffer,
    size_t length,
    aeron_reserved_value_supplier_t reserved_value_supplier,
    void *clientd,
    int32_t active_term_id,
    int32_t session_id,
    int32_t stream_id);

extern int32_t aeron_term_appender_append_unfragmented_messagev(
    aeron_mapped_buffer_t *term_buffer,
    volatile int64_t *term_tail_counter,
    aeron_iovec_t *iov,
    size_t iovcnt,
    size_t length,
    aeron_reserved_value_supplier_t reserved_value_supplier,
    void *clientd,
    int32_t active_term_id,
    int32_t session_id,
    int32_t stream_id);
