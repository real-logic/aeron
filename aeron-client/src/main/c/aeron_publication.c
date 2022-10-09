/*
 * Copyright 2014-2022 Real Logic Limited.
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
#include <inttypes.h>

#include "aeronc.h"
#include "aeron_common.h"
#include "aeron_publication.h"
#include "concurrent/aeron_term_appender.h"
#include "aeron_log_buffer.h"
#include "status/aeron_local_sockaddr.h"

int aeron_publication_create(
    aeron_publication_t **publication,
    aeron_client_conductor_t *conductor,
    const char *channel,
    int32_t stream_id,
    int32_t session_id,
    int32_t position_limit_counter_id,
    int64_t *position_limit_addr,
    int32_t channel_status_indicator_id,
    int64_t *channel_status_addr,
    aeron_log_buffer_t *log_buffer,
    int64_t original_registration_id,
    int64_t registration_id)
{
    aeron_publication_t *_publication;

    *publication = NULL;
    if (aeron_alloc((void **)&_publication, sizeof(aeron_publication_t)) < 0)
    {
        AERON_APPEND_ERR("Unable to allocate publication, registration_id: %" PRId64, registration_id);
        return -1;
    }

    _publication->command_base.type = AERON_CLIENT_TYPE_PUBLICATION;

    _publication->log_buffer = log_buffer;
    _publication->log_meta_data = (aeron_logbuffer_metadata_t *)log_buffer->mapped_raw_log.log_meta_data.addr;

    _publication->position_limit_counter_id = position_limit_counter_id;
    _publication->position_limit = position_limit_addr;
    _publication->channel_status_indicator_id = channel_status_indicator_id;
    _publication->channel_status_indicator = channel_status_addr;

    _publication->conductor = conductor;
    _publication->channel = channel;
    _publication->registration_id = registration_id;
    _publication->original_registration_id = original_registration_id;
    _publication->stream_id = stream_id;
    _publication->session_id = session_id;
    _publication->is_closed = false;

    size_t term_length = (size_t)_publication->log_meta_data->term_length;

    _publication->max_possible_position = ((int64_t)term_length << 31);
    _publication->max_payload_length = (size_t)(_publication->log_meta_data->mtu_length - AERON_DATA_HEADER_LENGTH);
    _publication->max_message_length = aeron_compute_max_message_length(term_length);
    _publication->position_bits_to_shift = (size_t)aeron_number_of_trailing_zeroes((int32_t)term_length);
    _publication->initial_term_id = _publication->log_meta_data->initial_term_id;

    *publication = _publication;
    return 0;
}

int aeron_publication_delete(aeron_publication_t *publication)
{
    aeron_free((void *)publication->channel);
    aeron_free(publication);

    return 0;
}

void aeron_publication_force_close(aeron_publication_t *publication)
{
    AERON_PUT_ORDERED(publication->is_closed, true);
}

int aeron_publication_close(
    aeron_publication_t *publication, aeron_notification_t on_close_complete, void *on_close_complete_clientd)
{
    if (NULL != publication)
    {
        bool is_closed;

        AERON_GET_VOLATILE(is_closed, publication->is_closed);
        if (!is_closed)
        {
            AERON_PUT_ORDERED(publication->is_closed, true);

            if (aeron_client_conductor_async_close_publication(
                publication->conductor, publication, on_close_complete, on_close_complete_clientd) < 0)
            {
                return -1;
            }
        }
    }

    return 0;
}

int64_t aeron_publication_offer(
    aeron_publication_t *publication,
    const uint8_t *buffer,
    size_t length,
    aeron_reserved_value_supplier_t reserved_value_supplier,
    void *clientd)
{
    int64_t new_position = AERON_PUBLICATION_CLOSED;

    if (NULL == publication || NULL == buffer)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, publication: %s, buffer: %s",
            AERON_NULL_STR(publication),
            AERON_NULL_STR(buffer));
        return AERON_PUBLICATION_ERROR;
    }

    bool is_closed;
    AERON_GET_VOLATILE(is_closed, publication->is_closed);
    if (!is_closed)
    {
        const int64_t limit = aeron_counter_get_volatile(publication->position_limit);
        const int32_t term_count = aeron_logbuffer_active_term_count(publication->log_meta_data);
        const size_t index = aeron_logbuffer_index_by_term_count(term_count);
        const int64_t raw_tail = aeron_term_appender_raw_tail_volatile(
            &publication->log_meta_data->term_tail_counters[index]);
        const int32_t term_offset = (int32_t)(raw_tail & 0xFFFFFFFF);
        const int32_t term_id = aeron_logbuffer_term_id(raw_tail);
        const int64_t position = aeron_logbuffer_compute_position(
            term_id, term_offset, publication->position_bits_to_shift, publication->initial_term_id);

        if (term_count != (term_id - publication->initial_term_id))
        {
            return AERON_PUBLICATION_ADMIN_ACTION;
        }

        if (position < limit)
        {
            int32_t resulting_offset;
            if (length <= publication->max_payload_length)
            {
                resulting_offset = aeron_term_appender_append_unfragmented_message(
                    &publication->log_buffer->mapped_raw_log.term_buffers[index],
                    &publication->log_meta_data->term_tail_counters[index],
                    buffer,
                    length,
                    reserved_value_supplier,
                    clientd,
                    term_id,
                    publication->session_id,
                    publication->stream_id);
            }
            else
            {
                if (length > publication->max_message_length)
                {
                    AERON_SET_ERR(
                        EINVAL,
                        "aeron_publication_offer: length=%" PRIu64 " > max_message_length=%" PRIu64,
                        (uint64_t)length,
                        (uint64_t)publication->max_message_length);
                    return AERON_PUBLICATION_ERROR;
                }

                resulting_offset = aeron_term_appender_append_fragmented_message(
                    &publication->log_buffer->mapped_raw_log.term_buffers[index],
                    &publication->log_meta_data->term_tail_counters[index],
                    buffer,
                    length,
                    publication->max_payload_length,
                    reserved_value_supplier,
                    clientd,
                    term_id,
                    publication->session_id,
                    publication->stream_id);
            }

            new_position = aeron_publication_new_position(
                publication, term_count, (int32_t)term_offset, term_id, position, resulting_offset);
        }
        else
        {
            new_position = aeron_publication_back_pressure_status(publication, position, (int32_t)length);
        }
    }

    return new_position;
}

int64_t aeron_publication_offerv(
    aeron_publication_t *publication,
    aeron_iovec_t *iov,
    size_t iovcnt,
    aeron_reserved_value_supplier_t reserved_value_supplier,
    void *clientd)
{
    int64_t new_position = AERON_PUBLICATION_CLOSED;

    if (NULL == publication || NULL == iov)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, publication: %s, iov: %s",
            AERON_NULL_STR(publication),
            AERON_NULL_STR(iov));
        return AERON_PUBLICATION_ERROR;
    }

    size_t length = 0;
    for (size_t i = 0; i < iovcnt; i++)
    {
        length += iov[i].iov_len;
    }

    bool is_closed;
    AERON_GET_VOLATILE(is_closed, publication->is_closed);
    if (!is_closed)
    {
        const int64_t limit = aeron_counter_get_volatile(publication->position_limit);
        const int32_t term_count = aeron_logbuffer_active_term_count(publication->log_meta_data);
        const size_t index = aeron_logbuffer_index_by_term_count(term_count);
        const int64_t raw_tail = aeron_term_appender_raw_tail_volatile(
            &publication->log_meta_data->term_tail_counters[index]);
        const int32_t term_offset = (int32_t)(raw_tail & 0xFFFFFFFF);
        const int32_t term_id = aeron_logbuffer_term_id(raw_tail);
        const int64_t position = aeron_logbuffer_compute_position(
            term_id, term_offset, publication->position_bits_to_shift, publication->initial_term_id);

        if (term_count != (term_id - publication->initial_term_id))
        {
            return AERON_PUBLICATION_ADMIN_ACTION;
        }

        if (position < limit)
        {
            int32_t resulting_offset;
            if (length <= publication->max_payload_length)
            {
                resulting_offset = aeron_term_appender_append_unfragmented_messagev(
                    &publication->log_buffer->mapped_raw_log.term_buffers[index],
                    &publication->log_meta_data->term_tail_counters[index],
                    iov,
                    iovcnt,
                    length,
                    reserved_value_supplier,
                    clientd,
                    term_id,
                    publication->session_id,
                    publication->stream_id);
            }
            else
            {
                if (length > publication->max_message_length)
                {
                    AERON_SET_ERR(
                        EINVAL,
                        "aeron_publication_offerv: length=%" PRIu64 " > max_message_length=%" PRIu64,
                        (uint64_t)length,
                        (uint64_t)publication->max_message_length);
                    return AERON_PUBLICATION_ERROR;
                }

                resulting_offset = aeron_term_appender_append_fragmented_messagev(
                    &publication->log_buffer->mapped_raw_log.term_buffers[index],
                    &publication->log_meta_data->term_tail_counters[index],
                    iov,
                    iovcnt,
                    length,
                    publication->max_payload_length,
                    reserved_value_supplier,
                    clientd,
                    term_id,
                    publication->session_id,
                    publication->stream_id);
            }

            new_position = aeron_publication_new_position(
            publication, term_count, (int32_t)term_offset, term_id, position, resulting_offset);
        }
        else
        {
            new_position = aeron_publication_back_pressure_status(publication, position, (int32_t)length);
        }
    }

    return new_position;
}

int64_t aeron_publication_try_claim(aeron_publication_t *publication, size_t length, aeron_buffer_claim_t *buffer_claim)
{
    int64_t new_position = AERON_PUBLICATION_CLOSED;

    if (NULL == publication || NULL == buffer_claim)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, publication: %s, buffer_claim: %s",
            AERON_NULL_STR(publication),
            AERON_NULL_STR(buffer_claim));
        return AERON_PUBLICATION_ERROR;
    }
    else if (length > publication->max_payload_length)
    {
        AERON_SET_ERR(
            EINVAL,
            "aeron_publication_try_claim: length=%" PRIu64 " > max_payload_length=%" PRIu64,
            (uint64_t)length,
            (uint64_t)publication->max_payload_length);
        return AERON_PUBLICATION_ERROR;
    }

    bool is_closed;
    AERON_GET_VOLATILE(is_closed, publication->is_closed);
    if (!is_closed)
    {
        const int64_t limit = aeron_counter_get_volatile(publication->position_limit);
        const int32_t term_count = aeron_logbuffer_active_term_count(publication->log_meta_data);
        const size_t index = aeron_logbuffer_index_by_term_count(term_count);
        const int64_t raw_tail = aeron_term_appender_raw_tail_volatile(
            &publication->log_meta_data->term_tail_counters[index]);
        const int32_t term_offset = (int32_t)(raw_tail & 0xFFFFFFFF);
        const int32_t term_id = aeron_logbuffer_term_id(raw_tail);
        const int64_t position = aeron_logbuffer_compute_position(
            term_id, term_offset, publication->position_bits_to_shift, publication->initial_term_id);

        if (term_count != (term_id - publication->initial_term_id))
        {
            return AERON_PUBLICATION_ADMIN_ACTION;
        }

        if (position < limit)
        {
            int32_t resulting_offset = aeron_term_appender_claim(
                &publication->log_buffer->mapped_raw_log.term_buffers[index],
                &publication->log_meta_data->term_tail_counters[index],
                length,
                buffer_claim,
                term_id,
                publication->session_id,
                publication->stream_id);

            new_position = aeron_publication_new_position(
                publication, term_count, (int32_t)term_offset, term_id, position, resulting_offset);
        }
        else
        {
            new_position = aeron_publication_back_pressure_status(publication, position, (int32_t)length);
        }
    }

    return new_position;
}

bool aeron_publication_is_closed(aeron_publication_t *publication)
{
    bool is_closed = true;

    if (NULL != publication)
    {
        AERON_GET_VOLATILE(is_closed, publication->is_closed);
    }

    return is_closed;
}

bool aeron_publication_is_connected(aeron_publication_t *publication)
{
    if (NULL != publication && !aeron_publication_is_closed(publication))
    {
        int32_t is_connected;

        AERON_GET_VOLATILE(is_connected, publication->log_meta_data->is_connected);
        return 1 == is_connected;
    }

    return false;
}

int aeron_publication_constants(aeron_publication_t *publication, aeron_publication_constants_t *constants)
{
    if (NULL == publication || NULL == constants)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, publication: %s, constants: %s",
            AERON_NULL_STR(publication),
            AERON_NULL_STR(constants));
        return -1;
    }

    constants->channel = publication->channel;
    constants->original_registration_id = publication->original_registration_id;
    constants->registration_id = publication->registration_id;
    constants->max_possible_position = publication->max_possible_position;
    constants->position_bits_to_shift = publication->position_bits_to_shift;
    constants->term_buffer_length = (size_t)publication->log_meta_data->term_length;
    constants->max_message_length = publication->max_message_length;
    constants->max_payload_length = publication->max_payload_length;
    constants->stream_id = publication->stream_id;
    constants->session_id = publication->session_id;
    constants->initial_term_id = publication->initial_term_id;
    constants->publication_limit_counter_id = publication->position_limit_counter_id;
    constants->channel_status_indicator_id = publication->channel_status_indicator_id;

    return 0;
}

int64_t aeron_publication_channel_status(aeron_publication_t *publication)
{
    if (NULL != publication && !aeron_publication_is_closed(publication))
    {
        int64_t value;
        AERON_GET_VOLATILE(value, *publication->channel_status_indicator);

        return value;
    }

    return AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_NO_ID_ALLOCATED;
}

int64_t aeron_publication_position(aeron_publication_t *publication)
{
    if (NULL == publication)
    {
        AERON_SET_ERR(EINVAL, "Parameters must not be null, publication: %s", AERON_NULL_STR(publication));
        return AERON_PUBLICATION_ERROR;
    }

    bool is_closed;
    AERON_GET_VOLATILE(is_closed, publication->is_closed);
    if (is_closed)
    {
        return AERON_PUBLICATION_CLOSED;
    }

    const int32_t term_count = aeron_logbuffer_active_term_count(publication->log_meta_data);
    const size_t index = aeron_logbuffer_index_by_term_count(term_count);
    const int64_t raw_tail = aeron_term_appender_raw_tail_volatile(
        &publication->log_meta_data->term_tail_counters[index]);
    const int64_t term_length = publication->log_meta_data->term_length;
    const int64_t term_offset = term_length < (raw_tail & 0xFFFFFFFF) ? term_length : (raw_tail & 0xFFFFFFFF);
    const int32_t term_id = aeron_logbuffer_term_id(raw_tail);
    const int64_t position = aeron_logbuffer_compute_position(
        term_id, (int32_t)term_offset, publication->position_bits_to_shift, publication->initial_term_id);

    return position;
}

int64_t aeron_publication_position_limit(aeron_publication_t *publication)
{
    if (NULL == publication)
    {
        AERON_SET_ERR(EINVAL, "Parameters must not be null, publication: %s", AERON_NULL_STR(publication));
        return AERON_PUBLICATION_ERROR;
    }

    bool is_closed;
    AERON_GET_VOLATILE(is_closed, publication->is_closed);
    if (is_closed)
    {
        return AERON_PUBLICATION_CLOSED;
    }

    return aeron_counter_get_volatile(publication->position_limit);
}

extern int64_t aeron_publication_new_position(
    aeron_publication_t *publication,
    int32_t term_count,
    int32_t term_offset,
    int32_t term_id,
    int64_t position,
    int32_t resulting_offset);

extern int64_t aeron_publication_back_pressure_status(
    aeron_publication_t *publication, int64_t current_position, int32_t message_length);

const char *aeron_publication_channel(aeron_publication_t *publication)
{
    return publication->channel;
}

int32_t aeron_publication_stream_id(aeron_publication_t *publication)
{
    return publication->stream_id;
}

int32_t aeron_publication_session_id(aeron_publication_t *publication)
{
    return publication->session_id;
}

int aeron_publication_local_sockaddrs(
    aeron_publication_t *publication, aeron_iovec_t *address_vec, size_t address_vec_len)
{
    if (NULL == publication || NULL == address_vec)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, publication: %s, address_vec: %s",
            AERON_NULL_STR(publication),
            AERON_NULL_STR(address_vec));
        return -1;
    }
    if (address_vec_len < 1)
    {
        AERON_SET_ERR(
            EINVAL, "Parameters must be valid, address_vec_len (%" PRIu64 ") < 1", (uint64_t)address_vec_len);
        return -1;
    }

    return aeron_local_sockaddr_find_addrs(
        &publication->conductor->counters_reader,
        publication->channel_status_indicator_id,
        address_vec,
        address_vec_len);
}
