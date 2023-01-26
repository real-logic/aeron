//
// Created by mike on 23/01/23.
//

#include <errno.h>
#include <inttypes.h>

#include "aeron_deque.h"
#include "aeron_alloc.h"
#include "aeron_error.h"


static inline size_t aeron_deque_size(aeron_deque_t *deque)
{
    return deque->last_element >= deque->first_element ? deque->last_element - deque->first_element :
        deque->element_count - (deque->first_element - deque->last_element);
}

static inline bool aeron_deque_has_capacity(aeron_deque_t *deque)
{
    return aeron_deque_size(deque) < (deque->element_count - 1);
}

int aeron_deque_init(aeron_deque_t *deque, size_t initial_element_count, size_t element_size)
{
    size_t initial_size = initial_element_count * element_size;
    if (aeron_alloc((void **)&deque->data, initial_size) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    deque->element_count = initial_element_count;
    deque->element_size = element_size;
    deque->first_element = 0;
    deque->last_element = 0;

    return 0;
}

void aeron_deque_close(aeron_deque_t *deque)
{
    aeron_free(deque->data);
}

static int aeron_deque_reallocate(aeron_deque_t *deque)
{
    if ((SIZE_MAX >> 1) <= deque->element_count)
    {
        AERON_SET_ERR(ENOMEM, "increased size will exceed %" PRIu64, SIZE_MAX);
        return -1;
    }

    if (deque->first_element == deque->last_element)
    {
        return 0;
    }

    size_t new_element_count = deque->element_count << 1;
    size_t new_size = new_element_count * deque->element_size;
    void *new_data = NULL;

    if (aeron_alloc(&new_data, new_size) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    size_t size = aeron_deque_size(deque);
    if (deque->first_element < deque->last_element)
    {
        size_t first_offset = deque->first_element * deque->element_size;
        uint8_t *first_ptr = deque->data + first_offset;
        size_t mem_size = size * deque->element_size;

        memcpy(new_data, first_ptr, mem_size);
    }
    else if (deque->first_element > deque->last_element)
    {
        size_t first_offset = deque->first_element * deque->element_size;
        uint8_t *first_ptr = deque->data + first_offset;
        size_t num_bytes_first_to_end_of_buffer = (deque->element_count - deque->first_element) * deque->element_size;
        size_t num_bytes_begin_of_buffer_to_last = deque->last_element * deque->element_size;
        uint8_t *dest_last_ptr = (uint8_t *)new_data + num_bytes_first_to_end_of_buffer;

        memcpy(new_data, (void *)first_ptr, num_bytes_first_to_end_of_buffer);
        memcpy((void *)dest_last_ptr, deque->data, num_bytes_begin_of_buffer_to_last);
    }

    void *old_data = (void *)deque->data;
    deque->data = new_data;
    deque->first_element = 0;
    deque->last_element = size;
    deque->element_count = new_element_count;

    aeron_free(old_data);

    return 0;
}

int aeron_deque_add_last(aeron_deque_t *deque, void *value)
{
    if (!aeron_deque_has_capacity(deque))
    {
        if (aeron_deque_reallocate(deque) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
    }

    size_t last_offset = deque->last_element * deque->element_size;
    uint8_t *last_ptr = deque->data + last_offset;
    memcpy(last_ptr, (uint8_t *)value, deque->element_size);
    ++deque->last_element;
    if (deque->last_element == deque->element_count)
    {
        deque->last_element = 0;
    }

    return 1;
}

int aeron_deque_remove_first(aeron_deque_t *deque, void *value)
{
    if (aeron_deque_size(deque) == 0)
    {
        return 0;
    }
    size_t first_offset = deque->first_element * deque->element_size;
    uint8_t *first_ptr = deque->data + first_offset;
    memcpy(value, (void *)first_ptr, deque->element_size);
    ++deque->first_element;
    if (deque->first_element == deque->element_count)
    {
        deque->first_element = 0;
    }

    return 1;
}

