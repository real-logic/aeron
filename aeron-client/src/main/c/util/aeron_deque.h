//
// Created by mike on 23/01/23.
//

#ifndef AERON_DEQUE_H
#define AERON_DEQUE_H

#include <stdint.h>
#include <string.h>

struct aeron_deque_stct
{
    uint8_t *data;
    size_t element_count;
    size_t element_size;
    size_t first_element;
    size_t last_element;
};
typedef struct aeron_deque_stct aeron_deque_t;

int aeron_deque_init(aeron_deque_t *deque, size_t initial_element_count, size_t element_size);

void aeron_deque_close(aeron_deque_t *deque);

/**
 * Add value into the deque as the last element.  Will memcpy into the deque from the void pointer provided using
 * the specified element size.  May need to allocate in order to increase the size of the dequeue.
 *
 * Errors:
 *   ENOMEM if growing the array fails.
 *
 * @param deque to add the value too.
 * @param value value to be added.
 * @return 0 on success, -1 on failure.
 */
int aeron_deque_add_last(aeron_deque_t *deque, void *value);

int aeron_deque_remove_first(aeron_deque_t *deque, void *value);

#endif //AERON_DEQUE_H
