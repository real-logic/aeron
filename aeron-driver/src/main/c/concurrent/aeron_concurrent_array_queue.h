//
// Created by Todd Montgomery on 6/6/17.
//

#ifndef AERON_CONCURRENT_ARRAY_QUEUE_H
#define AERON_CONCURRENT_ARRAY_QUEUE_H

typedef enum aeron_queue_offer_result_stct
{
    AERON_OFFER_SUCCESS = 0,
    AERON_OFFER_ERROR = -2,
    AERON_OFFER_FULL = -1
}
aeron_queue_offer_result_t;

typedef void (*aeron_queue_drain_func_t)(void *clientd, volatile void *item);

#endif //AERON_CONCURRENT_ARRAY_QUEUE_H
