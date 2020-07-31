//
// Created by mike on 31/07/20.
//

#ifndef AERON_TESTUTIL_H
#define AERON_TESTUTIL_H

#define WAIT_FOR_NON_NULL(val, op)               \
auto val = op;                                   \
do                                               \
{                                                \
    std::int64_t t0 = aeron_epoch_clock();       \
    while (!val)                                 \
    {                                            \
       ASSERT_LT(aeron_epoch_clock() - t0, 5000) << "Failed waiting for: "  << #op; \
       std::this_thread::yield();                \
       val = op;                                 \
    }                                            \
}                                                \
while (0)                                        \

#define WAIT_FOR(op)                             \
do                                               \
{                                                \
    std::int64_t t0 = aeron_epoch_clock();       \
    while (!(op))                                \
    {                                            \
       ASSERT_LT(aeron_epoch_clock() - t0, 5000) << "Failed waiting for: " << #op; \
       std::this_thread::yield();                \
    }                                            \
}                                                \
while (0)                                        \

#endif //AERON_TESTUTIL_H
