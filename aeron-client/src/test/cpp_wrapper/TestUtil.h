//
// Created by mike on 31/07/20.
//

#ifndef AERON_TESTUTIL_H
#define AERON_TESTUTIL_H

#include "ChannelUriStringBuilder.h"

using namespace aeron;

#define AERON_TEST_TIMEOUT (5000)

#define WAIT_FOR_NON_NULL(val, op)               \
auto val = op;                                   \
do                                               \
{                                                \
    std::int64_t t0 = aeron_epoch_clock();       \
    while (!val)                                 \
    {                                            \
       ASSERT_LT(aeron_epoch_clock() - t0, AERON_TEST_TIMEOUT) << "Failed waiting for: "  << #op; \
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
       ASSERT_LT(aeron_epoch_clock() - t0, AERON_TEST_TIMEOUT) << "Failed waiting for: " << #op; \
       std::this_thread::yield();                \
    }                                            \
}                                                \
while (0)                                        \

#define POLL_FOR_NON_NULL(val, op, invoker) \
auto val = op;                              \
do                                          \
{                                           \
    std::int64_t t0 = aeron_epoch_clock();  \
    while (!val)                            \
    {                                       \
       invoker.invoke();                    \
       ASSERT_LT(aeron_epoch_clock() - t0, AERON_TEST_TIMEOUT) << "Failed waiting for: "  << #op; \
       std::this_thread::yield();           \
       val = op;                            \
    }                                       \
}                                           \
while (0)                                   \

#define POLL_FOR(op, invoker)              \
do                                         \
{                                          \
    std::int64_t t0 = aeron_epoch_clock(); \
    while (!(op))                          \
    {                                      \
       invoker.invoke();                   \
       ASSERT_LT(aeron_epoch_clock() - t0, AERON_TEST_TIMEOUT) << "Failed waiting for: " << #op; \
       std::this_thread::yield();          \
    }                                      \
}                                          \
while (0)                                  \

ChannelUriStringBuilder &setParameters(const char *media, const char *endpoint, ChannelUriStringBuilder &builder)
{
    builder.media(media);
    if (endpoint)
    {
        builder.endpoint(endpoint);
    }
    return builder;
}

#endif //AERON_TESTUTIL_H
