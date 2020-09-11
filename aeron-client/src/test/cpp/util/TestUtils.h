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

#ifndef AERON_TESTUTILS_H
#define AERON_TESTUTILS_H

#if !defined(_MSC_VER)
#include <unistd.h>
#include <cstdlib>
#else
#include <windows.h>
#endif

#include "util/Exceptions.h"
#include "ChannelUriStringBuilder.h"

namespace aeron { namespace test {

std::string makeTempFileName()
{
#if !defined(_MSC_VER)
    char rawname[] = "/tmp/aeron-c.XXXXXXX";
    int fd = ::mkstemp(rawname);
    ::close(fd);
    ::unlink(rawname);

    return std::string(rawname);

#else
    char tmpdir[MAX_PATH + 1];
    char tmpfile[MAX_PATH];

    if (::GetTempPath(MAX_PATH, &tmpdir[0]) > 0)
    {
        if (::GetTempFileName(tmpdir, TEXT("aeron-c"), 0, &tmpfile[0]) != 0)
        {
            return std::string(tmpfile);
        }
    }

    throw util::IllegalStateException("could not make unique temp filename", SOURCEINFO);
#endif
}

inline void throwIllegalArgumentException()
{
    throw util::IllegalArgumentException("Intentional IllegalArgumentException", SOURCEINFO);
}

ChannelUriStringBuilder &setParameters(const char *media, const char *endpoint, ChannelUriStringBuilder &builder)
{
    builder.media(media);
    if (endpoint)
    {
        builder.endpoint(endpoint);
    }

    return builder;
}

}}

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

#endif //AERON_TESTUTILS_H
