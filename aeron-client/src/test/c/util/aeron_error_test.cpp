/*
 * Copyright 2014-2025 Real Logic Limited.
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

#include <array>
#include <atomic>
#include <thread>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

extern "C"
{
#include "util/aeron_error.h"
}

class ErrorTest : public testing::Test
{
public:
    ErrorTest()
    {
        aeron_err_clear();
    }
};

int functionA()
{
    AERON_SET_ERR(-EINVAL, "this is the root error: %d", 10);
    return -1;
}

int functionB()
{
    if (functionA() < 0)
    {
        AERON_APPEND_ERR("this is another error: %d", 20);
        return -1;
    }

    return 0;
}

int functionC()
{
    if (functionB() < 0)
    {
        AERON_APPEND_ERR("this got borked: %d", 30);
    }

    return 0;
}

static std::string::size_type assert_substring(
    const std::string &value, const std::string &token, const std::string::size_type index)
{
    auto new_index = value.find(token, index);
    EXPECT_NE(new_index, std::string::npos) << value;

    return new_index;
}

TEST_F(ErrorTest, shouldStackErrors)
{
    functionC();

    std::string err_msg = std::string(aeron_errmsg());

    auto index = assert_substring(err_msg, "(-22) unknown error code", 0);
    index = assert_substring(err_msg, "[functionA, aeron_error_test.cpp:", index);
    index = assert_substring(err_msg, "] this is the root error: 10", index);
    index = assert_substring(err_msg, "[functionB, aeron_error_test.cpp:", index);
    index = assert_substring(err_msg, "] this is another error: 20", index);
    index = assert_substring(err_msg, "[functionC, aeron_error_test.cpp:", index);
    index = assert_substring(err_msg, "] this got borked: 30", index);

    EXPECT_LT(index, err_msg.length());
}

TEST_F(ErrorTest, shouldHandleErrorsOverflow)
{
    AERON_SET_ERR(EINVAL, "%s", "this is the root error");

    for (int i = 0; i < 1000; i++)
    {
        AERON_APPEND_ERR("this is a nested error: %d", i);
    }

    std::string err_msg = std::string(aeron_errmsg());

    auto index = assert_substring(err_msg, "(22) Invalid argument", 0);
    index = assert_substring(err_msg, "[TestBody, aeron_error_test.cpp:", index);
    index = assert_substring(err_msg, "] this is the root error", index);
    index = assert_substring(err_msg, "[TestBody, aeron_error_test.cpp:", index);
    index = assert_substring(err_msg, "] this is a nested error: ", index);
    index = assert_substring(err_msg, "[TestBody, aeron_error_...", index);

    EXPECT_LT(index, err_msg.length());
}

TEST_F(ErrorTest, shouldReportZeroAsErrorForBackwardCompatibility)
{
    AERON_SET_ERR(0, "%s", "this is the root error");

    std::string err_msg = std::string(aeron_errmsg());

    auto index = assert_substring(err_msg, "(0) generic error, see message", 0);
    index = assert_substring(err_msg, "[TestBody, aeron_error_test.cpp:", index);
    index = assert_substring(err_msg, "] this is the root error", index);

    EXPECT_LT(index, err_msg.length());
}

TEST_F(ErrorTest, shouldAllowToAppendAfterClearing)
{
    AERON_APPEND_ERR("%s", "first error");
    aeron_err_clear();
    AERON_APPEND_ERR("%s", "second error");

    std::string err_msg = std::string(aeron_errmsg());

    EXPECT_THAT(err_msg, testing::Not(testing::HasSubstr("no error")));
    EXPECT_THAT(err_msg, testing::Not(testing::HasSubstr("first error")));
    EXPECT_THAT(err_msg, testing::HasSubstr("second error"));
}

#define CALLS_PER_THREAD (1000)
#define NUM_THREADS (2)
#define ITERATIONS (10)

static void test_concurrent_access()
{
    std::atomic<int> countDown(NUM_THREADS);
    std::vector<std::thread> threads;

    for (int i = 0; i < NUM_THREADS; i++)
    {
        threads.push_back(
            std::thread(
                [&]()
                {
                    const int thread_id = countDown.fetch_sub(1);
                    while (countDown > 0)
                    {
                        std::this_thread::yield();
                    }

                    const auto start("] [" + std::to_string(thread_id) + "] start");
                    const auto end("] [" + std::to_string(thread_id) + "] end:");
                    for (int m = 0; m < CALLS_PER_THREAD; m++)
                    {
                        AERON_SET_ERR(0, "[%d] %s", thread_id, "start");
                        AERON_APPEND_ERR("[%d] end: %d", thread_id, m);

                        std::string err_msg = std::string(aeron_errmsg());

                        auto index = assert_substring(err_msg, "(0) generic error, see message", 0);
                        index = assert_substring(err_msg, "[operator", index);
                        index = assert_substring(err_msg, start, index);
                        index = assert_substring(err_msg, "[operator", index);
                        index = assert_substring(err_msg, end, index);
                        EXPECT_LT(index, err_msg.length());

                        aeron_err_clear();
                    }
                }));
    }

    for (std::thread &t: threads)
    {
        if (t.joinable())
        {
            t.join();
        }
    }
}

TEST_F(ErrorTest, shouldAllowConcurrentAccess)
{
    for (int i = 0; i < ITERATIONS; i++)
    {
        test_concurrent_access();
    }
}
