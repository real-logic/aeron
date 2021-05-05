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

#include <array>
#include <cstdint>

#include <gtest/gtest.h>

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
    const std::string &value, const std::string token, const std::string::size_type index)
{
    auto new_index = value.find(token, index);
    EXPECT_NE(new_index, std::string::npos);

    return new_index;
}

TEST_F(ErrorTest, shouldStackErrors)
{
    functionC();

    std::string err_msg = std::string(aeron_errmsg());

    auto index = assert_substring(err_msg, "(-22) unknown error code", 0);
    index = assert_substring(err_msg, "[functionA,", index);
    index = assert_substring(err_msg, "aeron_error_test.cpp:", index);
    index = assert_substring(err_msg, "] this is the root error: 10", index);
    index = assert_substring(err_msg, "[functionB,", index);
    index = assert_substring(err_msg, "aeron_error_test.cpp:", index);
    index = assert_substring(err_msg, "] this is another error: 20", index);
    index = assert_substring(err_msg, "[functionC", index);
    index = assert_substring(err_msg, "aeron_error_test.cpp:", index);
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
    index = assert_substring(err_msg, "this is the root error", index);
    index = assert_substring(err_msg, "this is a nested error:", index);
    index = assert_substring(err_msg, "...", index);

    EXPECT_LT(index, err_msg.length());
}

TEST_F(ErrorTest, shouldReportZeroAsErrorForBackwardCompatibility)
{
    AERON_SET_ERR(0, "%s", "this is the root error");

    std::string err_msg = std::string(aeron_errmsg());

    auto index = assert_substring(err_msg, "(0) generic error, see message", 0);
    index = assert_substring(err_msg, "[TestBody,", index);
    index = assert_substring(err_msg, "aeron_error_test.cpp:", index);
    index = assert_substring(err_msg, "] this is the root error", index);

    EXPECT_LT(index, err_msg.length());
}
