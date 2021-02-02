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


TEST_F(ErrorTest, shouldStackErrors)
{
    functionC();

    printf("%s\n", aeron_errmsg());
    fflush(stdout);
}

TEST_F(ErrorTest, shouldHandleErrorsOverflow)
{
    AERON_SET_ERR(EINVAL, "%s", "this is the root error");

    for (int i = 0; i < 1000; i++)
    {
        AERON_APPEND_ERR("this is a nested error: %d", i);
    }

    printf("%s\n", aeron_errmsg());
    fflush(stdout);
}

TEST_F(ErrorTest, shouldReportZeroAsErrorForBackwardCompatibility)
{
    AERON_SET_ERR(0, "%s", "this is the root error");

    printf("%s\n", aeron_errmsg());
    fflush(stdout);
}