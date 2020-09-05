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

TEST_F(ErrorTest, shouldStackErrors)
{
    AERON_ERR(-1, "this is first error: %d", 10);
    AERON_ERR(-2, "this is second error: %d", 20);
    AERON_ERR(-3, "this is third error: %d", 30);
    AERON_ERR(-4, "This is another error: %d, %s", 30, "foo");

    printf("%s\n", aeron_errmsg());
    fflush(stdout);
}