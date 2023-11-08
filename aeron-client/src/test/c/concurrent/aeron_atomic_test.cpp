/*
 * Copyright 2023 Adaptive Financial Consulting Limited.
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

#include <gtest/gtest.h>

extern "C"
{
#include "concurrent/aeron_atomic.h"
}

TEST(AtomicTest, testGetAndAddInt64)
{
    int64_t var = 0;
    int64_t res;

    AERON_GET_AND_ADD_INT64(res, var, 1);
    ASSERT_EQ(0, res);
    ASSERT_EQ(1, var);

    AERON_GET_AND_ADD_INT64(res, var, -1);
    ASSERT_EQ(1, res);
    ASSERT_EQ(0, var);

    AERON_GET_AND_ADD_INT64(res, var, INT64_MAX);
    ASSERT_EQ(0, res);
    ASSERT_EQ(INT64_MAX, var);

    AERON_GET_AND_ADD_INT64(res, var, -INT64_MAX);
    ASSERT_EQ(INT64_MAX, res);
    ASSERT_EQ(0, var);
}
