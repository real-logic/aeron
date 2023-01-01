/*
 * Copyright 2014-2023 Real Logic Limited.
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
#include "util/aeron_error.h"
#include "util/aeron_netutil.h"
}

class NetutilTest : public testing::Test
{
public:
    NetutilTest() = default;
};

TEST_F(NetutilTest, shouldGetSocketBufferLengths)
{
    size_t default_so_rcvbuf = 0;
    size_t default_so_sndbuf = 0;

    ASSERT_GE(aeron_netutil_get_so_buf_lengths(
        &default_so_rcvbuf, &default_so_sndbuf), 0) << aeron_errmsg();

    EXPECT_NE(0U, default_so_rcvbuf);
    EXPECT_NE(0U, default_so_sndbuf);
}
