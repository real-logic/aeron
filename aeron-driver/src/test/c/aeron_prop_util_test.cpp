/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <functional>
#include <limits.h>

#include <gtest/gtest.h>

extern "C"
{
#include "util/aeron_prop_util.h"
}

class PropTest : public testing::Test
{
public:
    PropTest()
    {
    }

    virtual ~PropTest()
    {
    }
};

TEST_F(PropTest, shouldNotParseInvalidNumber)
{
    uint64_t value = 0;

    EXPECT_EQ(aeron_parse_size64(nullptr, &value), -1);
    EXPECT_EQ(aeron_parse_size64("", &value), -1);
    EXPECT_EQ(aeron_parse_size64("rubbish", &value), -1);
    EXPECT_EQ(aeron_parse_size64("-8", &value), -1);
    EXPECT_EQ(aeron_parse_size64("123Z", &value), -1);
    EXPECT_EQ(aeron_parse_size64("k", &value), -1);
}

TEST_F(PropTest, shouldParseValidNumber)
{
    uint64_t value = 0;

    EXPECT_EQ(aeron_parse_size64("0", &value), 0);
    EXPECT_EQ(value, (uint64_t)0);

    EXPECT_EQ(aeron_parse_size64("1", &value), 0);
    EXPECT_EQ(value, (uint64_t)1);

    EXPECT_EQ(aeron_parse_size64("77777777", &value), 0);
    EXPECT_EQ(value, (uint64_t)77777777);
}

TEST_F(PropTest, shouldParseValidQualifiedNumber)
{
    uint64_t value = 0;

    EXPECT_EQ(aeron_parse_size64("0k", &value), 0);
    EXPECT_EQ(value, (uint64_t)0);

    EXPECT_EQ(aeron_parse_size64("1k", &value), 0);
    EXPECT_EQ(value, (uint64_t)1024);

    EXPECT_EQ(aeron_parse_size64("64k", &value), 0);
    EXPECT_EQ(value, (uint64_t)64 * 1024);

    EXPECT_EQ(aeron_parse_size64("0m", &value), 0);
    EXPECT_EQ(value, (uint64_t)0);

    EXPECT_EQ(aeron_parse_size64("1m", &value), 0);
    EXPECT_EQ(value, (uint64_t)1024 * 1024);

    EXPECT_EQ(aeron_parse_size64("64m", &value), 0);
    EXPECT_EQ(value, (uint64_t)64 * 1024 * 1024);

    EXPECT_EQ(aeron_parse_size64("0g", &value), 0);
    EXPECT_EQ(value, (uint64_t)0);

    EXPECT_EQ(aeron_parse_size64("1g", &value), 0);
    EXPECT_EQ(value, (uint64_t)1024 * 1024 * 1024);

    EXPECT_EQ(aeron_parse_size64("64g", &value), 0);
    EXPECT_EQ(value, (uint64_t)64 * 1024 * 1024 * 1024);
}

TEST_F(PropTest, shouldNotParseInvalidDuration)
{
    uint64_t duration_ns = 0;

    EXPECT_EQ(aeron_parse_duration_ns(nullptr, &duration_ns), -1);
    EXPECT_EQ(aeron_parse_duration_ns("", &duration_ns), -1);
    EXPECT_EQ(aeron_parse_duration_ns("rubbish", &duration_ns), -1);
    EXPECT_EQ(aeron_parse_duration_ns("-8", &duration_ns), -1);
    EXPECT_EQ(aeron_parse_duration_ns("123ps", &duration_ns), -1);
    EXPECT_EQ(aeron_parse_duration_ns("s", &duration_ns), -1);
}

TEST_F(PropTest, shouldParseValidDuration)
{
    uint64_t duration_ns = 0;

    EXPECT_EQ(aeron_parse_duration_ns("0", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)0);

    EXPECT_EQ(aeron_parse_duration_ns("1", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)1);

    EXPECT_EQ(aeron_parse_duration_ns("77777777", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)77777777);
}

TEST_F(PropTest, shouldParseValidQualifiedDuration)
{
    uint64_t duration_ns = 0;

    EXPECT_EQ(aeron_parse_duration_ns("0ns", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)0);

    EXPECT_EQ(aeron_parse_duration_ns("1ns", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)1);

    EXPECT_EQ(aeron_parse_duration_ns("64ns", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)64);

    EXPECT_EQ(aeron_parse_duration_ns("0us", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)0);

    EXPECT_EQ(aeron_parse_duration_ns("1us", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)1000);

    EXPECT_EQ(aeron_parse_duration_ns("7us", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)7 * 1000);

    EXPECT_EQ(aeron_parse_duration_ns("7000us", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)7000 * 1000);

    EXPECT_EQ(aeron_parse_duration_ns("0ms", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)0);

    EXPECT_EQ(aeron_parse_duration_ns("1ms", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)1000 * 1000);

    EXPECT_EQ(aeron_parse_duration_ns("7ms", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)7000 * 1000);

    EXPECT_EQ(aeron_parse_duration_ns("7000ms", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)7000 * 1000 * 1000);

    EXPECT_EQ(aeron_parse_duration_ns("0s", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)0);

    EXPECT_EQ(aeron_parse_duration_ns("1s", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)1000 * 1000 * 1000);

    EXPECT_EQ(aeron_parse_duration_ns("7s", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)7000 * 1000 * 1000);

    EXPECT_EQ(aeron_parse_duration_ns("700s", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)700 * 1000 * 1000 * 1000);
}

TEST_F(PropTest, shouldParseMaxQualifiedDuration)
{
    uint64_t duration_ns = 0;

    EXPECT_EQ(aeron_parse_duration_ns("70000000000s", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)LLONG_MAX);
}