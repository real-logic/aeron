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
#include <aeron_driver_common.h>

extern "C"
{
#include "util/aeron_parse_util.h"
}

class ParseUtilTest : public testing::Test
{
public:
    ParseUtilTest()
    {
    }

    virtual ~ParseUtilTest()
    {
    }
};

TEST_F(ParseUtilTest, shouldNotParseInvalidNumber)
{
    uint64_t value = 0;

    EXPECT_EQ(aeron_parse_size64(nullptr, &value), -1);
    EXPECT_EQ(aeron_parse_size64("", &value), -1);
    EXPECT_EQ(aeron_parse_size64("rubbish", &value), -1);
    EXPECT_EQ(aeron_parse_size64("-8", &value), -1);
    EXPECT_EQ(aeron_parse_size64("123Z", &value), -1);
    EXPECT_EQ(aeron_parse_size64("k", &value), -1);
}

TEST_F(ParseUtilTest, shouldParseValidNumber)
{
    uint64_t value = 0;

    EXPECT_EQ(aeron_parse_size64("0", &value), 0);
    EXPECT_EQ(value, (uint64_t)0);

    EXPECT_EQ(aeron_parse_size64("1", &value), 0);
    EXPECT_EQ(value, (uint64_t)1);

    EXPECT_EQ(aeron_parse_size64("77777777", &value), 0);
    EXPECT_EQ(value, (uint64_t)77777777);
}

TEST_F(ParseUtilTest, shouldParseValidQualifiedNumber)
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

TEST_F(ParseUtilTest, shouldNotParseInvalidDuration)
{
    uint64_t duration_ns = 0;

    EXPECT_EQ(aeron_parse_duration_ns(nullptr, &duration_ns), -1);
    EXPECT_EQ(aeron_parse_duration_ns("", &duration_ns), -1);
    EXPECT_EQ(aeron_parse_duration_ns("rubbish", &duration_ns), -1);
    EXPECT_EQ(aeron_parse_duration_ns("-8", &duration_ns), -1);
    EXPECT_EQ(aeron_parse_duration_ns("123ps", &duration_ns), -1);
    EXPECT_EQ(aeron_parse_duration_ns("s", &duration_ns), -1);
}

TEST_F(ParseUtilTest, shouldParseValidDuration)
{
    uint64_t duration_ns = 0;

    EXPECT_EQ(aeron_parse_duration_ns("0", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)0);

    EXPECT_EQ(aeron_parse_duration_ns("1", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)1);

    EXPECT_EQ(aeron_parse_duration_ns("77777777", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)77777777);
}

TEST_F(ParseUtilTest, shouldParseValidQualifiedDuration)
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

TEST_F(ParseUtilTest, shouldParseMaxQualifiedDuration)
{
    uint64_t duration_ns = 0;

    EXPECT_EQ(aeron_parse_duration_ns("70000000000s", &duration_ns), 0);
    EXPECT_EQ(duration_ns, (uint64_t)LLONG_MAX);
}

TEST_F(ParseUtilTest, shouldSplitAddress)
{
    aeron_parsed_address_t split_address;

    EXPECT_EQ(aeron_address_split("localhost:1234", &split_address), 0);
    EXPECT_EQ(std::string(split_address.host), "localhost");
    EXPECT_EQ(std::string(split_address.port), "1234");
    EXPECT_EQ(split_address.ip_version_hint, 4);

    EXPECT_EQ(aeron_address_split("127.0.0.1:777", &split_address), 0);
    EXPECT_EQ(std::string(split_address.host), "127.0.0.1");
    EXPECT_EQ(std::string(split_address.port), "777");
    EXPECT_EQ(split_address.ip_version_hint, 4);

    EXPECT_EQ(aeron_address_split("localhost.local", &split_address), 0);
    EXPECT_EQ(std::string(split_address.host), "localhost.local");
    EXPECT_EQ(std::string(split_address.port), "");
    EXPECT_EQ(split_address.ip_version_hint, 4);

    EXPECT_EQ(aeron_address_split(":123", &split_address), 0);
    EXPECT_EQ(std::string(split_address.host), "");
    EXPECT_EQ(std::string(split_address.port), "123");
    EXPECT_EQ(split_address.ip_version_hint, 4);

    EXPECT_EQ(aeron_address_split("[FF01::FD]:40456", &split_address), 0);
    EXPECT_EQ(std::string(split_address.host), "FF01::FD");
    EXPECT_EQ(std::string(split_address.port), "40456");
    EXPECT_EQ(split_address.ip_version_hint, 6);
}

TEST_F(ParseUtilTest, shouldSplitInterface)
{
    aeron_parsed_interface_t split_interface;

    EXPECT_EQ(aeron_interface_split("localhost:1234/24", &split_interface), 0);
    EXPECT_EQ(std::string(split_interface.host), "localhost");
    EXPECT_EQ(std::string(split_interface.port), "1234");
    EXPECT_EQ(std::string(split_interface.prefix), "24");
    EXPECT_EQ(split_interface.ip_version_hint, 4);

    EXPECT_EQ(aeron_interface_split("127.0.0.1:777", &split_interface), 0);
    EXPECT_EQ(std::string(split_interface.host), "127.0.0.1");
    EXPECT_EQ(std::string(split_interface.port), "777");
    EXPECT_EQ(std::string(split_interface.prefix), "");
    EXPECT_EQ(split_interface.ip_version_hint, 4);

    EXPECT_EQ(aeron_interface_split("localhost.local", &split_interface), 0);
    EXPECT_EQ(std::string(split_interface.host), "localhost.local");
    EXPECT_EQ(std::string(split_interface.port), "");
    EXPECT_EQ(std::string(split_interface.prefix), "");
    EXPECT_EQ(split_interface.ip_version_hint, 4);

    EXPECT_EQ(aeron_interface_split(":123", &split_interface), 0);
    EXPECT_EQ(std::string(split_interface.host), "");
    EXPECT_EQ(std::string(split_interface.port), "123");
    EXPECT_EQ(std::string(split_interface.prefix), "");
    EXPECT_EQ(split_interface.ip_version_hint, 4);

    EXPECT_EQ(aeron_interface_split("[FF01::FD]:40456/8", &split_interface), 0);
    EXPECT_EQ(std::string(split_interface.host), "FF01::FD");
    EXPECT_EQ(std::string(split_interface.port), "40456");
    EXPECT_EQ(std::string(split_interface.prefix), "8");
    EXPECT_EQ(split_interface.ip_version_hint, 6);

    EXPECT_EQ(aeron_interface_split("[FF01::FD]:40456", &split_interface), 0);
    EXPECT_EQ(std::string(split_interface.host), "FF01::FD");
    EXPECT_EQ(std::string(split_interface.port), "40456");
    EXPECT_EQ(std::string(split_interface.prefix), "");
    EXPECT_EQ(split_interface.ip_version_hint, 6);

    EXPECT_EQ(aeron_interface_split("[FF01::FD]/128", &split_interface), 0);
    EXPECT_EQ(std::string(split_interface.host), "FF01::FD");
    EXPECT_EQ(std::string(split_interface.port), "");
    EXPECT_EQ(std::string(split_interface.prefix), "128");
    EXPECT_EQ(split_interface.ip_version_hint, 6);

    EXPECT_EQ(aeron_interface_split("[FF01::FD%eth0]", &split_interface), 0);
    EXPECT_EQ(std::string(split_interface.host), "FF01::FD");
    EXPECT_EQ(std::string(split_interface.port), "");
    EXPECT_EQ(std::string(split_interface.prefix), "");
    EXPECT_EQ(split_interface.ip_version_hint, 6);

    EXPECT_EQ(aeron_interface_split("[::1%eth0]:1234", &split_interface), 0);
    EXPECT_EQ(std::string(split_interface.host), "::1");
    EXPECT_EQ(std::string(split_interface.port), "1234");
    EXPECT_EQ(std::string(split_interface.prefix), "");
    EXPECT_EQ(split_interface.ip_version_hint, 6);
}