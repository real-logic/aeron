/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

#include <cstdint>

#include <util/ScopeUtils.h>
#include <util/StringUtil.h>
#include <util/BitUtil.h>

#include <gtest/gtest.h>

using namespace aeron::util;

TEST(utilTests, scopeTest)
{
    bool flag = false;

    if (1)
    {
        OnScopeExit onExit ([&]()
        {
           flag = true;
        });

        ASSERT_EQ(flag, false);
    }
    ASSERT_EQ(flag, true);
}

TEST(utilTests, stringUtilTrimTest)
{
    std::string test = "  test  ";

    ASSERT_EQ(trimWSLeft(test), "test  ");
    ASSERT_EQ(trimWSRight(test), "  test");
    ASSERT_EQ(trimWSBoth(test), "test");
}

TEST(utilTests, stringUtilParseTest)
{
    ASSERT_NO_THROW({
        ASSERT_EQ(parse<int> ("100"), 100);
        ASSERT_EQ(parse<double> ("100.25"), 100.25);
        ASSERT_EQ(parse<std::uint64_t> ("0x123456789abcdef0"), 0x123456789abcdef0UL);
    });

    ASSERT_THROW(parse<int>(""), ParseException);
    ASSERT_THROW(parse<int>("  "), ParseException);
    ASSERT_THROW(parse<int>("xxx"), ParseException);
    ASSERT_THROW(parse<int>("84473.3443"), ParseException);
}

TEST(utilTests, stringUtiltoStringTest)
{
    ASSERT_EQ(toString(100), "100");
    ASSERT_EQ(toString(1.25), "1.25");
    ASSERT_EQ(toString("hello"), "hello");
}

TEST(utilTests, stringUtilstrPrintfTest)
{
    std::string val = strPrintf("%s %s", "hello", "world");
    ASSERT_EQ(val, "hello world");

}

TEST(utilTests, findNextPowerOfTwo)
{
    EXPECT_EQ(BitUtil::findNextPowerOfTwo<std::uint32_t>(33), 64u);
    EXPECT_EQ(BitUtil::findNextPowerOfTwo<std::uint32_t>(4096), 4096u);
}
