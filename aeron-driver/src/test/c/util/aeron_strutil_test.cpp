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

#include <gtest/gtest.h>

extern "C"
{
#include "aeron_alloc.h"
#include "util/aeron_strutil.h"
}
#undef max

class StrUtilTest : public testing::Test
{
public:
    StrUtilTest() = default;
};

TEST_F(StrUtilTest, shouldHandleSingleValue)
{
    const int max_tokens = 10;
    char *tokens[max_tokens];
    char input[] = "single_token";

    int num_tokens = aeron_tokenise(input, ',', max_tokens, tokens);

    EXPECT_EQ(num_tokens, 1);
    EXPECT_STREQ(tokens[0], "single_token");
}

TEST_F(StrUtilTest, shouldHandleMultipleValues)
{
    const int max_tokens = 10;
    char *tokens[max_tokens];
    char input[] = "token_a,token_b,token_c";

    int num_tokens = aeron_tokenise(input, ',', max_tokens, tokens);

    EXPECT_EQ(num_tokens, 3);
    EXPECT_STREQ(tokens[0], "token_c");
    EXPECT_STREQ(tokens[1], "token_b");
    EXPECT_STREQ(tokens[2], "token_a");
}

TEST_F(StrUtilTest, shouldHandleMoreThanSpecifiedTokens)
{
    const int max_tokens = 2;
    char *tokens[max_tokens];
    char input[] = "token_a,token_b,token_c";

    int num_tokens = aeron_tokenise(input, ',', max_tokens, tokens);

    EXPECT_EQ(num_tokens, -ERANGE);
    EXPECT_STREQ(tokens[0], "token_c");
    EXPECT_STREQ(tokens[1], "token_b");
}

TEST_F(StrUtilTest, shouldHandleConsecutiveDelimeters)
{
    const int max_tokens = 10;
    char *tokens[max_tokens];
    char input[] = ",,token_a,,,,token_b,,token_c";

    int num_tokens = aeron_tokenise(input, ',', max_tokens, tokens);

    EXPECT_EQ(num_tokens, 3);
    EXPECT_STREQ(tokens[0], "token_c");
    EXPECT_STREQ(tokens[1], "token_b");
    EXPECT_STREQ(tokens[2], "token_a");
}

TEST_F(StrUtilTest, shouldHandleMaxRangeWithConsecutiveDelimeters)
{
    const int max_tokens = 3;
    char *tokens[max_tokens];
    char input[] = ",,token_a,,,,token_b,,token_c";

    int num_tokens = aeron_tokenise(input, ',', max_tokens, tokens);

    EXPECT_EQ(num_tokens, 3);
    EXPECT_STREQ(tokens[0], "token_c");
    EXPECT_STREQ(tokens[1], "token_b");
    EXPECT_STREQ(tokens[2], "token_a");
}

TEST_F(StrUtilTest, shouldHandleTrailingTokens)
{
    const int max_tokens = 6;
    char *tokens[max_tokens];
    char input[] = "token_a,token_b,token_c,,,,";

    int num_tokens = aeron_tokenise(input, ',', max_tokens, tokens);

    EXPECT_EQ(num_tokens, 3);
    EXPECT_STREQ(tokens[0], "token_c");
    EXPECT_STREQ(tokens[1], "token_b");
    EXPECT_STREQ(tokens[2], "token_a");
}

TEST_F(StrUtilTest, shouldHandleNull)
{
    const int max_tokens = 3;
    char *tokens[max_tokens];

    int num_tokens = aeron_tokenise(nullptr, ',', max_tokens, tokens);

    EXPECT_EQ(num_tokens, -EINVAL);
}

TEST_F(StrUtilTest, nullStringsAreEqual)
{
    EXPECT_TRUE(aeron_strn_equals(nullptr, nullptr, 10));
}

TEST_F(StrUtilTest, emptyStringsAreEqual)
{
    char str1[] = "";
    char str2[] = "";

    EXPECT_TRUE(aeron_strn_equals(str1, str2, 3));
}

TEST_F(StrUtilTest, stringIsEqualToItself)
{
    const char *str1 = "test";

    EXPECT_TRUE(aeron_strn_equals(str1, str1, 40));
}

TEST_F(StrUtilTest, nullTerminatedStringsAreEqual)
{
    std::string input = "This is the string to match";
    const char *str1 = input.c_str();
    char *str2 = new char[input.length() + 1];
    strcpy(str2, input.c_str());
    
    ASSERT_EQ('\0', str1[input.length()]);
    ASSERT_EQ('\0', str2[input.length()]);

    EXPECT_TRUE(aeron_strn_equals(str1, str2, 100));
    EXPECT_TRUE(aeron_strn_equals(str2, str1, 10000));

    aeron_free(str2);
}

TEST_F(StrUtilTest, nonNullTerminatedStringsAreNotEqual)
{
    std::string input = "There will be no match here!";
    const char *str1 = input.c_str();
    char *str2 = new char[input.length() + 1];
    strncpy(str2, input.c_str(), input.length());
    str2[input.length()] = -1;

    EXPECT_FALSE(aeron_strn_equals(str1, str2, 100));
    EXPECT_FALSE(aeron_strn_equals(str2, str1, 222));

    aeron_free(str2);
}

TEST_F(StrUtilTest, prefixMatchBasedOnTheGivenLength)
{
    const char *str1 = "abc";
    const char *str2 = "abcDEF";

    EXPECT_TRUE(aeron_strn_equals(str1, str2, 3));
    EXPECT_TRUE(aeron_strn_equals(str2, str1, 3));

    EXPECT_FALSE(aeron_strn_equals(str1, str2, 4));
    EXPECT_FALSE(aeron_strn_equals(str2, str1, 4));
}

TEST_F(StrUtilTest, prefixMatchNonNullTerminatedStrings)
{
    const char str1[] = {'h', 'e', 'l', 'l', 'o', -1};
    const char str2[] = {'h', 'e', 'l', 'l', 'o', ',', 'w', 'o', 'r', 'l', 'd', -1};

    EXPECT_TRUE(aeron_strn_equals(str1, str2, 5));
    EXPECT_TRUE(aeron_strn_equals(str2, str1, 5));

    EXPECT_FALSE(aeron_strn_equals(str1, str2, 6));
    EXPECT_FALSE(aeron_strn_equals(str2, str1, 6));
}
