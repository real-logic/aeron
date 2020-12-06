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
