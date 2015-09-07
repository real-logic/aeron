/*
 * Copyright 2015 Real Logic Ltd.
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
#include <gtest/gtest.h>

#include "uri/AeronUri.h"
#include "util/Exceptions.h"

using namespace aeron::driver::uri;

class AeronUriTest : public testing::Test
{
};

TEST_F(AeronUriTest, createUri)
{
    std::string scheme = "aeron";
    std::string media = "media";
    std::string key1 = "key1";
    std::string value1 = "value1";
    std::unordered_map<std::string, std::string> params;

    params[key1] = value1;

    AeronUri uri{media, params};

    EXPECT_EQ(scheme, uri.scheme());
    EXPECT_EQ(media, uri.media());
    EXPECT_EQ(value1, uri.param(key1));
    EXPECT_EQ(true, uri.hasParam(key1));
    EXPECT_EQ(false, uri.hasParam(media));
}

static void parseWithMedia(const char* uri, const char* media)
{
    std::string scheme = "aeron";
    std::string uriStr{uri, strlen(uri)};
    std::string mediaStr{media, strlen(media)};

    auto aeronUri = AeronUri::parse(uriStr);

    EXPECT_EQ(scheme, aeronUri->scheme());
    EXPECT_EQ(mediaStr, aeronUri->media());

    delete(aeronUri);
}

TEST_F(AeronUriTest, parseSimpleDefaultUri)
{
    parseWithMedia("aeron:udp", "udp");
    parseWithMedia("aeron:ipc", "ipc");
    parseWithMedia("aeron:", "");
}

TEST_F(AeronUriTest, parseWithUriParams)
{
    std::string uri = "aeron:udp?foo=bar|fool=barl";
    std::string key1 = "foo";
    std::string key2 = "fool";
    auto aeronUri = AeronUri::parse(uri);

    EXPECT_EQ(true, aeronUri->hasParam(key1));
    EXPECT_EQ(true, aeronUri->hasParam(key2));
}

TEST_F(AeronUriTest, failWithInvalidUri)
{
    std::string uri = "aero:udp?foo=bar|fool=barl";
    EXPECT_THROW(AeronUri::parse(uri), aeron::util::ParseException);
}