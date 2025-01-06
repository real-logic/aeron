/*
 * Copyright 2014-2025 Real Logic Limited.
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
#include <random>

extern "C"
{
#include "util/aeron_error.h"
#include "util/aeron_http_util.h"
int aeron_http_parse_response(aeron_http_response_t *response);
}

class HttpUtilTest : public testing::Test
{
public:
    HttpUtilTest() = default;
};

TEST_F(HttpUtilTest, shouldParseHttpContent)
{
    const char *content =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/plain\r\n"
        "Accept-Ranges: none\r\n"
        "Last-Modified: Thu, 05 May 2022 18:43:25 GMT\r\n"
        "Content-Length: 10\r\n"
        "Date: Thu, 05 May 2022 18:54:35 GMT\r\n"
        "Server: EC2ws\r\n"
        "Connection: close\r\n"
        "\r\n"
        "10.2.75.14";

    size_t total_length = strlen(content);

    for (size_t offset = 1; offset < total_length; offset++)
    {
        char buffer[1024] = {};

        aeron_http_response_t response;
        response.buffer = buffer;
        response.headers_offset = 0;
        response.cursor = 0;
        response.body_offset = 0;
        response.length = 0;
        response.capacity = 0;
        response.status_code = 0;
        response.content_length = 0;
        response.parse_err = false;
        response.is_complete = false;

        memcpy(buffer, content, offset);
        response.length = offset;

        ASSERT_EQ(0, aeron_http_parse_response(&response));
        ASSERT_FALSE(response.is_complete);

        memcpy(buffer + offset, content + offset, total_length - offset);
        response.length = total_length;

        ASSERT_EQ(0, aeron_http_parse_response(&response));
        ASSERT_TRUE(response.is_complete);

        ASSERT_EQ(200, response.status_code);
        ASSERT_EQ(203, response.body_offset);
        ASSERT_EQ(10, response.content_length);
        ASSERT_EQ(false, response.parse_err);
        ASSERT_STREQ("10.2.75.14", response.buffer + response.body_offset) << offset;
    }
}

TEST_F(HttpUtilTest, shouldParseHttpContentWithoutContentLength)
{
    const char *content =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/plain\r\n"
        "Accept-Ranges: none\r\n"
        "Last-Modified: Thu, 05 May 2022 18:43:25 GMT\r\n"
        "Date: Thu, 05 May 2022 18:54:35 GMT\r\n"
        "Server: EC2ws\r\n"
        "Connection: close\r\n"
        "\r\n"
        "10.2.75.14";

    size_t total_length = strlen(content);

    for (size_t offset = 1; offset < total_length; offset++)
    {
        char buffer[1024] = {};

        aeron_http_response_t response;
        response.buffer = buffer;
        response.headers_offset = 0;
        response.cursor = 0;
        response.body_offset = 0;
        response.length = 0;
        response.capacity = 0;
        response.status_code = 0;
        response.content_length = 0;
        response.parse_err = false;
        response.is_complete = false;

        memcpy(buffer, content, offset);
        response.length = offset;

        ASSERT_EQ(0, aeron_http_parse_response(&response));
        ASSERT_FALSE(response.is_complete);

        memcpy(buffer + offset, content + offset, total_length - offset);
        response.length = total_length;

        ASSERT_EQ(0, aeron_http_parse_response(&response));
        ASSERT_FALSE(response.is_complete);

        ASSERT_EQ(200, response.status_code);
        ASSERT_EQ(183, response.body_offset);
        ASSERT_EQ(0, response.content_length);
        ASSERT_EQ(false, response.parse_err);
        ASSERT_STREQ("10.2.75.14", response.buffer + response.body_offset) << offset;
    }
}

TEST_F(HttpUtilTest, shouldErrorWithInvalidStatusCode)
{
    const char *content =
        "HTTP/1.1 AAA OK\r\n"
        "Content-Type: text/plain\r\n"
        "Accept-Ranges: none\r\n"
        "Last-Modified: Thu, 05 May 2022 18:43:25 GMT\r\n"
        "Content-Length: 10\r\n"
        "Date: Thu, 05 May 2022 18:54:35 GMT\r\n"
        "Server: EC2ws\r\n"
        "Connection: close\r\n"
        "\r\n"
        "10.2.75.14";

    aeron_http_response_t response = {};
    response.buffer = const_cast<char *>(content);
    response.length = strlen(content);

    ASSERT_EQ(-1, aeron_http_parse_response(&response)) << aeron_errmsg();
    ASSERT_FALSE(response.is_complete);
    ASSERT_TRUE(response.parse_err);
}

TEST_F(HttpUtilTest, shouldErrorWithZeroStatusCode)
{
    const char *content =
        "HTTP/1.1 000 OK\r\n"
        "Content-Type: text/plain\r\n"
        "Accept-Ranges: none\r\n"
        "Last-Modified: Thu, 05 May 2022 18:43:25 GMT\r\n"
        "Content-Length: 10\r\n"
        "Date: Thu, 05 May 2022 18:54:35 GMT\r\n"
        "Server: EC2ws\r\n"
        "Connection: close\r\n"
        "\r\n"
        "10.2.75.14";

    aeron_http_response_t response = {};
    response.buffer = const_cast<char *>(content);
    response.length = strlen(content);

    ASSERT_EQ(-1, aeron_http_parse_response(&response)) << aeron_errmsg();
    ASSERT_FALSE(response.is_complete);
    ASSERT_TRUE(response.parse_err);
}
