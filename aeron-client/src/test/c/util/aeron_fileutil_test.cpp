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

#include <exception>
#include <functional>

#include <gtest/gtest.h>

extern "C"
{
#include "util/aeron_fileutil.h"
#include "util/aeron_error.h"
}

class FileUtilTest : public testing::Test {
public:
    FileUtilTest() = default;
};

TEST_F(FileUtilTest, rawLogCloseShouldUnmapAndDeleteLogFile)
{
    aeron_mapped_raw_log_t mapped_raw_log = {};
    const char *file = "test_close_unused_file.log";
    const size_t file_length = 16384;
    ASSERT_EQ(0, aeron_raw_log_map(&mapped_raw_log, file, true, 4096, 4096));

    EXPECT_NE(nullptr, mapped_raw_log.mapped_file.addr);
    EXPECT_EQ(file_length, mapped_raw_log.mapped_file.length);
    EXPECT_EQ((int64_t)file_length, aeron_file_length(file));

    ASSERT_EQ(0, aeron_raw_log_close(&mapped_raw_log, file));

    EXPECT_EQ(nullptr, mapped_raw_log.mapped_file.addr);
    EXPECT_EQ((size_t)0, mapped_raw_log.mapped_file.length);
    EXPECT_EQ((int64_t)-1, aeron_file_length(file));
}

TEST_F(FileUtilTest, rawLogFreeShouldUnmapAndDeleteLogFile)
{
    aeron_mapped_raw_log_t mapped_raw_log = {};
    const char *file = "test_free_unused_file.log";
    const size_t file_length = 16384;
    ASSERT_EQ(0, aeron_raw_log_map(&mapped_raw_log, file, true, 4096, 4096));

    EXPECT_NE(nullptr, mapped_raw_log.mapped_file.addr);
    EXPECT_EQ(file_length, mapped_raw_log.mapped_file.length);
    EXPECT_EQ((int64_t)file_length, aeron_file_length(file));

    ASSERT_EQ(true, aeron_raw_log_free(&mapped_raw_log, file));

    EXPECT_EQ(nullptr, mapped_raw_log.mapped_file.addr);
    EXPECT_EQ((size_t)0, mapped_raw_log.mapped_file.length);
    EXPECT_EQ((int64_t)-1, aeron_file_length(file));
}

TEST_F(FileUtilTest, rawLogCloseShouldNotDeleteFileIfUnmapFails)
{
    aeron_mapped_raw_log_t mapped_raw_log = {};
    const char *file = "test_close_unmap_fails.log";
    const size_t file_length = 16384;
    ASSERT_EQ(0, aeron_raw_log_map(&mapped_raw_log, file, true, 4096, 4096));
    const auto mapped_addr = mapped_raw_log.mapped_file.addr;
    mapped_raw_log.mapped_file.addr = reinterpret_cast<void *>(-1);

    ASSERT_EQ(-1, aeron_raw_log_close(&mapped_raw_log, file));
    EXPECT_EQ((int64_t)file_length, aeron_file_length(file));

    mapped_raw_log.mapped_file.addr = mapped_addr;
    ASSERT_EQ(0, aeron_raw_log_close(&mapped_raw_log, file));
    EXPECT_EQ((int64_t)-1, aeron_file_length(file));
}

TEST_F(FileUtilTest, rawLogFreeShouldNotDeleteFileIfUnmapFails)
{
    aeron_mapped_raw_log_t mapped_raw_log = {};
    const char *file = "test_free_unmap_fails.log";
    const size_t file_length = 16384;
    ASSERT_EQ(0, aeron_raw_log_map(&mapped_raw_log, file, true, 4096, 4096));
    const auto mapped_addr = mapped_raw_log.mapped_file.addr;
    mapped_raw_log.mapped_file.addr = reinterpret_cast<void *>(-1);

    ASSERT_EQ(false, aeron_raw_log_free(&mapped_raw_log, file));
    EXPECT_EQ((int64_t)file_length, aeron_file_length(file));

    mapped_raw_log.mapped_file.addr = mapped_addr;
    ASSERT_EQ(true, aeron_raw_log_free(&mapped_raw_log, file));
    EXPECT_EQ((int64_t)-1, aeron_file_length(file));
}
