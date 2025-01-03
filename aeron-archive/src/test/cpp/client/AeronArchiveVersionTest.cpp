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

#include "gtest/gtest.h"

#include "client/AeronArchiveVersion.h"

using namespace aeron::archive::client;

TEST(AeronArchiveVersionTest, shouldReturnVersionText)
{
    ASSERT_EQ(AERON_VERSION_TXT, AeronArchiveVersion::text());
}

TEST(AeronArchiveVersionTest, shouldReturnGitSha)
{
    ASSERT_EQ(AERON_VERSION_GITSHA, AeronArchiveVersion::gitSha());
}

TEST(AeronArchiveVersionTest, shouldReturnMajorVersion)
{
    ASSERT_EQ(AERON_VERSION_MAJOR, AeronArchiveVersion::major());
}

TEST(AeronArchiveVersionTest, shouldReturnMinorVersion)
{
    ASSERT_EQ(AERON_VERSION_MINOR, AeronArchiveVersion::minor());
}

TEST(AeronArchiveVersionTest, shouldReturnPatch)
{
    ASSERT_EQ(AERON_VERSION_PATCH, AeronArchiveVersion::patch());
}
