/*
 * Copyright 2014-2024 Real Logic Limited.
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

#ifdef _MSC_VER
#define AERON_FILE_SEP '\\'
#else
#define AERON_FILE_SEP '/'
#endif

extern "C"
{
#include "client/aeron_archive.h"
#include "aeron_agent.h"
}

#include "../TestArchive.h"

class AeronArchiveTestBase
{
public:
    ~AeronArchiveTestBase()
    {
        if (m_debug)
        {
            std::cout << m_stream.str();
        }
    }

    void DoSetUp(std::int64_t archiveId = 42)
    {
        char aeron_dir[100];
        aeron_default_path(aeron_dir, 100);

        std::string sourceArchiveDir = m_archiveDir + AERON_FILE_SEP + "source";
        m_archive = std::make_shared<TestArchive>(
            aeron_dir,
            sourceArchiveDir,
            std::cout,
            "aeron:udp?endpoint=localhost:8010",
            "aeron:udp?endpoint=localhost:0",
            archiveId);

        //setCredentials(m_context);
    }

    void DoTearDown()
    {
    }

protected:

    const std::string m_archiveDir = ARCHIVE_DIR;

    std::ostringstream m_stream;

    std::shared_ptr<TestArchive> m_destArchive;
    std::shared_ptr<TestArchive> m_archive;

    bool m_debug = true;
};

class AeronArchiveTest : public AeronArchiveTestBase, public testing::Test
{
public:
    void SetUp() final
    {
        DoSetUp();
    }

    void TearDown() final
    {
        DoTearDown();
    }
};

TEST_F(AeronArchiveTest, shouldAsyncConnectToArchive)
{
    aeron_archive_context_t *ctx;
    aeron_archive_async_connect_t *async;
    aeron_archive_t *archive = nullptr;

    const uint64_t idle_duration_ns = UINT64_C(1000) * UINT64_C(1000); /* 1ms */

    ASSERT_EQ(0, aeron_archive_context_init(&ctx));
    ASSERT_EQ(0, aeron_archive_async_connect(&async, ctx));
    ASSERT_EQ(0, aeron_archive_async_connect_poll(&archive, async));

    while (nullptr == archive)
    {
        aeron_idle_strategy_sleeping_idle((void *)&idle_duration_ns, 0);

        ASSERT_NE(-1, aeron_archive_async_connect_poll(&archive, async));
    }

    aeron_archive_control_response_poller_t *control_response_poller = aeron_archive_get_control_response_poller(archive);
    aeron_subscription_t *subscription = aeron_archive_control_response_poller_get_subscription(control_response_poller);
    ASSERT_TRUE(aeron_subscription_is_connected(subscription));

    ASSERT_EQ(42, aeron_archive_get_archive_id(archive));
}
