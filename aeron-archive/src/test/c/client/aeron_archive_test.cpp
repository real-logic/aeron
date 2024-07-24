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

void fragment_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_stct *header)
{
    fprintf(stderr, "got a message!!\n");

    auto *received_p = (size_t *)clientd;
    (*received_p)++;
}

class AeronCArchiveTestBase
{
public:
    ~AeronCArchiveTestBase()
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

    static int32_t getRecordingCounterId(int32_t session_id, aeron_counters_reader_t *counters_reader)
    {
        int32_t counter_id;

        while (AERON_NULL_COUNTER_ID ==
            (counter_id = aeron_archive_recording_pos_find_counter_id_by_session_id(counters_reader, session_id)))
        {
            std::this_thread::yield();
        }

        return counter_id;
    }

    static void offerMessages(
        aeron_publication_t *publication,
        size_t message_count,
        const char *message_prefix,
        size_t start_count)
    {
        for (size_t i = 0; i < message_count; i++)
        {
            size_t index = i + start_count;
            char message[1000];
            size_t len = snprintf(message, 1000, "%s%lu", message_prefix, index);

            while (aeron_publication_offer(publication, (uint8_t *)message, len, nullptr, nullptr) < 0)
            {
                aeron_idle_strategy_yielding_idle(nullptr, 0);
            }
        }
    }

    static void consumeMessages(
        aeron_subscription_t *subscription,
        size_t message_count,
        const char *message_prefix)
    {
        size_t received = 0;

        while (received < message_count)
        {
            if (0 == aeron_subscription_poll(subscription, fragment_handler, (void *)&received, 10))
            {
                aeron_idle_strategy_yielding_idle(nullptr, 0);
            }
        }

        ASSERT_EQ(received, message_count);
    }

protected:
    const std::string m_archiveDir = ARCHIVE_DIR;

    const std::string m_recordingChannel = "aeron:udp?endpoint=localhost:3333";
    const std::int32_t m_recordingStreamId = 33;

    std::ostringstream m_stream;

    std::shared_ptr<TestArchive> m_destArchive;
    std::shared_ptr<TestArchive> m_archive;

    bool m_debug = true;
};

class AeronCArchiveTest : public AeronCArchiveTestBase, public testing::Test
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

typedef struct recording_descriptor_consumer_clientd_stct
{
    int64_t recording_id;
    int32_t stream_id;
}
recording_descriptor_consumer_clientd_t;

static void recording_descriptor_consumer(
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t start_timestamp,
    int64_t stop_timestamp,
    int64_t start_position,
    int64_t stop_position,
    int32_t initial_term_id,
    int32_t segment_file_length,
    int32_t term_buffer_length,
    int32_t mtu_length,
    int32_t session_id,
    int32_t stream_id,
    const char *stripped_channel,
    const char *original_channel,
    const char *source_identity,
    void *clientd)
{
    auto *cd = (recording_descriptor_consumer_clientd_t *)clientd;

    EXPECT_EQ(cd->recording_id, recording_id);
    EXPECT_EQ(cd->stream_id, stream_id);

    fprintf(stderr, "GOT THE LIST RECORDING CALLBACK\n");
}

TEST_F(AeronCArchiveTest, shouldAsyncConnectToArchive)
{
    aeron_archive_context_t *ctx;
    aeron_archive_async_connect_t *async;
    aeron_archive_t *archive = nullptr;

    const uint64_t idle_duration_ns = UINT64_C(1000) * UINT64_C(1000); /* 1ms */

    ASSERT_EQ(0, aeron_archive_context_init(&ctx));
    ASSERT_EQ(0, aeron_archive_context_set_idle_strategy(ctx, aeron_idle_strategy_sleeping_idle, (void *)&idle_duration_ns));
    ASSERT_EQ(0, aeron_archive_async_connect(&async, ctx));
    ASSERT_EQ(0, aeron_archive_async_connect_poll(&archive, async));

    while (nullptr == archive)
    {
        aeron_idle_strategy_sleeping_idle((void *)&idle_duration_ns, 0);

        ASSERT_NE(-1, aeron_archive_async_connect_poll(&archive, async));
    }

    aeron_subscription_t *subscription = aeron_archive_get_control_response_subscription(archive);
    ASSERT_TRUE(aeron_subscription_is_connected(subscription));

    ASSERT_EQ(42, aeron_archive_get_archive_id(archive));

    ASSERT_EQ(0, aeron_archive_close(archive));
    ASSERT_EQ(0, aeron_archive_context_close(ctx));
}

TEST_F(AeronCArchiveTest, shouldConnectToArchive)
{
    aeron_archive_context_t *ctx;
    aeron_archive_t *archive = nullptr;

    const uint64_t idle_duration_ns = UINT64_C(1000) * UINT64_C(1000); /* 1ms */

    ASSERT_EQ(0, aeron_archive_context_init(&ctx));
    ASSERT_EQ(0, aeron_archive_context_set_idle_strategy(ctx, aeron_idle_strategy_sleeping_idle, (void *)&idle_duration_ns));
    ASSERT_EQ(0, aeron_archive_connect(&archive, ctx));

    aeron_subscription_t *subscription = aeron_archive_get_control_response_subscription(archive);
    ASSERT_TRUE(aeron_subscription_is_connected(subscription));

    ASSERT_EQ(42, aeron_archive_get_archive_id(archive));

    ASSERT_EQ(0, aeron_archive_close(archive));
    ASSERT_EQ(0, aeron_archive_context_close(ctx));
}

TEST_F(AeronCArchiveTest, shouldRecordPublicationAndFindRecording)
{
    size_t message_count = 10;
    const char *message_prefix = "Message ";
    aeron_archive_context_t *ctx;
    aeron_archive_t *archive = nullptr;
    int32_t session_id;
    int64_t recording_id_from_counter;
    int64_t stop_position;

    const uint64_t idle_duration_ns = UINT64_C(1000) * UINT64_C(1000); /* 1ms */

    ASSERT_EQ(0, aeron_archive_context_init(&ctx));
    ASSERT_EQ(0, aeron_archive_context_set_idle_strategy(ctx, aeron_idle_strategy_sleeping_idle, (void *)&idle_duration_ns));
    ASSERT_EQ(0, aeron_archive_connect(&archive, ctx));

    int64_t subscription_id;
    ASSERT_EQ(0, aeron_archive_start_recording(
        &subscription_id,
        archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL));


    fprintf(stderr, "start recording succeeded\n");

    {
        aeron_t *aeron = aeron_archive_get_aeron(archive);
        aeron_async_add_subscription_t *async_add_subscription;
        aeron_subscription_t *subscription = nullptr;
        aeron_async_add_publication_t *async_add_publication;
        aeron_publication_t *publication = nullptr;

        ASSERT_EQ(0, aeron_async_add_subscription(
            &async_add_subscription,
            aeron,
            m_recordingChannel.c_str(),
            m_recordingStreamId,
            nullptr,
            nullptr,
            nullptr,
            nullptr));

        ASSERT_EQ(0, aeron_async_add_publication(
            &async_add_publication,
            aeron,
            m_recordingChannel.c_str(),
            m_recordingStreamId));

        aeron_async_add_subscription_poll(&subscription, async_add_subscription);
        while (nullptr == subscription)
        {
            aeron_idle_strategy_sleeping_idle((void *)&idle_duration_ns, 0);
            aeron_async_add_subscription_poll(&subscription, async_add_subscription);
        }

        aeron_async_add_publication_poll(&publication, async_add_publication);
        while (nullptr == publication)
        {
            aeron_idle_strategy_sleeping_idle((void *)&idle_duration_ns, 0);
            aeron_async_add_publication_poll(&publication, async_add_publication);
        }

        session_id = aeron_publication_session_id(publication);

        aeron_counters_reader_t *counters_reader = aeron_counters_reader(aeron);
        int32_t counter_id = getRecordingCounterId(session_id, counters_reader);
        recording_id_from_counter = aeron_archive_recording_pos_get_recording_id(counters_reader, counter_id);

        fprintf(stderr, "found counter id :: %u\n", counter_id);

        offerMessages(publication, message_count, message_prefix, 0);
        consumeMessages(subscription, message_count, message_prefix);

        stop_position = aeron_publication_position(publication);

        while (*aeron_counters_reader_addr(counters_reader, counter_id) < stop_position)
        {
            aeron_idle_strategy_sleeping_idle((void *)&idle_duration_ns, 0);
        }

        fprintf(stderr, "stop position reached %llu\n", stop_position);

        int64_t found_recording_position;
        EXPECT_EQ(0, aeron_archive_get_recording_position(
            &found_recording_position,
            archive,
            recording_id_from_counter));
        EXPECT_EQ(stop_position, found_recording_position);

        fprintf(stderr, "found recording position %llu\n", found_recording_position);

        int64_t found_stop_position;
        EXPECT_EQ(0, aeron_archive_get_stop_position(
            &found_stop_position,
            archive,
            recording_id_from_counter));
        EXPECT_EQ(AERON_NULL_VALUE, found_stop_position);

        fprintf(stderr, "found stop position %llu\n", found_stop_position);

        int64_t found_max_recorded_position;
        EXPECT_EQ(0, aeron_archive_get_max_recorded_position(
            &found_max_recorded_position,
            archive,
            recording_id_from_counter));
        EXPECT_EQ(stop_position, found_max_recorded_position);

        fprintf(stderr, "found max recorded position %llu\n", found_max_recorded_position);
    }

    EXPECT_EQ(0, aeron_archive_stop_recording(
        archive,
        subscription_id));

    int64_t found_recording_id;
    const char *channel_fragment = "endpoint=localhost:3333";
    EXPECT_EQ(0, aeron_archive_find_last_matching_recording(
        &found_recording_id,
        archive,
        0,
        channel_fragment,
        m_recordingStreamId,
        session_id));

    EXPECT_EQ(recording_id_from_counter, found_recording_id);

    int64_t found_stop_position;
    EXPECT_EQ(0, aeron_archive_get_stop_position(
        &found_stop_position,
        archive,
        recording_id_from_counter));
    EXPECT_EQ(stop_position, found_stop_position);

    int32_t count;

    recording_descriptor_consumer_clientd_t clientd;
    clientd.recording_id = found_recording_id;
    clientd.stream_id = m_recordingStreamId;

    EXPECT_EQ(0, aeron_archive_list_recording(
        &count,
        archive,
        found_recording_id,
        recording_descriptor_consumer,
        &clientd));
    EXPECT_EQ(1, count);

    ASSERT_EQ(0, aeron_archive_close(archive));
    ASSERT_EQ(0, aeron_archive_context_close(ctx));
}
