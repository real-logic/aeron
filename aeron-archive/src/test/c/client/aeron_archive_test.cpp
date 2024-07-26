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
#include "aeron_counter.h"
}

#include "../TestArchive.h"

typedef struct fragment_handler_clientd_stct
{
    size_t received;
    int64_t position;
}
fragment_handler_clientd_t;

void fragment_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_stct *header)
{
    fprintf(stderr, "got a message!!\n");

    auto *cd = (fragment_handler_clientd_t *)clientd;
    cd->received++;
    cd->position = aeron_header_position(header);
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

    static void idle()
    {
        const uint64_t idle_duration_ns = UINT64_C(1000) * UINT64_C(1000); /* 1ms */
        aeron_idle_strategy_sleeping_idle((void *)&idle_duration_ns, 0);
    }

    static aeron_subscription_t *addSubscription(aeron_t *aeron, std::string channel, int32_t stream_id)
    {
        aeron_async_add_subscription_t *async_add_subscription;
        aeron_subscription_t *subscription = nullptr;

        aeron_async_add_subscription(
            &async_add_subscription,
            aeron,
            channel.c_str(),
            stream_id,
            nullptr,
            nullptr,
            nullptr,
            nullptr);

        aeron_async_add_subscription_poll(&subscription, async_add_subscription);
        while (nullptr == subscription)
        {
            idle();
            aeron_async_add_subscription_poll(&subscription, async_add_subscription);
        }

        return subscription;
    }

    static aeron_publication_t *addPublication(aeron_t *aeron, std::string channel, int32_t stream_id)
    {
        aeron_async_add_publication_t *async_add_publication;
        aeron_publication_t *publication = nullptr;

        aeron_async_add_publication(
            &async_add_publication,
            aeron,
            channel.c_str(),
            stream_id);

        aeron_async_add_publication_poll(&publication, async_add_publication);
        while (nullptr == publication)
        {
            idle();
            aeron_async_add_publication_poll(&publication, async_add_publication);
        }

        return publication;
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
        fragment_handler_clientd_t clientd;
        clientd.received = 0;

        while (clientd.received < message_count)
        {
            if (0 == aeron_subscription_poll(subscription, fragment_handler, (void *)&clientd, 10))
            {
                aeron_idle_strategy_yielding_idle(nullptr, 0);
            }
        }

        ASSERT_EQ(clientd.received, message_count);
    }

    static int64_t consumeMessagesExpectingBound(
        aeron_subscription_t *subscription,
        int64_t bound_position,
        const char *message_prefix,
        int64_t timeout_ms)
    {
        fragment_handler_clientd_t clientd;
        clientd.received = 0;
        clientd.position = 0;

        int64_t deadline_ms = currentTimeMillis() + timeout_ms;

        while (currentTimeMillis() < deadline_ms)
        {
            if (0 == aeron_subscription_poll(subscription, fragment_handler, (void *)&clientd, 10))
            {
                aeron_idle_strategy_yielding_idle(nullptr, 0);
            }
        }

        return clientd.position;
    }

protected:
    const std::string m_archiveDir = ARCHIVE_DIR;

    const std::string m_recordingChannel = "aeron:udp?endpoint=localhost:3333";
    const std::int32_t m_recordingStreamId = 33;
    const std::string m_replayChannel = "aeron:udp?endpoint=localhost:6666";
    const std::int32_t m_replayStreamId = 66;

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
    bool verify_recording_id;
    int64_t recording_id;
    bool verify_stream_id;
    int32_t stream_id;
    bool verify_start_equals_stop_position;
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

    if (cd->verify_recording_id)
    {
        EXPECT_EQ(cd->recording_id, recording_id);
    }
    if (cd->verify_stream_id)
    {
        EXPECT_EQ(cd->stream_id, stream_id);
    }
    if (cd->verify_start_equals_stop_position)
    {
        EXPECT_EQ(start_position, stop_position);
    }

    fprintf(stderr, "GOT THE LIST RECORDING CALLBACK\n");
    fprintf(stderr, "%s\n", stripped_channel);
    fprintf(stderr, "%s\n", original_channel);
    fprintf(stderr, "%s\n", source_identity);
}

aeron_archive_encoded_credentials_t default_creds = { "admin:admin", 11 };

static aeron_archive_encoded_credentials_t *encoded_credentials_supplier(void *clientd)
{
    return (aeron_archive_encoded_credentials_t *)clientd;
}

TEST_F(AeronCArchiveTest, shouldAsyncConnectToArchive)
{
    aeron_archive_context_t *ctx;
    aeron_archive_async_connect_t *async;
    aeron_archive_t *archive = nullptr;

    const uint64_t idle_duration_ns = UINT64_C(1000) * UINT64_C(1000); /* 1ms */

    ASSERT_EQ(0, aeron_archive_context_init(&ctx));
    ASSERT_EQ(0, aeron_archive_context_set_idle_strategy(ctx, aeron_idle_strategy_sleeping_idle, (void *)&idle_duration_ns));
    ASSERT_EQ(0, aeron_archive_context_set_credentials_supplier(
        ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds));
    ASSERT_EQ(0, aeron_archive_async_connect(&async, ctx));
    ASSERT_EQ(0, aeron_archive_async_connect_poll(&archive, async));

    while (nullptr == archive)
    {
        idle();

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
    ASSERT_EQ(0, aeron_archive_context_set_credentials_supplier(
        ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds));
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
    ASSERT_EQ(0, aeron_archive_context_set_credentials_supplier(
        ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds));
    ASSERT_EQ(0, aeron_archive_connect(&archive, ctx));

    int64_t subscription_id;
    ASSERT_EQ(0, aeron_archive_start_recording(
        &subscription_id,
        archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL));

    {
        aeron_t *aeron = aeron_archive_get_aeron(archive);

        aeron_subscription_t *subscription = addSubscription(aeron, m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(aeron, m_recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        aeron_counters_reader_t *counters_reader = aeron_counters_reader(aeron);
        int32_t counter_id = getRecordingCounterId(session_id, counters_reader);
        recording_id_from_counter = aeron_archive_recording_pos_get_recording_id(counters_reader, counter_id);

        offerMessages(publication, message_count, message_prefix, 0);
        consumeMessages(subscription, message_count, message_prefix);

        stop_position = aeron_publication_position(publication);

        while (*aeron_counters_reader_addr(counters_reader, counter_id) < stop_position)
        {
            idle();
        }

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
    clientd.verify_recording_id = true;
    clientd.recording_id = found_recording_id;
    clientd.verify_stream_id = true;
    clientd.stream_id = m_recordingStreamId;
    clientd.verify_start_equals_stop_position = false;

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

TEST_F(AeronCArchiveTest, shouldRecordThenReplay)
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
    ASSERT_EQ(0,
        aeron_archive_context_set_idle_strategy(ctx, aeron_idle_strategy_sleeping_idle, (void *)&idle_duration_ns));
    ASSERT_EQ(0, aeron_archive_context_set_credentials_supplier(
        ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds));
    ASSERT_EQ(0, aeron_archive_connect(&archive, ctx));

    int64_t subscription_id;
    ASSERT_EQ(0, aeron_archive_start_recording(
        &subscription_id,
        archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL));

    {
        aeron_t *aeron = aeron_archive_get_aeron(archive);

        aeron_subscription_t *subscription = addSubscription(aeron, m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(aeron, m_recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        aeron_counters_reader_t *counters_reader = aeron_counters_reader(aeron);
        int32_t counter_id = getRecordingCounterId(session_id, counters_reader);
        recording_id_from_counter = aeron_archive_recording_pos_get_recording_id(counters_reader, counter_id);

        bool is_active;
        EXPECT_EQ(0, aeron_archive_recording_pos_is_active(counters_reader, counter_id, recording_id_from_counter, &is_active));

        EXPECT_EQ(counter_id, aeron_archive_recording_pos_find_counter_id_by_recording_id(counters_reader, recording_id_from_counter));

        {
            int32_t sib_len = 1000;
            const char source_identity_buffer[1000] = { '\0' };

            EXPECT_EQ(0,
                aeron_archive_recording_pos_get_source_identity(
                    counters_reader,
                    counter_id,
                    source_identity_buffer,
                    &sib_len));
            fprintf(stderr, "SI :: %s (len: %i)\n", source_identity_buffer, sib_len);
            EXPECT_EQ(9, sib_len);
            EXPECT_STREQ("aeron:ipc", source_identity_buffer);
        }

        offerMessages(publication, message_count, message_prefix, 0);
        consumeMessages(subscription, message_count, message_prefix);

        stop_position = aeron_publication_position(publication);

        while (*aeron_counters_reader_addr(counters_reader, counter_id) < stop_position)
        {
            idle();
        }
    }

    EXPECT_EQ(0, aeron_archive_stop_recording(
        archive,
        subscription_id));

    int64_t found_stop_position;
    EXPECT_EQ(0, aeron_archive_get_stop_position(
        &found_stop_position,
        archive,
        recording_id_from_counter));
    while (found_stop_position != stop_position)
    {
        idle();

        EXPECT_EQ(0, aeron_archive_get_stop_position(
            &found_stop_position,
            archive,
            recording_id_from_counter));
    }

    {
        int64_t position = 0;
        int64_t length = stop_position - position;

        aeron_t *aeron = aeron_archive_get_aeron(archive);

        aeron_subscription_t *subscription = addSubscription(aeron, m_replayChannel, m_replayStreamId);

        aeron_archive_replay_params_t replay_params;
        aeron_archive_replay_params_init(&replay_params);

        replay_params.position = position;
        replay_params.length = length;
        replay_params.file_io_max_length = 4096;

        ASSERT_EQ(0, aeron_archive_start_replay(
            nullptr,
            archive,
            recording_id_from_counter,
            m_replayChannel.c_str(),
            m_replayStreamId,
            &replay_params));

        consumeMessages(subscription, message_count, message_prefix);

        aeron_image_t *image = aeron_subscription_image_at_index(subscription, 0);
        ASSERT_EQ(stop_position, aeron_image_position(image));
    }

    ASSERT_EQ(0, aeron_archive_close(archive));
    ASSERT_EQ(0, aeron_archive_context_close(ctx));
}

TEST_F(AeronCArchiveTest, shouldRecordThenBoundedReplay)
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
    ASSERT_EQ(0,
        aeron_archive_context_set_idle_strategy(ctx, aeron_idle_strategy_sleeping_idle, (void *)&idle_duration_ns));
    ASSERT_EQ(0, aeron_archive_context_set_credentials_supplier(
        ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds));
    ASSERT_EQ(0, aeron_archive_connect(&archive, ctx));

    int64_t subscription_id;
    ASSERT_EQ(0, aeron_archive_start_recording(
        &subscription_id,
        archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL));

    aeron_t *aeron = aeron_archive_get_aeron(archive);

    {

        aeron_subscription_t *subscription = addSubscription(aeron, m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(aeron, m_recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        aeron_counters_reader_t *counters_reader = aeron_counters_reader(aeron);
        int32_t counter_id = getRecordingCounterId(session_id, counters_reader);
        recording_id_from_counter = aeron_archive_recording_pos_get_recording_id(counters_reader, counter_id);

        offerMessages(publication, message_count, message_prefix, 0);
        consumeMessages(subscription, message_count, message_prefix);

        stop_position = aeron_publication_position(publication);

        while (*aeron_counters_reader_addr(counters_reader, counter_id) < stop_position)
        {
            idle();
        }
    }

    EXPECT_EQ(0, aeron_archive_stop_recording(
        archive,
        subscription_id));

    const char *counter_name = "BoundedTestCounter";

    aeron_async_add_counter_t *async_add_counter;
    aeron_async_add_counter(
        &async_add_counter,
        aeron,
        10001,
        (const uint8_t *)counter_name,
        strlen(counter_name),
        counter_name,
        strlen(counter_name));

    aeron_counter_t *counter = nullptr;
    aeron_async_add_counter_poll(&counter, async_add_counter);
    while (nullptr == counter)
    {
        idle();
        aeron_async_add_counter_poll(&counter, async_add_counter);
    }

    int64_t found_stop_position;
    EXPECT_EQ(0, aeron_archive_get_stop_position(
        &found_stop_position,
        archive,
        recording_id_from_counter));
    while (found_stop_position != stop_position)
    {
        idle();

        EXPECT_EQ(0, aeron_archive_get_stop_position(
            &found_stop_position,
            archive,
            recording_id_from_counter));
    }

    {
        int64_t position = 0;
        int64_t length = stop_position - position;
        int64_t bounded_length = (length / 4) * 3;
        aeron_counter_set_ordered(aeron_counter_addr(counter), bounded_length);

        aeron_subscription_t *subscription = addSubscription(aeron, m_replayChannel, m_replayStreamId);

        aeron_archive_replay_params_t replay_params;
        aeron_archive_replay_params_init(&replay_params);

        replay_params.position = position;
        replay_params.length = length;
        replay_params.bounding_limit_counter_id = counter->counter_id;
        replay_params.file_io_max_length = 4096;

        ASSERT_EQ(0, aeron_archive_start_replay(
            nullptr,
            archive,
            recording_id_from_counter,
            m_replayChannel.c_str(),
            m_replayStreamId,
            &replay_params));

        int64_t position_consumed = consumeMessagesExpectingBound(
            subscription, position + bounded_length, message_prefix, 1000);

        EXPECT_LT(position + (length / 2), position_consumed);
        EXPECT_LE(position_consumed, position + bounded_length);
    }

    ASSERT_EQ(0, aeron_archive_close(archive));
    ASSERT_EQ(0, aeron_archive_context_close(ctx));
}

TEST_F(AeronCArchiveTest, shouldRecordThenReplayThenTruncate)
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
    ASSERT_EQ(0,
        aeron_archive_context_set_idle_strategy(ctx, aeron_idle_strategy_sleeping_idle, (void *)&idle_duration_ns));
    ASSERT_EQ(0, aeron_archive_context_set_credentials_supplier(
        ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds));
    ASSERT_EQ(0, aeron_archive_connect(&archive, ctx));

    int64_t subscription_id;
    ASSERT_EQ(0, aeron_archive_start_recording(
        &subscription_id,
        archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL));

    {
        aeron_t *aeron = aeron_archive_get_aeron(archive);

        aeron_subscription_t *subscription = addSubscription(aeron, m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(aeron, m_recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        aeron_counters_reader_t *counters_reader = aeron_counters_reader(aeron);
        int32_t counter_id = getRecordingCounterId(session_id, counters_reader);
        recording_id_from_counter = aeron_archive_recording_pos_get_recording_id(counters_reader, counter_id);

        offerMessages(publication, message_count, message_prefix, 0);
        consumeMessages(subscription, message_count, message_prefix);

        stop_position = aeron_publication_position(publication);

        while (*aeron_counters_reader_addr(counters_reader, counter_id) < stop_position)
        {
            idle();
        }

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

    int64_t position = 0;

    {
        int64_t length = stop_position - position;

        aeron_archive_replay_params_t replay_params;
        aeron_archive_replay_params_init(&replay_params);

        replay_params.position = position;
        replay_params.length = length;
        replay_params.file_io_max_length = 4096;

        aeron_subscription_t *subscription;

        EXPECT_EQ(0, aeron_archive_replay(
            &subscription,
            archive,
            recording_id_from_counter,
            m_replayChannel.c_str(),
            m_replayStreamId,
            &replay_params));

        consumeMessages(subscription, message_count, message_prefix);

        aeron_image_t *image = aeron_subscription_image_at_index(subscription, 0);
        EXPECT_EQ(stop_position, aeron_image_position(image));
    }

    EXPECT_EQ(0, aeron_archive_truncate_recording(nullptr, archive, recording_id_from_counter, position));

    int32_t count;

    recording_descriptor_consumer_clientd_t clientd;
    clientd.verify_recording_id = false;
    clientd.verify_stream_id = false;
    clientd.verify_start_equals_stop_position = true;

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
