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
#include "uri/aeron_uri_string_builder.h"
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
    // fprintf(stderr, "got a message!!\n");

    auto *cd = (fragment_handler_clientd_t *)clientd;
    cd->received++;
    cd->position = aeron_header_position(header);
}

aeron_archive_encoded_credentials_t default_creds = { "admin:admin", 11 };
aeron_archive_encoded_credentials_t bad_creds = { "admin:NotAdmin", 14 };

static aeron_archive_encoded_credentials_t *encoded_credentials_supplier(void *clientd)
{
    return (aeron_archive_encoded_credentials_t *)clientd;
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
        m_test_archive = std::make_shared<TestArchive>(
            aeron_dir,
            sourceArchiveDir,
            std::cout,
            "aeron:udp?endpoint=localhost:8010",
            "aeron:udp?endpoint=localhost:0",
            archiveId);
    }

    void DoTearDown()
    {
        if (nullptr != m_archive)
        {
            ASSERT_EQ(0, aeron_archive_close(m_archive));
        }
        if (nullptr != m_ctx)
        {
            ASSERT_EQ(0, aeron_archive_context_close(m_ctx));
        }
    }

    void idle()
    {
        aeron_idle_strategy_sleeping_idle((void *)&m_idle_duration_ns, 0);
    }

    void connect()
    {
        ASSERT_EQ(0, aeron_archive_context_init(&m_ctx));
        ASSERT_EQ(0, aeron_archive_context_set_idle_strategy(m_ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
        ASSERT_EQ(0, aeron_archive_context_set_credentials_supplier(
            m_ctx,
            encoded_credentials_supplier,
            nullptr,
            nullptr,
            &default_creds));
        ASSERT_EQ(0, aeron_archive_connect(&m_archive, m_ctx));

        m_aeron = aeron_archive_get_aeron(m_archive);
    }

    aeron_subscription_t *addSubscription(std::string channel, int32_t stream_id)
    {
        aeron_async_add_subscription_t *async_add_subscription;
        aeron_subscription_t *subscription = nullptr;

        aeron_async_add_subscription(
            &async_add_subscription,
            m_aeron,
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

    aeron_publication_t *addPublication(aeron_t *aeron, std::string channel, int32_t stream_id)
    {
        aeron_async_add_publication_t *async_add_publication;
        aeron_publication_t *publication = nullptr;

        if (aeron_async_add_publication(
            &async_add_publication,
            aeron,
            channel.c_str(),
            stream_id) < 0)
        {
            fprintf(stderr, " -- GOT AN ERROR :: %s\n", aeron_errmsg());
        }

        if (aeron_async_add_publication_poll(&publication, async_add_publication) < 0)
        {
            fprintf(stderr, " -- GOT AN ERROR :: %s\n", aeron_errmsg());
        }
        while (nullptr == publication)
        {
            idle();
            if (aeron_async_add_publication_poll(&publication, async_add_publication) < 0)
            {
                fprintf(stderr, " -- GOT AN ERROR :: %s\n", aeron_errmsg());
            }
        }

        return publication;
    }

    void setupCounters(int32_t session_id)
    {
        m_counters_reader = aeron_counters_reader(m_aeron);
        m_counter_id = getRecordingCounterId(session_id, m_counters_reader);
        m_recording_id_from_counter = aeron_archive_recording_pos_get_recording_id(m_counters_reader, m_counter_id);
    }

    void waitUntilCaughtUp(int64_t position)
    {
        while (*aeron_counters_reader_addr(m_counters_reader, m_counter_id) < position)
        {
            idle();
        }
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
        size_t message_count = 10,
        size_t start_count = 0,
        const char *message_prefix = "Message ")
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
        size_t message_count = 10)
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

    bool attemptReplayMerge(
        aeron_archive_replay_merge_t *replay_merge,
        aeron_publication_t *publication,
        aeron_fragment_handler_t handler,
        void *clientd,
        size_t total_message_count,
        size_t *messages_published,
        size_t *received_message_count,
        const char *message_prefix = "Message ")
    {
        for (size_t i = *messages_published; i < total_message_count; i++)
        {
            char message[1000];
            size_t len = snprintf(message, 1000, "%s%lu", message_prefix, i);

            while (aeron_publication_offer(publication, (uint8_t *)message, len, nullptr, nullptr) < 0)
            {
                idle();

                int fragments = aeron_archive_replay_merge_poll(replay_merge, handler, clientd, m_fragment_limit);

                if (0 == fragments && aeron_archive_replay_merge_has_failed(replay_merge))
                {
                    return false;
                }
            }

            (*messages_published)++;
        }

        while (!aeron_archive_replay_merge_is_merged(replay_merge))
        {
            int fragments = aeron_archive_replay_merge_poll(replay_merge, handler, clientd, m_fragment_limit);

            if (0 == fragments && aeron_archive_replay_merge_has_failed(replay_merge))
            {
                return false;
            }

            idle();
        }

        aeron_image_t *image = aeron_archive_replay_merge_image(replay_merge);
        while (*received_message_count < total_message_count)
        {
            int fragments = aeron_image_poll(image, handler, clientd, m_fragment_limit);

            if (0 == fragments && aeron_image_is_closed(image))
            {
                return false;
            }

            idle();
        }

        return true;
    }

protected:
    const std::string m_archiveDir = ARCHIVE_DIR;

    const std::string m_recordingChannel = "aeron:udp?endpoint=localhost:3333";
    const std::int32_t m_recordingStreamId = 33;
    const std::string m_replayChannel = "aeron:udp?endpoint=localhost:6666";
    const std::int32_t m_replayStreamId = 66;

    const int m_fragment_limit = 10;

    aeron_counters_reader_t *m_counters_reader;
    std::int32_t m_counter_id;
    std::int64_t m_recording_id_from_counter;

    std::ostringstream m_stream;

    std::shared_ptr<TestArchive> m_test_archive;

    bool m_debug = true;

    const uint64_t m_idle_duration_ns = UINT64_C(1000) * UINT64_C(1000); /* 1ms */
    //const uint64_t m_idle_duration_ns = UINT64_C(5) * UINT64_C(1000) * UINT64_C(1000); /* 5ms */

    aeron_archive_context_t *m_ctx = nullptr;
    aeron_archive_t *m_archive = nullptr;
    aeron_t *m_aeron = nullptr;
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

struct SubscriptionDescriptor
{
    const std::int64_t m_controlSessionId;
    const std::int64_t m_correlationId;
    const std::int64_t m_subscriptionId;
    const std::int32_t m_streamId;

    SubscriptionDescriptor(
        std::int64_t controlSessionId,
        std::int64_t correlationId,
        std::int64_t subscriptionId,
        std::int32_t streamId) :
        m_controlSessionId(controlSessionId),
        m_correlationId(correlationId),
        m_subscriptionId(subscriptionId),
        m_streamId(streamId)
    {
    }
};

struct subscription_descriptor_consumer_clientd
{
    std::vector<SubscriptionDescriptor> descriptors;
};

static void recording_subscription_descriptor_consumer(
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t subscription_id,
    int32_t stream_id,
    const char *stripped_channel,
    void *clientd)
{
    fprintf(stderr, "GOT THE LIST RECORDING SUBSCRIPTION CALLBACK\n");

    auto cd = (subscription_descriptor_consumer_clientd *)clientd;
    cd->descriptors.emplace_back(control_session_id, correlation_id, subscription_id, stream_id);
}

TEST_F(AeronCArchiveTest, shouldAsyncConnectToArchive)
{
    aeron_archive_context_t *ctx;
    aeron_archive_async_connect_t *async;
    aeron_archive_t *archive = nullptr;

    ASSERT_EQ(0, aeron_archive_context_init(&ctx));
    ASSERT_EQ(0, aeron_archive_context_set_idle_strategy(ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
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

    ASSERT_EQ(0, aeron_archive_context_init(&ctx));
    ASSERT_EQ(0, aeron_archive_context_set_idle_strategy(ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ(0, aeron_archive_context_set_credentials_supplier(
        ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &default_creds));
    ASSERT_EQ(0, aeron_archive_connect(&archive, ctx));
    connect();

    aeron_subscription_t *subscription = aeron_archive_get_control_response_subscription(archive);
    ASSERT_TRUE(aeron_subscription_is_connected(subscription));

    ASSERT_EQ(42, aeron_archive_get_archive_id(archive));

    ASSERT_EQ(0, aeron_archive_close(archive));
    ASSERT_EQ(0, aeron_archive_context_close(ctx));
}

TEST_F(AeronCArchiveTest, shouldRecordPublicationAndFindRecording)
{
    int32_t session_id;
    int64_t stop_position;

    connect();

    int64_t subscription_id;
    ASSERT_EQ(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    {
        aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(m_aeron, m_recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        setupCounters(session_id);

        offerMessages(publication);
        consumeMessages(subscription);

        stop_position = aeron_publication_position(publication);

        waitUntilCaughtUp(stop_position);

        int64_t found_recording_position;
        EXPECT_EQ(0, aeron_archive_get_recording_position(
            &found_recording_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(stop_position, found_recording_position);

        fprintf(stderr, "found recording position %llu\n", found_recording_position);

        int64_t found_stop_position;
        EXPECT_EQ(0, aeron_archive_get_stop_position(
            &found_stop_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(AERON_NULL_VALUE, found_stop_position);

        fprintf(stderr, "found stop position %llu\n", found_stop_position);

        int64_t found_max_recorded_position;
        EXPECT_EQ(0, aeron_archive_get_max_recorded_position(
            &found_max_recorded_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(stop_position, found_max_recorded_position);

        fprintf(stderr, "found max recorded position %llu\n", found_max_recorded_position);
    }

    EXPECT_EQ(0, aeron_archive_stop_recording(
        m_archive,
        subscription_id));

    int64_t found_recording_id;
    const char *channel_fragment = "endpoint=localhost:3333";
    EXPECT_EQ(0, aeron_archive_find_last_matching_recording(
        &found_recording_id,
        m_archive,
        0,
        channel_fragment,
        m_recordingStreamId,
        session_id));

    EXPECT_EQ(m_recording_id_from_counter, found_recording_id);

    int64_t found_stop_position;
    EXPECT_EQ(0, aeron_archive_get_stop_position(
        &found_stop_position,
        m_archive,
        m_recording_id_from_counter));
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
        m_archive,
        found_recording_id,
        recording_descriptor_consumer,
        &clientd));
    EXPECT_EQ(1, count);
}

TEST_F(AeronCArchiveTest, shouldRecordThenReplay)
{
    int32_t session_id;
    int64_t stop_position;

    connect();

    int64_t subscription_id;
    ASSERT_EQ(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    {
        aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(m_aeron, m_recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        setupCounters(session_id);

        bool is_active;
        EXPECT_EQ(0, aeron_archive_recording_pos_is_active(
            &is_active,
            m_counters_reader,
            m_counter_id,
            m_recording_id_from_counter));
        EXPECT_TRUE(is_active);

        EXPECT_EQ(m_counter_id, aeron_archive_recording_pos_find_counter_id_by_recording_id(
            m_counters_reader,
            m_recording_id_from_counter));

        {
            int32_t sib_len = 1000;
            const char source_identity_buffer[1000] = { '\0' };

            EXPECT_EQ(0,
                aeron_archive_recording_pos_get_source_identity(
                    m_counters_reader,
                    m_counter_id,
                    source_identity_buffer,
                    &sib_len));
            fprintf(stderr, "SI :: %s (len: %i)\n", source_identity_buffer, sib_len);
            EXPECT_EQ(9, sib_len);
            EXPECT_STREQ("aeron:ipc", source_identity_buffer);
        }

        offerMessages(publication);
        consumeMessages(subscription);

        stop_position = aeron_publication_position(publication);

        waitUntilCaughtUp(stop_position);
    }

    EXPECT_EQ(0, aeron_archive_stop_recording(
        m_archive,
        subscription_id));

    int64_t found_stop_position;
    EXPECT_EQ(0, aeron_archive_get_stop_position(
        &found_stop_position,
        m_archive,
        m_recording_id_from_counter));
    while (found_stop_position != stop_position)
    {
        idle();

        EXPECT_EQ(0, aeron_archive_get_stop_position(
            &found_stop_position,
            m_archive,
            m_recording_id_from_counter));
    }

    {
        int64_t position = 0;
        int64_t length = stop_position - position;

        aeron_subscription_t *subscription = addSubscription(m_replayChannel, m_replayStreamId);

        aeron_archive_replay_params_t replay_params;
        aeron_archive_replay_params_init(&replay_params);

        replay_params.position = position;
        replay_params.length = length;
        replay_params.file_io_max_length = 4096;

        ASSERT_EQ(0, aeron_archive_start_replay(
            nullptr,
            m_archive,
            m_recording_id_from_counter,
            m_replayChannel.c_str(),
            m_replayStreamId,
            &replay_params));

        consumeMessages(subscription);

        aeron_image_t *image = aeron_subscription_image_at_index(subscription, 0);
        ASSERT_EQ(stop_position, aeron_image_position(image));
    }
}

TEST_F(AeronCArchiveTest, shouldRecordThenBoundedReplay)
{
    int32_t session_id;
    int64_t stop_position;

    connect();

    int64_t subscription_id;
    ASSERT_EQ(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    {

        aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(m_aeron, m_recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        setupCounters(session_id);

        offerMessages(publication);
        consumeMessages(subscription);

        stop_position = aeron_publication_position(publication);

        waitUntilCaughtUp(stop_position);
    }

    EXPECT_EQ(0, aeron_archive_stop_recording(
        m_archive,
        subscription_id));

    const char *counter_name = "BoundedTestCounter";

    aeron_async_add_counter_t *async_add_counter;
    aeron_async_add_counter(
        &async_add_counter,
        m_aeron,
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
        m_archive,
        m_recording_id_from_counter));
    while (found_stop_position != stop_position)
    {
        idle();

        EXPECT_EQ(0, aeron_archive_get_stop_position(
            &found_stop_position,
            m_archive,
            m_recording_id_from_counter));
    }

    {
        int64_t position = 0;
        int64_t length = stop_position - position;
        int64_t bounded_length = (length / 4) * 3;
        aeron_counter_set_ordered(aeron_counter_addr(counter), bounded_length);

        aeron_subscription_t *subscription = addSubscription(m_replayChannel, m_replayStreamId);

        aeron_archive_replay_params_t replay_params;
        aeron_archive_replay_params_init(&replay_params);

        replay_params.position = position;
        replay_params.length = length;
        replay_params.bounding_limit_counter_id = counter->counter_id;
        replay_params.file_io_max_length = 4096;

        ASSERT_EQ(0, aeron_archive_start_replay(
            nullptr,
            m_archive,
            m_recording_id_from_counter,
            m_replayChannel.c_str(),
            m_replayStreamId,
            &replay_params));

        int64_t position_consumed = consumeMessagesExpectingBound(
            subscription, position + bounded_length, 1000);

        EXPECT_LT(position + (length / 2), position_consumed);
        EXPECT_LE(position_consumed, position + bounded_length);
    }
}

TEST_F(AeronCArchiveTest, shouldRecordThenReplayThenTruncate)
{
    int32_t session_id;
    int64_t stop_position;

    connect();

    int64_t subscription_id;
    ASSERT_EQ(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    {
        aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(m_aeron, m_recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        setupCounters(session_id);

        offerMessages(publication);
        consumeMessages(subscription);

        stop_position = aeron_publication_position(publication);

        waitUntilCaughtUp(stop_position);

        int64_t found_recording_position;
        EXPECT_EQ(0, aeron_archive_get_recording_position(
            &found_recording_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(stop_position, found_recording_position);

        fprintf(stderr, "found recording position %llu\n", found_recording_position);

        int64_t found_stop_position;
        EXPECT_EQ(0, aeron_archive_get_stop_position(
            &found_stop_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(AERON_NULL_VALUE, found_stop_position);

        fprintf(stderr, "found stop position %llu\n", found_stop_position);

        int64_t found_max_recorded_position;
        EXPECT_EQ(0, aeron_archive_get_max_recorded_position(
            &found_max_recorded_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(stop_position, found_max_recorded_position);

        fprintf(stderr, "found max recorded position %llu\n", found_max_recorded_position);
    }

    EXPECT_EQ(0, aeron_archive_stop_recording(
        m_archive,
        subscription_id));

    int64_t found_recording_id;
    const char *channel_fragment = "endpoint=localhost:3333";
    EXPECT_EQ(0, aeron_archive_find_last_matching_recording(
        &found_recording_id,
        m_archive,
        0,
        channel_fragment,
        m_recordingStreamId,
        session_id));

    EXPECT_EQ(m_recording_id_from_counter, found_recording_id);

    int64_t found_stop_position;
    EXPECT_EQ(0, aeron_archive_get_stop_position(
        &found_stop_position,
        m_archive,
        m_recording_id_from_counter));
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
            m_archive,
            m_recording_id_from_counter,
            m_replayChannel.c_str(),
            m_replayStreamId,
            &replay_params));

        consumeMessages(subscription);

        aeron_image_t *image = aeron_subscription_image_at_index(subscription, 0);
        EXPECT_EQ(stop_position, aeron_image_position(image));
    }

    EXPECT_EQ(0, aeron_archive_truncate_recording(
        nullptr,
        m_archive,
        m_recording_id_from_counter,
        position));

    int32_t count;

    recording_descriptor_consumer_clientd_t clientd;
    clientd.verify_recording_id = false;
    clientd.verify_stream_id = false;
    clientd.verify_start_equals_stop_position = true;

    EXPECT_EQ(0, aeron_archive_list_recording(
        &count,
        m_archive,
        found_recording_id,
        recording_descriptor_consumer,
        &clientd));
    EXPECT_EQ(1, count);
}

TEST_F(AeronCArchiveTest, shouldRecordAndCancelReplayEarly)
{
    int32_t session_id;
    int64_t stop_position;

    connect();

    int64_t subscription_id;
    ASSERT_EQ(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    {
        aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(m_aeron, m_recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        setupCounters(session_id);

        offerMessages(publication);
        consumeMessages(subscription);

        stop_position = aeron_publication_position(publication);

        waitUntilCaughtUp(stop_position);

        int64_t found_recording_position;
        EXPECT_EQ(0, aeron_archive_get_recording_position(
            &found_recording_position,
            m_archive,
            m_recording_id_from_counter));
        EXPECT_EQ(stop_position, found_recording_position);

        EXPECT_EQ(0, aeron_archive_stop_recording(m_archive, subscription_id));

        EXPECT_EQ(0, aeron_archive_get_recording_position(
            &found_recording_position,
            m_archive,
            m_recording_id_from_counter));
        while (AERON_NULL_VALUE != found_recording_position)
        {
            idle();

            EXPECT_EQ(0, aeron_archive_get_recording_position(
                &found_recording_position,
                m_archive,
                m_recording_id_from_counter));
        }
    }

    const int64_t position = 0;
    const int64_t length = stop_position - position;
    int64_t replay_session_id;

    aeron_archive_replay_params_t replay_params;
    aeron_archive_replay_params_init(&replay_params);

    replay_params.position = position;
    replay_params.length = length;
    replay_params.file_io_max_length = 4096;

    ASSERT_EQ(0, aeron_archive_start_replay(
        &replay_session_id,
        m_archive,
        m_recording_id_from_counter,
        m_replayChannel.c_str(),
        m_replayStreamId,
        &replay_params));

    ASSERT_EQ(0, aeron_archive_stop_replay(m_archive, replay_session_id));
}

TEST_F(AeronCArchiveTest, shouldReplayRecordingFromLateJoinPosition)
{
    int32_t session_id;

    connect();

    int64_t subscription_id;
    ASSERT_EQ(0, aeron_archive_start_recording(
        &subscription_id,
        m_archive,
        m_recordingChannel.c_str(),
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    {
        aeron_subscription_t *subscription = addSubscription(m_recordingChannel, m_recordingStreamId);
        aeron_publication_t *publication = addPublication(m_aeron, m_recordingChannel, m_recordingStreamId);

        session_id = aeron_publication_session_id(publication);

        setupCounters(session_id);

        offerMessages(publication);
        consumeMessages(subscription);

        int64_t current_position = aeron_publication_position(publication);

        waitUntilCaughtUp(current_position);

        aeron_archive_replay_params_t replay_params;
        aeron_archive_replay_params_init(&replay_params);

        replay_params.position = current_position;
        replay_params.file_io_max_length = 4096;

        aeron_subscription_t *replay_subscription;

        EXPECT_EQ(0, aeron_archive_replay(
            &replay_subscription,
            m_archive,
            m_recording_id_from_counter,
            m_replayChannel.c_str(),
            m_replayStreamId,
            &replay_params));

        offerMessages(publication);
        consumeMessages(subscription);
        consumeMessages(replay_subscription);

        int64_t end_position = aeron_publication_position(publication);

        aeron_image_t *image = aeron_subscription_image_at_index(replay_subscription, 0);
        EXPECT_EQ(end_position, aeron_image_position(image));
    }
}

TEST_F(AeronCArchiveTest, shouldListRegisteredRecordingSubscriptions)
{
    subscription_descriptor_consumer_clientd clientd;

    int32_t expected_stream_id = 7;
    const char *channelOne = "aeron:ipc";
    const char *channelTwo = "aeron:udp?endpoint=localhost:5678";
    const char *channelThree = "aeron:udp?endpoint=localhost:4321";

    connect();

    int64_t subscription_id_one;
    ASSERT_EQ(0, aeron_archive_start_recording(
        &subscription_id_one,
        m_archive,
        channelOne,
        expected_stream_id,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    int64_t subscription_id_two;
    ASSERT_EQ(0, aeron_archive_start_recording(
        &subscription_id_two,
        m_archive,
        channelTwo,
        expected_stream_id + 1,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    int64_t subscription_id_three;
    ASSERT_EQ(0, aeron_archive_start_recording(
        &subscription_id_three,
        m_archive,
        channelThree,
        expected_stream_id + 2,
        AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
        false));

    int32_t count_one;
    EXPECT_EQ(0, aeron_archive_list_recording_subscriptions(
        &count_one,
        m_archive,
        0,
        5,
        "ipc",
        expected_stream_id,
        true,
        recording_subscription_descriptor_consumer,
        &clientd));
    EXPECT_EQ(1, clientd.descriptors.size());
    EXPECT_EQ(1, count_one);

    clientd.descriptors.clear();

    int32_t count_two;
    EXPECT_EQ(0, aeron_archive_list_recording_subscriptions(
        &count_two,
        m_archive,
        0,
        5,
        "",
        expected_stream_id,
        false,
        recording_subscription_descriptor_consumer,
        &clientd));
    EXPECT_EQ(3, clientd.descriptors.size());
    EXPECT_EQ(3, count_two);

    aeron_archive_stop_recording(m_archive, subscription_id_two);
    clientd.descriptors.clear();

    int32_t count_three;
    EXPECT_EQ(0, aeron_archive_list_recording_subscriptions(
        &count_three,
        m_archive,
        0,
        5,
        "",
        expected_stream_id,
        false,
        recording_subscription_descriptor_consumer,
        &clientd));
    EXPECT_EQ(2, clientd.descriptors.size());
    EXPECT_EQ(2, count_three);

    EXPECT_EQ(1, std::count_if(
        clientd.descriptors.begin(),
        clientd.descriptors.end(),
        [=](const SubscriptionDescriptor &descriptor) { return descriptor.m_subscriptionId == subscription_id_one; }));

    EXPECT_EQ(1, std::count_if(
        clientd.descriptors.begin(),
        clientd.descriptors.end(),
        [=](const SubscriptionDescriptor &descriptor) { return descriptor.m_subscriptionId == subscription_id_three; }));
}

TEST_F(AeronCArchiveTest, shouldMergeFromReplayToLive)
{
    const std::size_t termLength = 64 * 1024;
    const std::string message_prefix = "Message ";
    const std::size_t min_messages_per_term = termLength / (message_prefix.length() + AERON_DATA_HEADER_LENGTH);
    const char *control_endpoint = "localhost:23265";
    const char *recording_endpoint = "localhost:23266";
    const char *live_endpoint = "localhost:23267";
    const char *replay_endpoint = "localhost:0";

    char publication_channel[AERON_MAX_PATH + 1];
    char live_destination[AERON_MAX_PATH + 1];
    char replay_destination[AERON_MAX_PATH + 1];
    char recording_channel[AERON_MAX_PATH + 1];
    char subscription_channel[AERON_MAX_PATH + 1];

    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_CONTROL_KEY, control_endpoint);
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_CONTROL_MODE_KEY, AERON_UDP_CHANNEL_CONTROL_MODE_DYNAMIC_VALUE);
        aeron_uri_string_builder_put(&builder, AERON_URI_FC_KEY, "tagged,g:99901/1,t:5s");
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_TERM_LENGTH_KEY, termLength);

        aeron_uri_string_builder_sprint(&builder, publication_channel, AERON_MAX_PATH + 1);
        aeron_uri_string_builder_close(&builder);

        // aeron:udp?control-mode=dynamic|term-length=65536|fc=tagged,g:99901/1,t:5s|control=localhost:23265
        fprintf(stderr, "pub channel :: %s\n", publication_channel);
    }

    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_ENDPOINT_KEY, live_endpoint);
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_CONTROL_KEY, control_endpoint);

        aeron_uri_string_builder_sprint(&builder, live_destination, AERON_MAX_PATH + 1);
        aeron_uri_string_builder_close(&builder);

        fprintf(stderr, "live destination :: %s\n", live_destination);
    }

    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_ENDPOINT_KEY, replay_endpoint);

        aeron_uri_string_builder_sprint(&builder, replay_destination, AERON_MAX_PATH + 1);
        aeron_uri_string_builder_close(&builder);

        fprintf(stderr, "replay destination :: %s\n", replay_destination);
    }

    const size_t initial_message_count = min_messages_per_term * 3;
    const size_t subsequent_message_count = min_messages_per_term * 3;
    const size_t total_message_count = min_messages_per_term + subsequent_message_count;

    connect();

    aeron_publication_t *publication = addPublication(m_aeron, publication_channel, m_recordingStreamId);

    int32_t session_id = aeron_publication_session_id(publication);

    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put(&builder, AERON_URI_GTAG_KEY, "99901");
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_SESSION_ID_KEY, session_id);
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_ENDPOINT_KEY, recording_endpoint);
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_CONTROL_KEY, control_endpoint);

        aeron_uri_string_builder_sprint(&builder, recording_channel, AERON_MAX_PATH + 1);
        aeron_uri_string_builder_close(&builder);

        fprintf(stderr, "recording channel :: %s\n", recording_channel);
    }

    {
        aeron_uri_string_builder_t builder;

        aeron_uri_string_builder_init_new(&builder);

        aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
        aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_CONTROL_MODE_KEY, AERON_UDP_CHANNEL_CONTROL_MODE_MANUAL_VALUE);
        aeron_uri_string_builder_put_int32(&builder, AERON_URI_SESSION_ID_KEY, session_id);

        aeron_uri_string_builder_sprint(&builder, subscription_channel, AERON_MAX_PATH + 1);
        aeron_uri_string_builder_close(&builder);

        fprintf(stderr, "subscription channel :: %s\n", subscription_channel);
    }

    ASSERT_EQ(0, aeron_archive_start_recording(
        nullptr,
        m_archive,
        recording_channel,
        m_recordingStreamId,
        AERON_ARCHIVE_SOURCE_LOCATION_REMOTE,
        true));

    setupCounters(session_id);

    bool is_active;
    EXPECT_EQ(0, aeron_archive_recording_pos_is_active(
        &is_active,
        m_counters_reader,
        m_counter_id,
        m_recording_id_from_counter));
    EXPECT_TRUE(is_active);

    EXPECT_EQ(m_counter_id, aeron_archive_recording_pos_find_counter_id_by_recording_id(
        m_counters_reader,
        m_recording_id_from_counter));

    {
        int32_t sib_len = 1000;
        const char source_identity_buffer[1000] = { '\0' };

        EXPECT_EQ(0,
            aeron_archive_recording_pos_get_source_identity(
                m_counters_reader,
                m_counter_id,
                source_identity_buffer,
                &sib_len));
        EXPECT_STREQ("127.0.0.1:23265", source_identity_buffer);
    }

    offerMessages(publication, initial_message_count);

    waitUntilCaughtUp(aeron_publication_position(publication));

    size_t messages_published = initial_message_count;

    fragment_handler_clientd_t clientd;
    clientd.received = 0;
    clientd.position = 0;

    while (true)
    {
        aeron_subscription_t *subscription = addSubscription(subscription_channel, m_recordingStreamId);

        char replay_channel[AERON_MAX_PATH + 1];
        {
            aeron_uri_string_builder_t builder;

            aeron_uri_string_builder_init_new(&builder);

            aeron_uri_string_builder_put(&builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, "udp");
            aeron_uri_string_builder_put_int32(&builder, AERON_URI_SESSION_ID_KEY, session_id);

            aeron_uri_string_builder_sprint(&builder, replay_channel, AERON_MAX_PATH + 1);
            aeron_uri_string_builder_close(&builder);

            fprintf(stderr, "replay channel :: %s\n", replay_channel);
        }

        aeron_archive_replay_merge_t *replay_merge;

        ASSERT_EQ(0, aeron_archive_replay_merge_init(
            &replay_merge,
            subscription,
            m_archive,
            replay_channel,
            replay_destination,
            live_destination,
            m_recording_id_from_counter,
            clientd.position,
            currentTimeMillis(),
            REPLAY_MERGE_PROGRESS_TIMEOUT_DEFAULT_MS));

        if (attemptReplayMerge(
            replay_merge,
            publication,
            fragment_handler,
            &clientd,
            total_message_count,
            &messages_published,
            &clientd.received))
        {
            ASSERT_EQ(0, aeron_archive_replay_merge_close(replay_merge));
            break;
        }

        ASSERT_EQ(0, aeron_archive_replay_merge_close(replay_merge));
        idle();
    }

    EXPECT_EQ(clientd.received, total_message_count);
    EXPECT_EQ(clientd.position, aeron_publication_position(publication));
}

TEST_F(AeronCArchiveTest, shouldFailForIncorrectInitialCredentials)
{
    ASSERT_EQ(0, aeron_archive_context_init(&m_ctx));
    ASSERT_EQ(0, aeron_archive_context_set_idle_strategy(m_ctx, aeron_idle_strategy_sleeping_idle, (void *)&m_idle_duration_ns));
    ASSERT_EQ(0, aeron_archive_context_set_credentials_supplier(
        m_ctx,
        encoded_credentials_supplier,
        nullptr,
        nullptr,
        &bad_creds));

    ASSERT_EQ(-1, aeron_archive_connect(&m_archive, m_ctx));
}
