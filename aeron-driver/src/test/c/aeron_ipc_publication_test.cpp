/*
 * Copyright 2014-2022 Real Logic Limited.
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

#include <array>
#include <gtest/gtest.h>

extern "C"
{
#include "aeron_ipc_publication.h"
#include "aeron_driver_sender.h"
#include "aeron_position.h"

int aeron_driver_ensure_dir_is_recreated(aeron_driver_context_t *context);
}

#define CAPACITY (32 * 1024)

typedef std::array<std::uint8_t, CAPACITY> buffer_t;
typedef std::array<std::uint8_t, 4 * CAPACITY> buffer_4x_t;

class IpcPublicationTest : public testing::Test
{
protected:
    void SetUp() override
    {
        aeron_driver_context_init(&m_context);
        aeron_driver_context_set_dir_delete_on_start(m_context, true);

        aeron_counters_manager_init(
            &m_counters_manager,
            m_counter_meta_buffer.data(), m_counter_meta_buffer.size(),
            m_counter_value_buffer.data(), m_counter_value_buffer.size(),
            &m_cached_clock,
            1000);

        aeron_system_counters_init(&m_system_counters, &m_counters_manager);

        aeron_driver_ensure_dir_is_recreated(m_context);
    }

    void TearDown() override
    {
        for (auto publication : m_publications)
        {
            aeron_ipc_publication_close(&m_counters_manager, publication);
        }

        aeron_system_counters_close(&m_system_counters);
        aeron_counters_manager_close(&m_counters_manager);
        aeron_driver_context_close(m_context);
    }

    aeron_ipc_publication_t *createPublication(const char *uri)
    {
        int64_t registration_id = 1;
        int32_t stream_id = 10;
        int32_t session_id = 10;
        size_t uri_length = strlen(uri);

        aeron_position_t pub_pos_position;
        aeron_position_t pub_lmt_position;

        pub_pos_position.counter_id = aeron_counter_publisher_position_allocate(
            &m_counters_manager, registration_id, session_id, stream_id, uri_length, uri);
        pub_lmt_position.counter_id = aeron_counter_publisher_limit_allocate(
            &m_counters_manager, registration_id, session_id, stream_id, uri_length, uri);

        if (pub_pos_position.counter_id < 0 || pub_lmt_position.counter_id < 0)
        {
            return nullptr;
        }

        aeron_counters_manager_counter_owner_id(
            &m_counters_manager, pub_lmt_position.counter_id, 1);

        pub_pos_position.value_addr = aeron_counters_manager_addr(
            &m_counters_manager, pub_pos_position.counter_id);
        pub_lmt_position.value_addr = aeron_counters_manager_addr(
            &m_counters_manager, pub_lmt_position.counter_id);

        aeron_driver_uri_publication_params_t params = {};

        aeron_ipc_publication_t *publication = nullptr;
        if (aeron_ipc_publication_create(
            &publication,
            m_context,
            session_id,
            stream_id,
            registration_id,
            &pub_pos_position,
            &pub_lmt_position,
            0,
            &params,
            false,
            &m_system_counters,
            uri_length,
            uri) < 0)
        {
            return nullptr;
        }

        m_publications.push_back(publication);

        return publication;
    }

    static uint64_t noSpaceAvailable(const char* path)
    {
        return 0;
    }

    aeron_driver_context_t *m_context = nullptr;
private:
    aeron_clock_cache_t m_cached_clock = {};
    aeron_counters_manager_t m_counters_manager = {};
    aeron_system_counters_t m_system_counters = {};
    AERON_DECL_ALIGNED(buffer_t m_counter_value_buffer, 16) = {};
    AERON_DECL_ALIGNED(buffer_4x_t m_counter_meta_buffer, 16) = {};
    std::vector<aeron_ipc_publication_t *> m_publications;
};

TEST_F(IpcPublicationTest, shouldCreatePublication)
{
    auto channel = "aeron:ipc|alias=test|mtu=2048";
    auto channel_length = strlen(channel);

    aeron_ipc_publication_t *publication = createPublication(channel);

    ASSERT_NE(nullptr, publication) << aeron_errmsg();
    EXPECT_EQ(static_cast<int32_t>(channel_length), publication->channel_length);
    EXPECT_EQ(0, strncmp(channel, publication->channel, channel_length));
    EXPECT_FALSE(publication->is_exclusive);
}

TEST_F(IpcPublicationTest, shouldReturnStorageSpaceErrorIfNotEnoughStorageSpaceAvailable)
{
    m_context->usable_fs_space_func = noSpaceAvailable;
    aeron_ipc_publication_t *publication = createPublication("aeron:ipc");
    ASSERT_EQ(nullptr, publication) << aeron_errmsg();
    EXPECT_EQ(-AERON_ERROR_CODE_STORAGE_SPACE, aeron_errcode());
}
