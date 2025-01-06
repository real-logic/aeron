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

#include <exception>
#include <functional>
#include <string>

#include <gtest/gtest.h>

#include "aeron_client_test_utils.h"

extern "C"
{
#include "aeron_subscription.h"
#include "aeron_image.h"
}

#define FILE_PAGE_SIZE (4 * 1024)

#define SUB_URI "aeron:udp?endpoint=localhost:24567"
#define STREAM_ID (101)
#define SESSION_ID (110)
#define REGISTRATION_ID (27)
#define CHANNEL_STATUS_INDICATOR_ID (45)
#define SUBSCRIBER_POSITION_ID (49)

using namespace aeron::test;

class SubscriptionTest : public testing::Test
{
public:
    SubscriptionTest() :
        m_conductor(nullptr),
        m_subscription(createSubscription(m_conductor, &m_channel_status))
    {
    }

    ~SubscriptionTest() override
    {
        if (nullptr != m_subscription)
        {
            aeron_subscription_delete(m_subscription);
        }

        for (auto &filename : m_filenames)
        {
            ::unlink(filename.c_str());
        }
    }

    static aeron_subscription_t *createSubscription(aeron_client_conductor_t *conductor, int64_t *channel_status)
    {
        aeron_subscription_t *subscription = nullptr;

        if (aeron_subscription_create(
            &subscription,
            conductor,
            ::strdup(SUB_URI),
            STREAM_ID,
            REGISTRATION_ID,
            CHANNEL_STATUS_INDICATOR_ID,
            channel_status,
            nullptr,
            nullptr,
            nullptr,
            nullptr) < 0)
        {
            throw std::runtime_error("could not create subscription: " + std::string(aeron_errmsg()));
        }

        return subscription;
    }

    int64_t createImage(int64_t *sub_pos)
    {
        aeron_image_t *image = nullptr;
        aeron_log_buffer_t *log_buffer = nullptr;
        std::string filename = tempFileName();

        createLogFile(filename);

        if (aeron_log_buffer_create(&log_buffer, filename.c_str(), m_correlationId, false) < 0)
        {
            throw std::runtime_error("could not create log_buffer: " + std::string(aeron_errmsg()));
        }

        if (aeron_image_create(
            &image,
            m_subscription,
            m_conductor,
            log_buffer,
            SUBSCRIBER_POSITION_ID,
            sub_pos,
            m_correlationId,
            (int32_t)m_correlationId,
            "none",
            strlen("none")) < 0)
        {
            throw std::runtime_error("could not create image: " + std::string(aeron_errmsg()));
        }

        m_imageMap.insert(std::pair<int64_t, aeron_image_t *>(m_correlationId, image));
        m_filenames.emplace_back(filename);

        return m_correlationId++;
    }

    static void null_fragment_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
    {
    }

protected:
    aeron_client_conductor_t *m_conductor = nullptr;
    aeron_subscription_t *m_subscription = nullptr;
    int64_t m_channel_status = AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ACTIVE;
    int64_t m_sub_pos = 0;

    int64_t m_correlationId = 0;

    std::map<int64_t, aeron_image_t *> m_imageMap;
    std::vector<std::string> m_filenames;
};

TEST_F(SubscriptionTest, shouldInitAndDelete)
{
}

TEST_F(SubscriptionTest, shouldAddAndRemoveImageWithoutPoll)
{
    int64_t image_id = createImage(&m_sub_pos);
    aeron_image_t *image = m_imageMap.find(image_id)->second;

    ASSERT_EQ(aeron_client_conductor_subscription_add_image(m_subscription, image), 0);
    EXPECT_EQ(aeron_subscription_image_count(m_subscription), 1);

    ASSERT_EQ(aeron_client_conductor_subscription_remove_image(m_subscription, image), 0);
    EXPECT_EQ(aeron_subscription_image_count(m_subscription), 0);

    EXPECT_EQ(aeron_client_conductor_subscription_prune_image_lists(m_subscription), 0);
    EXPECT_TRUE(
        aeron_image_is_in_use_by_subscription(image, aeron_subscription_last_image_list_change_number(m_subscription)));

    aeron_log_buffer_delete(image->log_buffer);
    aeron_image_delete(image);
}

TEST_F(SubscriptionTest, shouldAddAndRemoveImageWithPollAfter)
{
    int64_t image_id = createImage(&m_sub_pos);
    aeron_image_t *image = m_imageMap.find(image_id)->second;

    ASSERT_EQ(aeron_client_conductor_subscription_add_image(m_subscription, image), 0);
    ASSERT_EQ(aeron_client_conductor_subscription_remove_image(m_subscription, image), 0);
    ASSERT_EQ(aeron_subscription_poll(m_subscription, null_fragment_handler, this, 1), 0);

    EXPECT_EQ(aeron_subscription_image_count(m_subscription), 0);
    EXPECT_EQ(aeron_client_conductor_subscription_prune_image_lists(m_subscription), 2);
    EXPECT_FALSE(
        aeron_image_is_in_use_by_subscription(image, aeron_subscription_last_image_list_change_number(m_subscription)));

    aeron_log_buffer_delete(image->log_buffer);
    aeron_image_delete(image);
}

TEST_F(SubscriptionTest, shouldAddAndRemoveImageWithPollBetween)
{
    int64_t image_id = createImage(&m_sub_pos);
    aeron_image_t *image = m_imageMap.find(image_id)->second;

    ASSERT_EQ(aeron_client_conductor_subscription_add_image(m_subscription, image), 0);
    ASSERT_EQ(aeron_subscription_poll(m_subscription, null_fragment_handler, this, 1), 0);
    ASSERT_EQ(aeron_client_conductor_subscription_remove_image(m_subscription, image), 0);

    EXPECT_EQ(aeron_subscription_image_count(m_subscription), 0);
    EXPECT_EQ(aeron_client_conductor_subscription_prune_image_lists(m_subscription), 1);
    EXPECT_TRUE(
        aeron_image_is_in_use_by_subscription(image, aeron_subscription_last_image_list_change_number(m_subscription)));

    aeron_log_buffer_delete(image->log_buffer);
    aeron_image_delete(image);
}

TEST_F(SubscriptionTest, shouldFetchConstants)
{
    aeron_subscription_constants_t constants;
    ASSERT_EQ(0, aeron_subscription_constants(m_subscription, &constants));
    ASSERT_NE(0, constants.channel_status_indicator_id);
}
