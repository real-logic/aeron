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

using namespace aeron::test;

class SubscriptionTest : public testing::Test
{
public:
    SubscriptionTest() :
        m_conductor(NULL),
        m_subscription(createSubscription(m_conductor, &m_channel_status))
    {
    }

    virtual ~SubscriptionTest()
    {
        if (NULL != m_subscription)
        {
            aeron_subscription_delete(m_subscription);
        }

        std::for_each(m_filenames.begin(), m_filenames.end(),
            [&](std::string &filename)
            {
                ::unlink(filename.c_str());
            });
    }

    static aeron_subscription_t *createSubscription(aeron_client_conductor_t *conductor, int64_t *channel_status)
    {
        aeron_subscription_t *subscription = NULL;

        if (aeron_subscription_create(
            &subscription,
            conductor,
            ::strdup(SUB_URI),
            STREAM_ID,
            REGISTRATION_ID,
            channel_status,
            NULL,
            NULL,
            NULL,
            NULL) < 0)
        {
            throw std::runtime_error("could not create subscription: %s" + std::string(aeron_errmsg()));
        }

        return subscription;
    }

    int64_t createImage(int64_t *subscriber_position)
    {
        aeron_image_t *image = NULL;
        aeron_log_buffer_t *log_buffer = NULL;
        std::string filename = tempFileName();

        createLogFile(filename);

        if (aeron_log_buffer_create(&log_buffer, filename.c_str(), m_correlationId, false) < 0)
        {
            throw std::runtime_error("could not create log_buffer: %s" + std::string(aeron_errmsg()));
        }

        if (aeron_image_create(
            &image, m_conductor, log_buffer, subscriber_position, m_correlationId, (int32_t)m_correlationId) < 0)
        {
            throw std::runtime_error("could not create image: %s" + std::string(aeron_errmsg()));
        }

        m_imageMap.insert(std::pair<int64_t, aeron_image_t *>(m_correlationId, image));
        m_filenames.emplace_back(filename);

        return m_correlationId++;
    }

protected:
    aeron_client_conductor_t *m_conductor = NULL;
    aeron_subscription_t *m_subscription = NULL;
    int64_t m_channel_status = AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ACTIVE;

    int64_t m_correlationId = 0;

    std::map<int64_t, aeron_image_t *> m_imageMap;
    std::vector<std::string> m_filenames;
};

TEST_F(SubscriptionTest, shouldInitAndDelete)
{
}
