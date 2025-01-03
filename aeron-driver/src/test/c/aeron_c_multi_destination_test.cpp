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

#include <functional>

#include <gtest/gtest.h>

#include "aeron_test_base.h"

extern "C"
{
#include "concurrent/aeron_atomic.h"
#include "agent/aeron_driver_agent.h"
#include "aeron_driver_context.h"
}

#define PUB_URI_1 "aeron:udp?endpoint=localhost:24324"
#define PUB_URI_2 "aeron:udp?endpoint=localhost:24325"
#define CONTROL_URI "aeron:udp?control=localhost:24326"
#define MDC_URI (CONTROL_URI "|control-mode=dynamic")
#define MDC_DEST_URI (CONTROL_URI "|endpoint=localhost:24327")
#define MDS_URI "aeron:udp?control-mode=manual"
#define STREAM_ID (117)

class CMultiDestinationTest : public CSystemTestBase, public testing::Test
{
protected:
    CMultiDestinationTest() : CSystemTestBase(
        std::vector<std::pair<std::string, std::string>>
            {
            })
    {
    }
};

static void mdsParameters(
    aeron_exclusive_publication_t *pub,
    std::int32_t *initialTermId,
    std::int32_t *termId,
    std::int32_t *termOffset,
    std::int32_t *sessionId)
{
    aeron_publication_constants_t pubConstants = {};
    aeron_exclusive_publication_constants(pub, &pubConstants);

    int64_t position = aeron_exclusive_publication_position(pub);
    *initialTermId = pubConstants.initial_term_id;
    *termId = aeron_logbuffer_compute_term_id_from_position(
        position, pubConstants.position_bits_to_shift, pubConstants.initial_term_id);
    *termOffset = (int32_t)(position & (pubConstants.term_buffer_length - 1));
    *sessionId = pubConstants.session_id;
}

TEST_F(CMultiDestinationTest, shouldAddTwoPublicationDestinationsForMds)
{
    aeron_async_add_subscription_t *async_sub = nullptr;

    ASSERT_TRUE(connect());

    aeron_async_add_exclusive_publication_t *async_pub1 = nullptr;
    ASSERT_EQ(aeron_async_add_exclusive_publication(&async_pub1, m_aeron, PUB_URI_1, STREAM_ID), 0);
    aeron_exclusive_publication_t *pub1 = awaitExclusivePublicationOrError(async_pub1);
    ASSERT_TRUE(pub1) << aeron_errmsg();

    std::int32_t initialTermId;
    std::int32_t termId;
    std::int32_t termOffset;
    std::int32_t sessionId;
    mdsParameters(pub1, &initialTermId, &termId, &termOffset, &sessionId);

    auto pubUri2 = std::string(PUB_URI_2)
        .append("|init-term-id=").append(std::to_string(initialTermId))
        .append("|term-id=").append(std::to_string(termId))
        .append("|term-offset=").append(std::to_string(termOffset))
        .append("|session-id=").append(std::to_string(sessionId));

    aeron_async_add_exclusive_publication_t *async_pub2 = nullptr;
    ASSERT_EQ(0, aeron_async_add_exclusive_publication(&async_pub2, m_aeron, pubUri2.c_str(), STREAM_ID));
    aeron_exclusive_publication_t *pub2 = awaitExclusivePublicationOrError(async_pub2);
    ASSERT_TRUE(pub2) << aeron_errmsg();


    ASSERT_EQ(0, aeron_async_add_subscription(
        &async_sub, m_aeron, MDS_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr));
    aeron_subscription_t *subscription = awaitSubscriptionOrError(async_sub);
    ASSERT_TRUE(subscription) << aeron_errmsg();

    aeron_async_destination_t *dest1;
    aeron_subscription_async_add_destination(&dest1, m_aeron, subscription, PUB_URI_1);
    ASSERT_TRUE(awaitDestinationOrError(dest1)) << aeron_errmsg();

    aeron_async_destination_t *dest2;
    aeron_subscription_async_add_destination(&dest2, m_aeron, subscription, PUB_URI_2);
    ASSERT_TRUE(awaitDestinationOrError(dest2)) << aeron_errmsg();

    awaitConnected(subscription);

    EXPECT_EQ(aeron_exclusive_publication_close(pub1, nullptr, nullptr), 0);
    EXPECT_EQ(aeron_exclusive_publication_close(pub2, nullptr, nullptr), 0);
    EXPECT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
}

TEST_F(CMultiDestinationTest, shouldAddPublicationAndMdcPublicationForMds)
{
    aeron_async_add_subscription_t *async_sub = nullptr;

    ASSERT_TRUE(connect());

    aeron_async_add_exclusive_publication_t *async_pub1 = nullptr;
    ASSERT_EQ(aeron_async_add_exclusive_publication(&async_pub1, m_aeron, MDC_URI, STREAM_ID), 0);
    aeron_exclusive_publication_t *pub1 = awaitExclusivePublicationOrError(async_pub1);
    ASSERT_TRUE(pub1) << aeron_errmsg();

    std::int32_t initialTermId;
    std::int32_t termId;
    std::int32_t termOffset;
    std::int32_t sessionId;
    mdsParameters(pub1, &initialTermId, &termId, &termOffset, &sessionId);

    auto pubUri2 = std::string(PUB_URI_2)
        .append("|init-term-id=").append(std::to_string(initialTermId))
        .append("|term-id=").append(std::to_string(termId))
        .append("|term-offset=").append(std::to_string(termOffset))
        .append("|session-id=").append(std::to_string(sessionId));

    aeron_async_add_exclusive_publication_t *async_pub2 = nullptr;
    ASSERT_EQ(0, aeron_async_add_exclusive_publication(&async_pub2, m_aeron, pubUri2.c_str(), STREAM_ID));
    aeron_exclusive_publication_t *pub2 = awaitExclusivePublicationOrError(async_pub2);
    ASSERT_TRUE(pub2) << aeron_errmsg();


    ASSERT_EQ(0, aeron_async_add_subscription(
        &async_sub, m_aeron, MDS_URI, STREAM_ID, nullptr, nullptr, nullptr, nullptr));
    aeron_subscription_t *subscription = awaitSubscriptionOrError(async_sub);
    ASSERT_TRUE(subscription) << aeron_errmsg();

    aeron_async_destination_t *dest1;
    aeron_subscription_async_add_destination(&dest1, m_aeron, subscription, MDC_DEST_URI);
    ASSERT_TRUE(awaitDestinationOrError(dest1)) << aeron_errmsg();

    aeron_async_destination_t *dest2;
    aeron_subscription_async_add_destination(&dest2, m_aeron, subscription, PUB_URI_2);
    ASSERT_TRUE(awaitDestinationOrError(dest2)) << aeron_errmsg();

    awaitConnected(subscription);

    EXPECT_EQ(aeron_exclusive_publication_close(pub1, nullptr, nullptr), 0);
    EXPECT_EQ(aeron_exclusive_publication_close(pub2, nullptr, nullptr), 0);
    EXPECT_EQ(aeron_subscription_close(subscription, nullptr, nullptr), 0);
}

TEST_F(CMultiDestinationTest, shouldNotAllowChannelsWithControlButWithoutEndpointOrControlMode)
{
    ASSERT_TRUE(connect());

    aeron_async_add_exclusive_publication_t *async_pub1 = nullptr;
    ASSERT_EQ(aeron_async_add_exclusive_publication(&async_pub1, m_aeron, CONTROL_URI, STREAM_ID), 0);
    aeron_exclusive_publication_t *pub1 = awaitExclusivePublicationOrError(async_pub1);
    ASSERT_EQ(nullptr, pub1);
}

TEST_F(CMultiDestinationTest, shouldNotAllowChannelsWithControlModeDynamicButWithoutControl)
{
    ASSERT_TRUE(connect());

    aeron_async_add_exclusive_publication_t *async_pub1 = nullptr;
    ASSERT_EQ(aeron_async_add_exclusive_publication(
        &async_pub1, m_aeron, "aeron:udp?control-mode=dynamic", STREAM_ID), 0);
    aeron_exclusive_publication_t *pub1 = awaitExclusivePublicationOrError(async_pub1);
    ASSERT_EQ(nullptr, pub1);
}
