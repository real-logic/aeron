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

#include <functional>
#include <utility>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "aeron_test_base.h"

extern "C"
{
#include "concurrent/aeron_thread.h"
#include "aeron_system_counters.h"
#include "command/aeron_control_protocol.h"
}

#define PUB_URI "aeron:udp?endpoint=localhost:24325"
#define STREAM_ID (117)

class ErrorCallbackValidation
{
public:
    explicit ErrorCallbackValidation(std::vector<std::string> expectedSubstrings) : 
        m_expectedSubstrings(std::move(expectedSubstrings))
    {}

    void reset()
    {
        m_observations.clear();
    }

    void validate(std::string &errorStr) 
    {
        m_observations.push_back(errorStr);

        for (auto &subString : m_expectedSubstrings)
        {
            if (std::string::npos == errorStr.find(subString))
            {
                return;
            }
        }
        
        m_validated = true;
    }
    
    bool validated() const
    {
        return m_validated;
    }

    friend std::ostream& operator<<(std::ostream& os, const ErrorCallbackValidation& bar)
    {
        os << "Unable to find: ";
        os << "{ ";
        for (auto &subString : bar.m_expectedSubstrings)
        {
            os << subString << "; ";
        }
        os << "} in:";
        os << std::endl;
        for (auto &subString : bar.m_observations)
        {
            os << subString;
        }

        return os;
    }

private:
    std::vector<std::string> m_expectedSubstrings;
    std::vector<std::string> m_observations{};
    bool m_validated = false;
};

const char *CSV_NAME_CONFIG_WITH_UNRESOLVABLE_ADDRESS = "server0,endpoint,foo.example.com:24326,localhost:24326|";

class CErrorsTest : public CSystemTestBase, public testing::Test
{
public:
    CErrorsTest() : CSystemTestBase(
        std::vector<std::pair<std::string, std::string>>{
            { "AERON_COUNTERS_BUFFER_LENGTH", "32768" },
            { "AERON_NAME_RESOLVER_SUPPLIER", "csv_table" },
            { "AERON_NAME_RESOLVER_INIT_ARGS", CSV_NAME_CONFIG_WITH_UNRESOLVABLE_ADDRESS }
        })
    {
    }

protected:
    std::int64_t *m_errorCounter = NULL;
    std::int64_t m_initialErrorCount = 0;
    aeron_counters_reader_t *m_countersReader = NULL;

    aeron_t *connect() override
    {
        aeron_t *aeron = CSystemTestBase::connect();

        m_countersReader = aeron_counters_reader(aeron);
        m_errorCounter = aeron_counters_reader_addr(m_countersReader, AERON_SYSTEM_COUNTER_ERRORS);
        AERON_GET_VOLATILE(m_initialErrorCount, *m_errorCounter);

        return aeron;
    }

    void waitForErrorCounterIncrease()
    {
        int64_t currentErrorCount;
        do
        {
            proc_yield();
            AERON_GET_VOLATILE(currentErrorCount, *m_errorCounter);
        }
        while (currentErrorCount <= m_initialErrorCount);
    }
    
    void verifyDistinctErrorLogContains(const char *text, std::int64_t timeoutMs = 0)
    {
        aeron_cnc_t *aeronCnc;
        int result = aeron_cnc_init(&aeronCnc, aeron_context_get_dir(m_context), 100);
        EXPECT_EQ(0, result);
        if (result < 0)
        {
            aeron_cnc_close(aeronCnc);
            return;
        }

        ErrorCallbackValidation errorCallbackValidation
            {
                std::vector<std::string>{ text }
            };


        std::int64_t deadlineMs = aeron_epoch_clock() + timeoutMs;
        do
        {
            std::this_thread::yield();
            errorCallbackValidation.reset();
            aeron_cnc_error_log_read(aeronCnc, errorCallback, &errorCallbackValidation, 0);
        }
        while (!errorCallbackValidation.validated() && aeron_epoch_clock() <= deadlineMs);

        EXPECT_TRUE(errorCallbackValidation.validated()) << errorCallbackValidation;
        
        aeron_cnc_close(aeronCnc);
    }

    static void errorCallback(
        int32_t observation_count,
        int64_t first_observation_timestamp,
        int64_t last_observation_timestamp,
        const char *error,
        size_t error_length,
        void *clientd)
    {
        auto *callbackValidation = reinterpret_cast<ErrorCallbackValidation *>(clientd);
        std::string errorStr = std::string(error, error_length);
        callbackValidation->validate(errorStr);
    }

};

TEST_F(CErrorsTest, shouldErrorOnAddCounterPoll)
{
    ASSERT_EQ(-1, aeron_async_add_counter_poll(NULL, NULL));
    std::string errorMessage = std::string(aeron_errmsg());
    ASSERT_THAT(errorMessage, testing::HasSubstr("Invalid argument"));
    ASSERT_THAT(errorMessage, testing::HasSubstr("Parameters must not be null"));
}

TEST_F(CErrorsTest, shouldValidatePollType)
{
    aeron_t *aeron = connect();
    aeron_async_add_publication_t *pub_async;
    aeron_publication_t *pub;
    aeron_counter_t *counter;

    ASSERT_EQ(0, aeron_async_add_publication(&pub_async, aeron, "aeron:ipc", 1001));
    ASSERT_EQ(-1, aeron_async_add_counter_poll(&counter, (aeron_async_add_counter_t *)pub_async)) << aeron_errmsg();
    std::string errorMessage = std::string(aeron_errmsg());
    ASSERT_THAT(errorMessage, testing::HasSubstr("Invalid argument"));
    ASSERT_THAT(errorMessage, testing::HasSubstr("Parameters must be valid, async->type"));

    while (1 != aeron_async_add_publication_poll(&pub, pub_async))
    {
        proc_yield();
    }

    aeron_publication_close(pub, NULL, NULL);
}

TEST_F(CErrorsTest, publicationErrorIncludesClientAndDriverErrorAndReportsInDistinctLog)
{
    aeron_t *aeron = connect();
    aeron_async_add_publication_t *pub_async;
    aeron_publication_t *pub;

    ASSERT_EQ(0, aeron_async_add_publication(&pub_async, aeron, "aeron:tcp?endpoint=localhost:21345", 1001));

    int result;
    while (0 == (result = aeron_async_add_publication_poll(&pub, pub_async)))
    {
        proc_yield();
    }

    ASSERT_EQ(-1, result);
    std::string errorMessage = std::string(aeron_errmsg());
    const char *expectedDriverMessage = "invalid URI scheme or transport: aeron:tcp?endpoint=localhost:21345";

    ASSERT_THAT(-AERON_ERROR_CODE_INVALID_CHANNEL, aeron_errcode());
    ASSERT_THAT(
        errorMessage, testing::HasSubstr("async_add_publication registration"));
    ASSERT_THAT(
        errorMessage, testing::HasSubstr(expectedDriverMessage));

    waitForErrorCounterIncrease();
    verifyDistinctErrorLogContains(expectedDriverMessage);
}

TEST_F(CErrorsTest, exclusivePublicationErrorIncludesClientAndDriverErrorAndReportsInDistinctLog)
{
    aeron_t *aeron = connect();
    aeron_async_add_exclusive_publication_t *pub_async;
    aeron_exclusive_publication_t *pub;

    ASSERT_EQ(0, aeron_async_add_exclusive_publication(&pub_async, aeron, "aeron:tcp?endpoint=localhost:21345", 1001));

    int result;
    while (0 == (result = aeron_async_add_exclusive_publication_poll(&pub, pub_async)))
    {
        proc_yield();
    }

    ASSERT_EQ(-1, result);
    std::string errorMessage = std::string(aeron_errmsg());
    const char *expectedDriverMessage = "invalid URI scheme or transport: aeron:tcp?endpoint=localhost:21345";

    ASSERT_THAT(-AERON_ERROR_CODE_INVALID_CHANNEL, aeron_errcode());
    ASSERT_THAT(
        errorMessage, testing::HasSubstr("async_add_exclusive_publication registration"));
    ASSERT_THAT(
        errorMessage, testing::HasSubstr(expectedDriverMessage));

    waitForErrorCounterIncrease();
    verifyDistinctErrorLogContains(expectedDriverMessage);
}

TEST_F(CErrorsTest, subscriptionErrorIncludesClientAndDriverErrorAndReportsInDistinctLog)
{
    aeron_t *aeron = connect();
    aeron_async_add_subscription_t *sub_async;
    aeron_subscription_t *sub;

    ASSERT_EQ(0, aeron_async_add_subscription(
        &sub_async, aeron, "aeron:tcp?endpoint=localhost:21345", 1001, NULL, NULL, NULL, NULL));

    int result;
    while (0 == (result = aeron_async_add_subscription_poll(&sub, sub_async)))
    {
        proc_yield();
    }

    ASSERT_EQ(-1, result) << aeron_errmsg();
    std::string errorMessage = std::string(aeron_errmsg());
    const char *expectedDriverMessage = "invalid URI scheme or transport: aeron:tcp?endpoint=localhost:21345";

    ASSERT_THAT(-AERON_ERROR_CODE_INVALID_CHANNEL, aeron_errcode());
    ASSERT_THAT(
        errorMessage, testing::HasSubstr("async_add_subscription registration"));
    ASSERT_THAT(
        errorMessage, testing::HasSubstr(expectedDriverMessage));

    waitForErrorCounterIncrease();
    verifyDistinctErrorLogContains(expectedDriverMessage);
}

TEST_F(CErrorsTest, counterErrorIncludesClientAndDriverErrorAndReportsInDistinctLog)
{
    aeron_t *aeron = connect();
    aeron_async_add_counter_t *counter_async;
    aeron_counter_t *counter;

    int32_t key = 1000000;

    int result;
    do
    {
        ASSERT_EQ(0,
            aeron_async_add_counter(&counter_async, aeron, 2002, (const uint8_t *)&key, sizeof(key), "label", 5));
        while (0 == (result = aeron_async_add_counter_poll(&counter, counter_async)))
        {
            proc_yield();
        }

        if (result < 0)
        {
            break;
        }

        key++;
    }
    while (true);

    ASSERT_EQ(-1, result);
    std::string errorMessage = std::string(aeron_errmsg());
    const char *expectedDriverMessage = "Unable to allocate counter: type: 2002, label: label";

    ASSERT_THAT(-AERON_ERROR_CODE_GENERIC_ERROR, aeron_errcode());
    ASSERT_THAT(
        errorMessage, testing::HasSubstr("async_add_counter registration"));
    ASSERT_THAT(
        errorMessage, testing::HasSubstr(expectedDriverMessage));

    waitForErrorCounterIncrease();
    verifyDistinctErrorLogContains(expectedDriverMessage);
}

TEST_F(CErrorsTest, destinationErrorIncludesClientAndDriverErrorAndReportsInDistinctLog)
{
    aeron_t *aeron = connect();
    aeron_async_add_exclusive_publication_t *pub_async;
    aeron_async_destination_t *dest_async;
    aeron_exclusive_publication_t *pub;

    ASSERT_EQ(0, aeron_async_add_exclusive_publication(&pub_async, aeron, "aeron:udp?control-mode=manual", 1001));

    int result;
    while (0 == (result = aeron_async_add_exclusive_publication_poll(&pub, pub_async)))
    {
        proc_yield();
    }

    ASSERT_EQ(1, result) << aeron_errmsg();

    ASSERT_EQ(0, aeron_exclusive_publication_async_add_destination(
        &dest_async, aeron, pub, "aeron:tcp?endpoint=localhost:21345"));

    while (0 == (result = aeron_exclusive_publication_async_destination_poll(dest_async)))
    {
        proc_yield();
    }

    ASSERT_EQ(-1, result);
    std::string errorMessage = std::string(aeron_errmsg());

    const char *expectedDriverMessage = "invalid URI scheme or transport: aeron:tcp?endpoint=localhost:21345";

    ASSERT_THAT(-AERON_ERROR_CODE_INVALID_CHANNEL, aeron_errcode());
    ASSERT_THAT(
        errorMessage, testing::HasSubstr("async_add_destination registration"));
    ASSERT_THAT(
        errorMessage, testing::HasSubstr(expectedDriverMessage));

    waitForErrorCounterIncrease();
    verifyDistinctErrorLogContains(expectedDriverMessage);
}


TEST_F(CErrorsTest, shouldFailToResovleNameOnPublication)
{
    aeron_t *aeron = connect();
    aeron_async_add_publication_t *pub_async;
    aeron_publication_t *pub;

    ASSERT_EQ(0, aeron_async_add_publication(&pub_async, aeron, "aeron:udp?endpoint=foo.example.com:20202", 1001));

    int result;
    while (0 == (result = aeron_async_add_publication_poll(&pub, pub_async)))
    {
        proc_yield();
    }

    ASSERT_EQ(-1, result);
    std::string errorMessage = std::string(aeron_errmsg());
    const char *expectedDriverMessage = "Unable to resolve host";

    ASSERT_THAT(-AERON_ERROR_CODE_UNKNOWN_HOST, aeron_errcode());
    ASSERT_THAT(
        errorMessage, testing::HasSubstr("async_add_publication registration"));
    ASSERT_THAT(
        errorMessage, testing::HasSubstr(expectedDriverMessage));

    waitForErrorCounterIncrease();
    verifyDistinctErrorLogContains(expectedDriverMessage);
}

TEST_F(CErrorsTest, shouldFailToResovleNameOnDestination)
{
    aeron_t *aeron = connect();
    aeron_async_add_exclusive_publication_t *pub_async;
    aeron_async_destination_t *dest_async;
    aeron_exclusive_publication_t *pub;

    ASSERT_EQ(0, aeron_async_add_exclusive_publication(&pub_async, aeron, "aeron:udp?control-mode=manual", 1001));

    int result;
    while (0 == (result = aeron_async_add_exclusive_publication_poll(&pub, pub_async)))
    {
        proc_yield();
    }

    ASSERT_EQ(1, result) << aeron_errmsg();

    ASSERT_EQ(0, aeron_exclusive_publication_async_add_destination(
        &dest_async, aeron, pub, "aeron:udp?endpoint=foo.example.com:21345"));

    while (0 == (result = aeron_exclusive_publication_async_destination_poll(dest_async)))
    {
        proc_yield();
    }

    ASSERT_EQ(-1, result);
    std::string errorMessage = std::string(aeron_errmsg());
    const char *expectedDriverMessage = "Unable to resolve host";

    ASSERT_THAT(-AERON_ERROR_CODE_UNKNOWN_HOST, aeron_errcode());
    ASSERT_THAT(
        errorMessage, testing::HasSubstr("async_add_destination registration"));
    ASSERT_THAT(
        errorMessage, testing::HasSubstr(expectedDriverMessage));

    waitForErrorCounterIncrease();
    verifyDistinctErrorLogContains(expectedDriverMessage);
}

TEST_F(CErrorsTest, shouldRecordDistinctErrorCorrectlyOnReresolve)
{
    aeron_t *aeron = connect();

    aeron_async_add_publication_t *pub_async;
    aeron_publication_t *pub;

    ASSERT_EQ(0, aeron_async_add_publication(&pub_async, aeron, "aeron:udp?endpoint=server0", 1001));

    int result;
    while (0 == (result = aeron_async_add_publication_poll(&pub, pub_async)))
    {
        proc_yield();
    }

    ASSERT_EQ(1, result);
    const char *expectedDriverMessage = "Unable to resolve host";

    waitForErrorCounterIncrease();
    verifyDistinctErrorLogContains(expectedDriverMessage, 10000);
}