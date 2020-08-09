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

#include <gtest/gtest.h>

#include "EmbeddedMediaDriver.h"
#include "aeronc.h"

extern "C"
{
#include "util/aeron_fileutil.h"
#include "aeron_context.h"
#include "aeron_client.h"
}

#ifdef _MSC_VER
#define AERON_FILE_SEP '\\'
#else
#define AERON_FILE_SEP '/'
#endif

using namespace aeron;

class TerminateTest : public testing::Test
{
public:
    TerminateTest()
    {
    }

    ~TerminateTest() override
    {
        if (m_aeron)
        {
            aeron_close(m_aeron);
        }

        if (m_context)
        {
            aeron_context_close(m_context);
        }
    }

    aeron_t *connect()
    {
        if (aeron_context_init(&m_context) < 0)
        {
            throw std::runtime_error(aeron_errmsg());
        }

        if (aeron_init(&m_aeron, m_context) < 0)
        {
            throw std::runtime_error(aeron_errmsg());
        }

        if (aeron_start(m_aeron) < 0)
        {
            throw std::runtime_error(aeron_errmsg());
        }

        return m_aeron;
    }


protected:
    aeron_context_t *m_context = nullptr;
    aeron_t *m_aeron = nullptr;
};

TEST_F(TerminateTest, shouldShutdownDriver)
{
    EmbeddedMediaDriver driver;
    driver.start();

    aeron_t *aeron = connect();
    const char *dir = aeron_context_get_dir(aeron->context);

    char path[1024];
    snprintf(path, sizeof(path), "%s%c%s", dir, AERON_FILE_SEP, AERON_CNC_FILE);

    ASSERT_EQ(1, aeron_context_request_driver_termination(
        aeron_context_get_dir(m_context), (uint8_t *)TERMINATION_KEY, strlen(TERMINATION_KEY)));

    driver.joinAndClose();

    struct stat sb;
    while (0 == stat(path, &sb))
    {
        sched_yield();
    }
}