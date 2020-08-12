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

extern "C"
{
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
    TerminateTest() = default;
};

TEST_F(TerminateTest, shouldShutdownDriver)
{
    EmbeddedMediaDriver driver;
    driver.start();

    char path[1024];
    snprintf(path, sizeof(path), "%s%c%s", driver.directory(), AERON_FILE_SEP, AERON_CNC_FILE);

    ASSERT_EQ(1, aeron_context_request_driver_termination(
            driver.directory(), (uint8_t *)TERMINATION_KEY, strlen(TERMINATION_KEY))) << aeron_errmsg();

    driver.joinAndClose();
}