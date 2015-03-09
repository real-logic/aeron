/*
 * Copyright 2014 - 2015 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <cstdint>
#include <array>
#include <vector>
#include <map>
#include <thread>
#include <string>

#include <gtest/gtest.h>

#include <concurrent/AtomicBuffer.h>
#include <concurrent/CountersManager.h>
#include <util/Exceptions.h>

using namespace aeron::common::concurrent;
using namespace aeron::common::util;

static const std::int32_t NUMCOUNTERS = 4;
static std::array<std::uint8_t, NUMCOUNTERS * CountersManager::LABEL_SIZE> labelsBuffer;
static std::array<std::uint8_t, NUMCOUNTERS * CountersManager::COUNTER_SIZE> countersBuffer;

static void clearBuffers()
{
    labelsBuffer.fill(0);
    countersBuffer.fill(0);
}

TEST(testCountersManager, checkEmpty)
{
    clearBuffers();
    CountersManager cm(AtomicBuffer(&labelsBuffer[0], labelsBuffer.size()),
            AtomicBuffer(&countersBuffer[0], countersBuffer.size()));

    cm.forEach([](int id, const std::string &label)
    {
        FAIL();
    });
}

TEST(testCountersManager, checkOverflow)
{
    clearBuffers();
    CountersManager cm (AtomicBuffer(&labelsBuffer[0], labelsBuffer.size()),
            AtomicBuffer(&countersBuffer[0], countersBuffer.size()));

    std::vector<std::string> labels = { "lab0", "lab1", "lab2", "lab3", "lab4" };

    ASSERT_THROW({
        for (auto& l: labels)
        {
            cm.allocate(l);
        }
    }, IllegalArgumentException);
}

TEST(testCountersManager, checkAlloc)
{
    clearBuffers();
    CountersManager cm (AtomicBuffer(&labelsBuffer[0], labelsBuffer.size()),
                        AtomicBuffer(&countersBuffer[0], countersBuffer.size()));

    std::vector<std::string> labels = { "lab0", "lab1", "lab2", "lab3"};
    std::map<std::int32_t, std::string> allocated;

    ASSERT_NO_THROW({
        for (auto& l: labels)
        {
            allocated[cm.allocate(l)] = l;
        }
    });

    ASSERT_NO_THROW({
        cm.forEach([&](int id, const std::string &label)
        {
            ASSERT_EQ(label, allocated[id]);
            allocated.erase(allocated.find(id));
        });
    });

    ASSERT_EQ(allocated.empty(), true);
}

TEST(testCountersManager, checkRecycle)
{
    clearBuffers();
    CountersManager cm (AtomicBuffer(&labelsBuffer[0], labelsBuffer.size()),
            AtomicBuffer(&countersBuffer[0], countersBuffer.size()));

    std::vector<std::string> labels = { "lab0", "lab1", "lab2", "lab3"};

    ASSERT_NO_THROW({
        for (auto& l: labels)
        {
            cm.allocate(l);
        }
    });

    ASSERT_NO_THROW({
        cm.free(2);
        ASSERT_EQ(cm.allocate("newLab2"), 2);
    });
}

TEST(testCountersManager, checkInvalidFree)
{
    clearBuffers();
    CountersManager cm (AtomicBuffer(&labelsBuffer[0], labelsBuffer.size()),
            AtomicBuffer(&countersBuffer[0], countersBuffer.size()));

    std::vector<std::string> labels = { "lab0", "lab1", "lab2", "lab3"};

    ASSERT_THROW({
        cm.free(2);
    }, IllegalArgumentException);

    ASSERT_NO_THROW({
        for (auto& l: labels)
        {
            cm.allocate(l);
        }
    });

    ASSERT_THROW({
        cm.free(2);
        cm.free(2);
    }, IllegalArgumentException);
}
