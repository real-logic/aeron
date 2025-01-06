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

#include <array>
#include <thread>
#include <atomic>
#include <vector>

#include <gtest/gtest.h>

#include "concurrent/AtomicArrayUpdater.h"

using namespace aeron;
using namespace aeron::concurrent;

#define NUM_ELEMENTS (10000)
#define NUM_ITERATIONS (10)

class AtomicArrayUpdaterTest : public testing::Test
{
};

TEST(AtomicArrayUpdaterTest, shouldAddElements)
{
    AtomicArrayUpdater<int64_t> arrayUpdater;

    {
        auto pair = arrayUpdater.load();
        ASSERT_EQ(nullptr, pair.first);
        ASSERT_EQ(0, pair.second);
    }

    {
        auto pair = arrayUpdater.addElement(INT64_MAX);
        ASSERT_EQ(nullptr, pair.first);
        ASSERT_EQ(0, pair.second);
        delete[] pair.first;
    }

    {
        auto pair = arrayUpdater.load();
        ASSERT_EQ(1, pair.second);
        ASSERT_EQ(INT64_MAX, pair.first[0]);
    }

    {
        auto pair = arrayUpdater.addElement(INT64_MIN);
        ASSERT_EQ(1, pair.second);
        ASSERT_EQ(INT64_MAX, pair.first[0]);
        delete[] pair.first;
    }

    {
        auto pair = arrayUpdater.load();
        ASSERT_EQ(2, pair.second);
        ASSERT_EQ(INT64_MAX, pair.first[0]);
        ASSERT_EQ(INT64_MIN, pair.first[1]);
        delete[] pair.first;
    }
}

TEST(AtomicArrayUpdaterTest, shouldRemoveElements)
{
    AtomicArrayUpdater<int64_t> arrayUpdater;

    delete[] arrayUpdater.addElement(1).first;
    delete[] arrayUpdater.addElement(2).first;
    delete[] arrayUpdater.addElement(3).first;

    auto pair = arrayUpdater.load();
    ASSERT_EQ(3, pair.second);
    ASSERT_EQ(1, pair.first[0]);
    ASSERT_EQ(2, pair.first[1]);
    ASSERT_EQ(3, pair.first[2]);

    auto removed = arrayUpdater.removeElement(
        [&](int64_t elem)
        {
            return elem == 2;
        });
    ASSERT_EQ(1, removed.second);
    ASSERT_EQ(1, removed.first[0]);
    ASSERT_EQ(2, removed.first[1]);
    ASSERT_EQ(3, removed.first[2]);
    delete[] removed.first;

    pair = arrayUpdater.load();
    ASSERT_EQ(2, pair.second);
    ASSERT_EQ(1, pair.first[0]);
    ASSERT_EQ(3, pair.first[1]);

    removed = arrayUpdater.removeElement(
        [&](int64_t elem)
        {
            return elem == 2;
        });
    ASSERT_EQ(nullptr, removed.first);
    ASSERT_EQ(0, removed.second);

    delete[] pair.first;
}

TEST(AtomicArrayUpdaterTest, shouldStoreAnArray)
{
    AtomicArrayUpdater<int64_t> arrayUpdater;

    delete[] arrayUpdater.addElement(INT64_MAX).first;
    delete[] arrayUpdater.addElement(INT64_MIN).first;

    auto pair = arrayUpdater.load();
    auto originalArray = pair.first;
    ASSERT_EQ(2, pair.second);

    int64_t newArray[] = { 1, 2, 3 };
    auto oldPair = arrayUpdater.store(newArray, 3);
    ASSERT_EQ(2, oldPair.second);

    pair = arrayUpdater.load();
    ASSERT_NE(originalArray, pair.first);
    ASSERT_EQ(newArray, pair.first);
    ASSERT_EQ(3, pair.second);

    delete[] originalArray;
}

TEST(AtomicArrayUpdaterTest, shouldAddElementsConcurrently)
{
    for (int iter = 0; iter < NUM_ITERATIONS; iter++)
    {
        AtomicArrayUpdater<int64_t> arrayUpdater;
        delete[] arrayUpdater.addElement(INT64_MIN).first;

        std::atomic<int> countDown(2);
        std::vector<int64_t *> deleteList;
        const int64_t minElement = 212121 * iter;

        std::thread mutator = std::thread(
            [&]()
            {
                countDown--;
                while (countDown > 0)
                {
                    std::this_thread::yield();
                }

                int64_t element = minElement;
                for (int j = 0; j < NUM_ELEMENTS; j++)
                {
                    auto pair = arrayUpdater.addElement(++element);
                    deleteList.emplace_back(pair.first);
                }
            });

        countDown--;
        while (countDown > 0)
        {
            std::this_thread::yield();
        }

        while (true)
        {
            auto pair = arrayUpdater.load();
            const int index = static_cast<int>(pair.second) - 1;
            if (0 == index)
            {
                ASSERT_EQ(INT64_MIN, pair.first[index]);
            }
            else
            {
                ASSERT_EQ(INT64_MIN, pair.first[0]);
                ASSERT_NE(INT64_MIN, pair.first[index]);
            }

            if (pair.second > NUM_ELEMENTS)
            {
                break;
            }
        }

        if (mutator.joinable())
        {
            mutator.join();
        }

        auto pair = arrayUpdater.load();
        ASSERT_EQ(NUM_ELEMENTS + 1, pair.second);
        ASSERT_EQ(INT64_MIN, pair.first[0]);
        ASSERT_EQ(minElement + NUM_ELEMENTS, pair.first[pair.second - 1]);

        delete[] pair.first;
        for (auto &array: deleteList)
        {
            delete[] array;
        }
    }
}

TEST(AtomicArrayUpdaterTest, shouldRemoveElementsConcurrently)
{
    for (int iter = 0; iter < NUM_ITERATIONS; iter++)
    {
        AtomicArrayUpdater<int64_t> arrayUpdater;
        for (int i = 0; i < NUM_ELEMENTS; i++)
        {
            delete[] arrayUpdater.addElement(i).first;
        }

        delete[] arrayUpdater.addElement(INT64_MIN).first;

        std::atomic<int> countDown(2);
        std::vector<int64_t *> deleteList;

        std::thread mutator = std::thread(
            [&]()
            {
                countDown--;
                while (countDown > 0)
                {
                    std::this_thread::yield();
                }

                for (int j = 0; j < NUM_ELEMENTS; j++)
                {
                    auto pair = arrayUpdater.removeElement(
                        [&](int64_t elem)
                        {
                            return elem > 0;
                        });
                    deleteList.emplace_back(pair.first);
                }
            });

        countDown--;
        while (countDown > 0)
        {
            std::this_thread::yield();
        }

        while (true)
        {
            auto pair = arrayUpdater.load();
            const int index = static_cast<int>(pair.second) - 1;
            ASSERT_EQ(INT64_MIN, pair.first[index]);
            if (index > 0)
            {
                ASSERT_EQ(0, pair.first[0]);
            }

            if (2 == pair.second)
            {
                break;
            }
        }

        if (mutator.joinable())
        {
            mutator.join();
        }

        auto pair = arrayUpdater.load();
        ASSERT_EQ(2, pair.second);
        ASSERT_EQ(0, pair.first[0]);
        ASSERT_EQ(INT64_MIN, pair.first[1]);

        delete[] pair.first;
        for (auto &array: deleteList)
        {
            delete[] array;
        }
    }
}

TEST(AtomicArrayUpdaterTest, shouldAddAndRemoveElementsConcurrently)
{
    for (int iter = 0; iter < NUM_ITERATIONS; iter++)
    {
        AtomicArrayUpdater<int64_t> arrayUpdater;
        for (int i = 0; i < NUM_ELEMENTS; i++)
        {
            delete[] arrayUpdater.addElement(i).first;
        }

        std::atomic<int> countDown(2);
        std::vector<int64_t *> deleteList;

        std::thread mutator = std::thread(
            [&]()
            {
                countDown--;
                while (countDown > 0)
                {
                    std::this_thread::yield(); // wait for other thread
                }

                int64_t next = 11111111111111;
                for (int j = 0; j < NUM_ELEMENTS; j++)
                {
                    auto pair = arrayUpdater.addElement(next++);
                    deleteList.emplace_back(pair.first);

                    pair = arrayUpdater.removeElement(
                        [&](int64_t elem)
                        {
                            return elem > 5 && (elem & 1) == 0;
                        });
                    deleteList.emplace_back(pair.first);
                }
            });

        countDown--;
        while (countDown > 0)
        {
            std::this_thread::yield();
        }

        while (true)
        {
            auto pair = arrayUpdater.load();
            const int index = static_cast<int>(pair.second) - 1;
            ASSERT_GE(pair.first[index], 0);

            if (pair.second > NUM_ELEMENTS)
            {
                break;
            }
        }

        if (mutator.joinable())
        {
            mutator.join();
        }

        auto pair = arrayUpdater.load();
        ASSERT_EQ(NUM_ELEMENTS + 3, pair.second);

        delete[] pair.first;
        for (auto &array: deleteList)
        {
            delete[] array;
        }
    }
}
