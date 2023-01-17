/*
 * Copyright 2014-2023 Real Logic Limited.
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

#define NUM_THREADS (2)
#define NUM_ELEMENTS (12480)

class AtomicArrayUpdaterTest : public testing::Test
{
public:
    void TearDown() override
    {
        auto pair = m_arrayUpdater.load();
        delete[] pair.first;
    }

protected:
    AtomicArrayUpdater<std::int64_t> m_arrayUpdater = {};
};

TEST_F(AtomicArrayUpdaterTest, shouldAddElements)
{
    auto pair = m_arrayUpdater.load();
    ASSERT_EQ(nullptr, pair.first);
    ASSERT_EQ(0, pair.second);

    pair = m_arrayUpdater.addElement(INT64_MAX);
    ASSERT_EQ(nullptr, pair.first);
    ASSERT_EQ(0, pair.second);
    delete[] pair.first;

    pair = m_arrayUpdater.load();
    ASSERT_EQ(1, pair.second);
    ASSERT_EQ(INT64_MAX, pair.first[0]);

    pair = m_arrayUpdater.addElement(INT64_MIN);
    ASSERT_EQ(1, pair.second);
    ASSERT_EQ(INT64_MAX, pair.first[0]);
    delete[] pair.first;

    pair = m_arrayUpdater.load();
    ASSERT_EQ(2, pair.second);
    ASSERT_EQ(INT64_MAX, pair.first[0]);
    ASSERT_EQ(INT64_MIN, pair.first[1]);
}

TEST_F(AtomicArrayUpdaterTest, shouldRemoveElements)
{
    auto pair = m_arrayUpdater.addElement(1);
    delete[] pair.first;
    pair = m_arrayUpdater.addElement(2);
    delete[] pair.first;
    pair = m_arrayUpdater.addElement(3);
    delete[] pair.first;

    pair = m_arrayUpdater.load();
    ASSERT_EQ(3, pair.second);
    ASSERT_EQ(1, pair.first[0]);
    ASSERT_EQ(2, pair.first[1]);
    ASSERT_EQ(3, pair.first[2]);

    auto removed = m_arrayUpdater.removeElement(
        [&](int64_t elem)
        {
            return elem == 2;
        });
    ASSERT_EQ(1, removed.second);
    ASSERT_EQ(1, removed.first[0]);
    ASSERT_EQ(2, removed.first[1]);
    ASSERT_EQ(3, removed.first[2]);
    delete[] removed.first;

    pair = m_arrayUpdater.load();
    ASSERT_EQ(2, pair.second);
    ASSERT_EQ(1, pair.first[0]);
    ASSERT_EQ(3, pair.first[1]);

    removed = m_arrayUpdater.removeElement(
        [&](int64_t elem)
        {
            return elem == 2;
        });
    ASSERT_EQ(nullptr, removed.first);
    ASSERT_EQ(0, removed.second);
}

TEST_F(AtomicArrayUpdaterTest, shouldStoreAnArray)
{
    AtomicArrayUpdater<std::int64_t> arrayUpdater = {};
    auto pair = arrayUpdater.addElement(INT64_MAX);
    delete[] pair.first;
    pair = arrayUpdater.addElement(INT64_MIN);
    delete[] pair.first;
    pair = arrayUpdater.load();
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

TEST_F(AtomicArrayUpdaterTest, shouldAddElementsConcurrently)
{
    std::atomic<int> countDown(NUM_THREADS);
    std::vector<std::thread> threads;

    for (int i = 0; i < NUM_THREADS; i++)
    {
        threads.push_back(
            std::thread(
                [&]()
                {
                    const int phase = (countDown--) - 1;
                    while (countDown > 0)
                    {
                        std::this_thread::yield(); // wait for other threads
                    }

                    int64_t element = phase * 1000000000000;
                    for (int j = 0; j < NUM_ELEMENTS; j++)
                    {
                        auto pair = m_arrayUpdater.addElement(element++);
                        delete[] pair.first;
                    }
                }));
    }

    for (std::thread &t: threads)
    {
        if (t.joinable())
        {
            t.join();
        }
    }

    auto pair = m_arrayUpdater.load();
    ASSERT_EQ(NUM_THREADS * NUM_ELEMENTS, pair.second);
}

TEST_F(AtomicArrayUpdaterTest, shouldRemoveElementsConcurrently)
{
    for (int i = 0; i < NUM_ELEMENTS; i++)
    {
        auto pair = m_arrayUpdater.addElement(i);
        delete[] pair.first;
    }
    auto pair = m_arrayUpdater.addElement(INT64_MIN);
    delete[] pair.first;

    std::atomic<int> countDown(NUM_THREADS);
    std::vector<std::thread> threads;

    for (int i = 0; i < NUM_THREADS; i++)
    {
        threads.push_back(
            std::thread(
                [&]()
                {
                    const int phase = (countDown--) - 1;
                    while (countDown > 0)
                    {
                        std::this_thread::yield(); // wait for other threads
                    }

                    for (int j = 0; j < NUM_ELEMENTS; j++)
                    {
                        auto pair = m_arrayUpdater.removeElement(
                            [&](int64_t elem)
                            {
                                return elem > 0 && ((elem & 1) == phase);
                            });
                        delete[] pair.first;
                    }
                }));
    }

    for (std::thread &t: threads)
    {
        if (t.joinable())
        {
            t.join();
        }
    }

    pair = m_arrayUpdater.load();
    ASSERT_EQ(2, pair.second);
    ASSERT_EQ(0, pair.first[0]);
    ASSERT_EQ(INT64_MIN, pair.first[1]);
}

TEST_F(AtomicArrayUpdaterTest, shouldAddAndRemoveElementsConcurrently)
{
    for (int i = 0; i < NUM_ELEMENTS; i++)
    {
        auto pair = m_arrayUpdater.addElement(i);
        delete[] pair.first;
    }

    std::atomic<int> countDown(NUM_THREADS);
    std::vector<std::thread> threads;

    threads.push_back(
        std::thread(
            [&]()
            {
                countDown--;
                while (countDown > 0)
                {
                    std::this_thread::yield(); // wait for other thread
                }

                int64_t elem = 1000000000001;
                for (int j = 0; j < NUM_ELEMENTS; j++)
                {
                    auto pair = m_arrayUpdater.addElement(elem); // add odd
                    delete[] pair.first;
                    elem += 2;
                }
            }));

    threads.push_back(
        std::thread(
            [&]()
            {
                countDown--;
                while (countDown > 0)
                {
                    std::this_thread::yield(); // wait for other threads
                }

                for (int j = 0; j < NUM_ELEMENTS; j++)
                {
                    auto pair = m_arrayUpdater.removeElement(
                        [&](int64_t elem)
                        {
                            return (elem & 1) == 0; // remove even
                        });
                    delete[] pair.first;
                }
            }));

    for (std::thread &t: threads)
    {
        if (t.joinable())
        {
            t.join();
        }
    }

    auto pair = m_arrayUpdater.load();
    ASSERT_EQ((NUM_ELEMENTS / 2) + NUM_ELEMENTS, pair.second);
}
