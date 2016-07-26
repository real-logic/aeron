/*
 * Copyright 2015 - 2016 Real Logic Ltd.
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
#include <gtest/gtest.h>
#include <thread>

#include "concurrent/OneToOneConcurrentArrayQueue.h"

class OneToOneConcurrentArrayQueueTest : public testing::Test, public testing::WithParamInterface<int>
{

};

TEST_P(OneToOneConcurrentArrayQueueTest, pollFetchsAllValuesOffered)
{
    const int queueLength = GetParam();

    aeron::driver::concurrent::OneToOneConcurrentArrayQueue<int> q(queueLength);

    const int iterations = queueLength + queueLength / 3;
    std::unique_ptr<int[]> values(new int[iterations]);

    int *valueArray = values.get();

    std::thread consumer(
        [&q, &valueArray, iterations]()
        {
            int i = 0;

            while (i < iterations)
            {
                int *value = q.poll();

                if (nullptr != value)
                {
                    EXPECT_EQ(valueArray[i], *value);
                    i++;
                }

                std::this_thread::yield();
            }
        });

    for (int i = 0; i < iterations; i++)
    {
        values[i] = i;

        while (!q.offer(&valueArray[i]))
        {
            std::this_thread::yield();
        }
    }

    consumer.join();
}

INSTANTIATE_TEST_CASE_P(
    OneToOneConcurrentArrayQueueLengthTest,
    OneToOneConcurrentArrayQueueTest,
    testing::Range(2, (16 * 1024), 101));
