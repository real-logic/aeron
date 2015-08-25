/*
 * Copyright 2015 Real Logic Ltd.
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

#include "concurrent/OneToOneConcurrentArrayQueue.h"

//using namespace aeron::test;

class OneToOneConcurrentArrayQueueTest : public testing::Test
{

};

TEST_F(OneToOneConcurrentArrayQueueTest, pollFetchsAllValuesOffered)
{
    aeron::driver::concurrent::OneToOneConcurrentArrayQueue<int> q(1024);

    const int iterations = 10;
    int values[iterations];

    for (int i = 0; i < iterations; i++)
    {
        values[i] = i;
        q.offer(&values[i]);
    }

    for (int i = 0; i < iterations; i++)
    {
        EXPECT_EQ(values[i], *q.poll());
    }
}