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