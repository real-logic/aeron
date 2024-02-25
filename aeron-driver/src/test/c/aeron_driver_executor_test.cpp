/*
 * Copyright 2014-2024 Real Logic Limited.
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

#include <gtest/gtest.h>
#include <queue>
#include <thread>

extern "C"
{
#include "aeron_driver_executor.h"
}

typedef struct {
    int some_value;
} task_clientd_t;

class DriverConductorSyncExecutorTest : public testing::Test
{
public:
    void SetUp() override
    {
        if (aeron_driver_executor_init(
            &m_executor,
            on_execution_complete_cb(),
            this) < 0)
        {
            throw std::runtime_error("could not init q");
        }
    }

    void TearDown() override
    {
        aeron_driver_executor_close(&m_executor);
    }

    virtual aeron_driver_executor_on_execution_complete_func_t on_execution_complete_cb()
    {
        return nullptr;
    }

    static int on_execute(void *task_clientd, void *executor_clientd)
    {
        auto *e = (DriverConductorSyncExecutorTest *)executor_clientd;

        e->m_on_execute_count++;

        return e->m_on_execute(task_clientd, e);
    }

    static int on_complete(int result, void *task_clientd, void *executor_clientd)
    {
        auto *e = (DriverConductorSyncExecutorTest *)executor_clientd;

        e->m_on_complete_count++;

        return e->m_on_complete(result, task_clientd, e);
    }

protected:
    aeron_driver_executor_t m_executor = {};
    std::function<int(void *, void *)> m_on_execute;
    std::function<int(int, void *, void *)> m_on_complete;
    int m_on_execute_count = 0;
    int m_on_complete_count = 0;
};

TEST_F(DriverConductorSyncExecutorTest, shouldExecuteSynchronously)
{
    task_clientd_t tcd;

    tcd.some_value = 0;

    m_on_execute =
        [&](void *task_clientd, void *executor_clientd)
        {
            ((task_clientd_t *)task_clientd)->some_value += 100;

            return 1;
        };

    m_on_complete =
        [&](int result, void *task_clientd, void *executor_clientd)
        {
            ((task_clientd_t *)task_clientd)->some_value += 100;

            return ++result;
        };

    int result = aeron_driver_executor_execute(
        &m_executor,
        DriverConductorSyncExecutorTest::on_execute,
        DriverConductorSyncExecutorTest::on_complete,
        &tcd);

    ASSERT_EQ(aeron_driver_executor_do_work(&m_executor), 0);

    ASSERT_EQ(result, 2);
    ASSERT_EQ(m_on_execute_count, 1);
    ASSERT_EQ(m_on_complete_count, 1);
    ASSERT_EQ(tcd.some_value, 200);
}

class DriverConductorAsyncExecutorTest : public DriverConductorSyncExecutorTest
{
public:
    aeron_driver_executor_on_execution_complete_func_t on_execution_complete_cb() override
    {
        return DriverConductorAsyncExecutorTest::on_execution_complete_cb;
    };

    static int on_execution_complete_cb(aeron_driver_executor_task_t *task, void *executor_clientd)
    {
        auto e = ((DriverConductorAsyncExecutorTest *)executor_clientd);

        e->tasks.push(task);
        e->m_on_execution_complete_count++;

        return 0;
    }

protected:
    std::queue<aeron_driver_executor_task_t *> tasks;
    int m_on_execution_complete_count = 0;
};

#define TOTAL_TASKS 1000

TEST_F(DriverConductorAsyncExecutorTest, shouldExecuteAsynchronously)
{
    int work_count = 0;
    task_clientd_t tcd;

    tcd.some_value = 0;

    std::thread dequeue_thread = std::thread(
        [&]()
        {
            work_count = aeron_driver_executor_do_work(&m_executor);
        });

    m_on_execute =
        [&](void *task_clientd, void *executor_clientd)
        {
            ((task_clientd_t *)task_clientd)->some_value += 100;

            return 1;
        };

    m_on_complete =
        [&](int result, void *task_clientd, void *executor_clientd)
        {
            ((task_clientd_t *)task_clientd)->some_value += 100;

            return ++result;
        };

    for (int i = 0; i < TOTAL_TASKS; i++)
    {
        int result = aeron_driver_executor_execute(
            &m_executor,
            DriverConductorSyncExecutorTest::on_execute,
            DriverConductorSyncExecutorTest::on_complete,
            &tcd);
        ASSERT_EQ(result, 0);
    }

    while (m_on_execution_complete_count < TOTAL_TASKS)
    {
        sched_yield();
    }

    ASSERT_EQ(tasks.size(), TOTAL_TASKS);

    while (!tasks.empty())
    {
        aeron_driver_executor_task_do_complete(tasks.front());
        tasks.pop();
    }

    ASSERT_EQ(tasks.size(), 0);

    aeron_blocking_linked_queue_unblock(&m_executor.queue);

    dequeue_thread.join();

    ASSERT_EQ(m_on_execution_complete_count, TOTAL_TASKS);
    ASSERT_EQ(m_on_execute_count, TOTAL_TASKS);
    ASSERT_EQ(m_on_complete_count, TOTAL_TASKS);
    ASSERT_EQ(tcd.some_value, TOTAL_TASKS * 200);
}
