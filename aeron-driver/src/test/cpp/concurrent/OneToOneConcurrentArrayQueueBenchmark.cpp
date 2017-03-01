/*
 * Copyright 2014-2017 Real Logic Ltd.
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

#include <string>
#include <benchmark/benchmark.h>
#include <thread>
#include <atomic>
#include <concurrent/Atomic64.h>

#include "concurrent/OneToOneConcurrentArrayQueue.h"

using namespace aeron::driver::concurrent;

static void listener(
    OneToOneConcurrentArrayQueue<int>& qIn,
    OneToOneConcurrentArrayQueue<int>& qOut,
    std::atomic<bool>& start,
    std::atomic<bool>& running)
{
    start.store(true);

    while (running)
    {
        int* p = qIn.poll();
        if (p != nullptr)
        {
            qOut.offer(p);
        }
        aeron::concurrent::atomic::cpu_pause();
    }
}

static void BM_Queue(benchmark::State& state)
{
    int* i = new int{42};
    OneToOneConcurrentArrayQueue<int> qIn{1024};
    OneToOneConcurrentArrayQueue<int> qOut{1024};
    std::atomic<bool> start{false};
    std::atomic<bool> running{true};

    std::thread t{listener, std::ref(qIn), std::ref(qOut), std::ref(start), std::ref(running)};

    while (!start)
    {
        ; // Spin
    }

    while (state.KeepRunning())
    {
        qIn.offer(i);
        while (qOut.poll() != nullptr)
        {
            aeron::concurrent::atomic::cpu_pause();
        }
    }

    running.store(false);

    t.join();
}
BENCHMARK(BM_Queue);

BENCHMARK_MAIN();
