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

#ifndef AERON_SLEEPING_IDLE_STRATEGY_H
#define AERON_SLEEPING_IDLE_STRATEGY_H

#include <thread>
#include <chrono>

namespace aeron { namespace concurrent {

class SleepingIdleStrategy
{
public:
    explicit SleepingIdleStrategy(const std::chrono::duration<long, std::milli> duration) :
        m_duration(duration)
    {
    }

    inline void idle(int workCount)
    {
        if (0 == workCount)
        {
            std::this_thread::sleep_for(m_duration);
        }
    }

    inline void reset()
    {
    }

    inline void idle()
    {
        std::this_thread::sleep_for(m_duration);
    }

private:
    std::chrono::duration<long, std::milli> m_duration;
};

}}

#endif
