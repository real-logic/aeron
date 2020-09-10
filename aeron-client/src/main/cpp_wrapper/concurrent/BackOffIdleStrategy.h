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

#ifndef AERON_BACKOFF_IDLE_STRATEGY_H
#define AERON_BACKOFF_IDLE_STRATEGY_H

#include <thread>
#include <chrono>
#include <algorithm>

#include "Atomic64.h"
#include "util/BitUtil.h"

namespace aeron { namespace concurrent {

static const std::uint8_t BACKOFF_STATE_NOT_IDLE = 0;
static const std::uint8_t BACKOFF_STATE_SPINNING = 1;
static const std::uint8_t BACKOFF_STATE_YIELDING = 2;
static const std::uint8_t BACKOFF_STATE_PARKING = 3;

class BackoffIdleStrategy
{
public:
    explicit BackoffIdleStrategy(
        std::int64_t maxSpins = 10,
        std::int64_t maxYields = 20,
        std::chrono::duration<long, std::nano> minParkPeriodNs = std::chrono::duration<long, std::nano>(1000),
        std::chrono::duration<long, std::nano> maxParkPeriodNs = std::chrono::duration<long, std::milli>(1)) :
        m_prePad(),
        m_maxSpins(maxSpins),
        m_maxYields(maxYields),
        m_minParkPeriodNs(minParkPeriodNs),
        m_maxParkPeriodNs(maxParkPeriodNs),
        m_spins(0),
        m_yields(0),
        m_parkPeriodNs(minParkPeriodNs),
        m_state(BACKOFF_STATE_NOT_IDLE),
        m_postPad()
    {
    }

    inline void idle(int workCount)
    {
        if (workCount > 0)
        {
            reset();
        }
        else
        {
            idle();
        }
    }

    inline void reset()
    {
        m_spins = 0;
        m_yields = 0;
        m_parkPeriodNs = m_minParkPeriodNs;
        m_state = BACKOFF_STATE_NOT_IDLE;
    }

    inline void idle()
    {
        switch(m_state)
        {
            case BACKOFF_STATE_NOT_IDLE:
                m_state = BACKOFF_STATE_SPINNING;
                m_spins++;
                break;

            case BACKOFF_STATE_SPINNING:
                atomic::cpu_pause();
                if (++m_spins > m_maxSpins)
                {
                    m_state = BACKOFF_STATE_YIELDING;
                    m_yields = 0;
                }
                break;

            case BACKOFF_STATE_YIELDING:
                if (++m_yields > m_maxYields)
                {
                    m_state = BACKOFF_STATE_PARKING;
                    m_parkPeriodNs = m_minParkPeriodNs;
                }
                else
                {
                    std::this_thread::yield();
                }
                break;

            case BACKOFF_STATE_PARKING:
            default:
                std::this_thread::sleep_for(m_parkPeriodNs);
                m_parkPeriodNs = std::min(m_parkPeriodNs * 2, m_maxParkPeriodNs);
                break;
        }
    }

protected:
    std::uint8_t m_prePad[aeron::util::BitUtil::CACHE_LINE_LENGTH - sizeof(std::int64_t)];
    std::int64_t m_maxSpins;
    std::int64_t m_maxYields;
    std::chrono::duration<long, std::nano> m_minParkPeriodNs;
    std::chrono::duration<long, std::nano> m_maxParkPeriodNs;
    std::int64_t m_spins;
    std::int64_t m_yields;
    std::chrono::duration<long, std::nano> m_parkPeriodNs;
    std::uint8_t m_state;
    std::uint8_t m_postPad[aeron::util::BitUtil::CACHE_LINE_LENGTH];
};

}}

#endif //AERON_BACKOFF_IDLE_STRATEGY_H
