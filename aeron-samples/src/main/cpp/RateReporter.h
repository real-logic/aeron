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

#ifndef AERON_RATEREPORTER_H
#define AERON_RATEREPORTER_H

#include <functional>
#include <atomic>
#include <thread>
#include <chrono>

#include "util/BitUtil.h"

namespace aeron
{

using namespace std::chrono;

class RateReporter
{
public:
    typedef std::function<void(double, double, int64_t, int64_t)> on_rate_report_t;

    RateReporter(nanoseconds reportInterval, const on_rate_report_t &onReport) :
        m_reportInterval(reportInterval),
        m_onReport(onReport),
        m_lastTimestamp(steady_clock::now())
    {
        static_cast<void>(m_paddingBefore);
        static_cast<void>(m_paddingAfter);
    }

    void run()
    {
        while (m_running)
        {
            std::this_thread::sleep_for(m_reportInterval);
            report();
        }
    }

    void report()
    {
        int64_t totalBytes = std::atomic_load_explicit(&m_totalBytes, std::memory_order_acquire);
        int64_t totalMessages = std::atomic_load_explicit(&m_totalMessages, std::memory_order_acquire);
        steady_clock::time_point timestamp = steady_clock::now();

        const double timeSpanSec = duration<double, std::ratio<1, 1>>(timestamp - m_lastTimestamp).count();
        const double messagesPerSec = (totalMessages - m_lastTotalMessages) / timeSpanSec;
        const double bytesPerSec = (totalBytes - m_lastTotalBytes) / timeSpanSec;

        m_onReport(messagesPerSec, bytesPerSec, totalMessages, totalBytes);

        m_lastTotalBytes = totalBytes;
        m_lastTotalMessages = totalMessages;
        m_lastTimestamp = timestamp;
    }

    void reset()
    {
        int64_t currentTotalBytes = std::atomic_load_explicit(&m_totalBytes, std::memory_order_relaxed);
        int64_t currentTotalMessages = std::atomic_load_explicit(&m_totalMessages, std::memory_order_relaxed);
        steady_clock::time_point currentTimestamp = steady_clock::now();

        m_lastTotalBytes = currentTotalBytes;
        m_lastTotalMessages = currentTotalMessages;
        m_lastTimestamp = currentTimestamp;
    }

    inline void halt()
    {
        m_running = false;
    }

    inline void onMessage(long messages, long bytes)
    {
        int64_t totalBytes = std::atomic_load_explicit(&m_totalBytes, std::memory_order_relaxed);
        int64_t totalMessages = std::atomic_load_explicit(&m_totalMessages, std::memory_order_relaxed);

        std::atomic_store_explicit(&m_totalBytes, totalBytes + bytes, std::memory_order_release);
        std::atomic_store_explicit(&m_totalMessages, totalMessages + messages, std::memory_order_release);
    }

    int64_t totalMessages()
    {
        return m_totalMessages.load();
    }

private:
    const nanoseconds m_reportInterval;
    const on_rate_report_t m_onReport;

    char m_paddingBefore[aeron::util::BitUtil::CACHE_LINE_LENGTH]{};
    std::atomic<bool> m_running = { true };
    std::atomic<int64_t> m_totalBytes = { 0 };
    std::atomic<int64_t> m_totalMessages = { 0 };
    char m_paddingAfter[aeron::util::BitUtil::CACHE_LINE_LENGTH]{};

    int64_t m_lastTotalBytes = 0;
    int64_t m_lastTotalMessages = 0;

    steady_clock::time_point m_lastTimestamp;
};

}

#endif //AERON_RATEREPORTER_H
