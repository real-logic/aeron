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

#ifndef AERON_EMBEDDED_MEDIA_DRIVER_H
#define AERON_EMBEDDED_MEDIA_DRIVER_H

#if defined(__linux__)
#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#endif

#include <string>
#include <thread>
#include <atomic>

extern "C"
{
#include "aeronmd.h"
}

namespace aeron
{

class EmbeddedMediaDriver
{
public:
    EmbeddedMediaDriver() :
        m_running(true),
        m_context(nullptr),
        m_driver(nullptr),
        m_livenessTimeout(5 * 1000 * 1000 * 1000LL)
    {
    }

    ~EmbeddedMediaDriver()
    {
        aeron_driver_close(m_driver);
        aeron_driver_context_close(m_context);
    }

    void driverLoop()
    {
        while (m_running)
        {
            aeron_driver_main_idle_strategy(m_driver, aeron_driver_main_do_work(m_driver));
        }
    }

    void stop()
    {
        m_running = false;
        m_thread.join();
    }

    void start()
    {
        if (init() < 0)
        {
            throw std::runtime_error("could not initialize");
        }

        m_thread = std::thread(
            [&]()
            {
                driverLoop();
            });
    }

    void livenessTimeoutNs(uint16_t livenessTimeout)
    {
        m_livenessTimeout = livenessTimeout;
    }

protected:
    int init()
    {
        if (aeron_driver_context_init(&m_context) < 0)
        {
            fprintf(stderr, "ERROR: context init (%d) %s\n", aeron_errcode(), aeron_errmsg());
            return -1;
        }

        aeron_driver_context_set_threading_mode(m_context, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(m_context, true);
        aeron_driver_context_set_dir_delete_on_shutdown(m_context, true);
        aeron_driver_context_set_shared_idle_strategy(m_context, "sleeping");
        aeron_driver_context_set_term_buffer_sparse_file(m_context, true);
        aeron_driver_context_set_term_buffer_length(m_context, 64 * 1024);
        aeron_driver_context_set_timer_interval_ns(m_context, m_livenessTimeout / 5);
        aeron_driver_context_set_client_liveness_timeout_ns(m_context, m_livenessTimeout);

        long long debugTimeoutMs;
        if (0 != (debugTimeoutMs = EmbeddedMediaDriver::getDebugTimeoutMs()))
        {
            aeron_driver_context_set_driver_timeout_ms(m_context, debugTimeoutMs);
            aeron_driver_context_set_client_liveness_timeout_ns(m_context, debugTimeoutMs * 1000000LL);
            aeron_driver_context_set_image_liveness_timeout_ns(m_context, debugTimeoutMs * 1000000LL);
            aeron_driver_context_set_publication_unblock_timeout_ns(m_context, 2 * debugTimeoutMs * 1000000LL);
        }

        if (aeron_driver_init(&m_driver, m_context) < 0)
        {
            fprintf(stderr, "ERROR: driver init (%d) %s\n", aeron_errcode(), aeron_errmsg());
            return -1;
        }

        if (aeron_driver_start(m_driver, true) < 0)
        {
            fprintf(stderr, "ERROR: driver start (%d) %s\n", aeron_errcode(), aeron_errmsg());
            return -1;
        }

        return 0;
    }

private:
    std::atomic<bool> m_running;
    std::thread m_thread;
    aeron_driver_context_t *m_context;
    aeron_driver_t *m_driver;
    std::uint64_t m_livenessTimeout;

    static long long getDebugTimeoutMs()
    {
        const char* debug_timeout_str = getenv("AERON_DEBUG_TIMEOUT");
        return NULL != debug_timeout_str ? strtoll(debug_timeout_str, NULL, 10) : 0LL;
    }
};

}

#endif //AERON_EMBEDDED_MEDIA_DRIVER_H
