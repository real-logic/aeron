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
#ifndef INCLUDED_AERON_COMMON_AGENT_RUNNER__
#define INCLUDED_AERON_COMMON_AGENT_RUNNER__

#include <string>
#include <util/Exceptions.h>
#include <functional>
#include <thread>
#include <atomic>
#include <concurrent/logbuffer/TermReader.h>

namespace aeron {

namespace concurrent {

template <typename Agent, typename IdleStrategy>
class AgentRunner
{
public:
    AgentRunner(Agent& agent, IdleStrategy& idleStrategy, logbuffer::exception_handler_t& exceptionHandler) :
        m_agent(agent),
        m_idleStrategy(idleStrategy),
        m_exceptionHandler(exceptionHandler),
        m_running(true)
    {
    }

    /**
     * Start the Agent running
     *
     * Will spawn a std::thread.
     */
    inline void start()
    {
        m_thread = std::thread([&]()
        {
            run();
        });
    }

    /**
     * Run the Agent duty cycle until closed
     */
    inline void run()
    {
        try
        {
            m_agent.onStart();
        }
        catch (const util::SourcedException &exception)
        {
            m_exceptionHandler(exception);
            m_running = false;
        }

        while (m_running)
        {
            try
            {
                m_idleStrategy.idle(m_agent.doWork());
            }
            catch (const util::SourcedException &exception)
            {
                m_exceptionHandler(exception);
            }
        }

        try
        {
            m_agent.onClose();
        }
        catch (const util::SourcedException &exception)
        {
            m_exceptionHandler(exception);
        }

    }

    inline void close()
    {
        m_running = false;
        m_thread.join();
    }

private:
    Agent& m_agent;
    IdleStrategy& m_idleStrategy;
    logbuffer::exception_handler_t& m_exceptionHandler;
    std::atomic<bool> m_running;
    std::thread m_thread;
};

}}

#endif
