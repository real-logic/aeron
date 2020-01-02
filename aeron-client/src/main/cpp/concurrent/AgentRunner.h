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
#ifndef AERON_AGENT_RUNNER_H
#define AERON_AGENT_RUNNER_H

#include <string>
#include <util/Exceptions.h>
#include <functional>
#include <thread>
#include <atomic>
#include <concurrent/logbuffer/TermReader.h>

#if !defined(AERON_COMPILER_MSVC)
#include <pthread.h>
#endif

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
        m_isRunning(false),
        m_isClosed(false),
        m_name("aeron-agent")
    {
    }

    AgentRunner(
        Agent& agent, IdleStrategy& idleStrategy, logbuffer::exception_handler_t& exceptionHandler, const std::string& name) :
        m_agent(agent),
        m_idleStrategy(idleStrategy),
        m_exceptionHandler(exceptionHandler),
        m_isRunning(false),
        m_isClosed(false),
        m_name(name)
    {
    }

    /**
     * Name given to the thread running the agent.
     *
     * @return the name given to the thread running the agent.
     */
    inline const std::string& name() const
    {
        return m_name;
    }

    /**
 * Is the Agent running?
 *
 * @return is the Agent been started successfully and not closed?
 */
    inline bool isRunning() const
    {
        return m_isRunning;
    }

    /**
     * Has the Agent been closed?
     *
     * @return has the Agent been closed?
     */
    inline bool isClosed() const
    {
        return m_isClosed;
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
#if defined(AERON_COMPILER_MSVC)
#elif defined(Darwin)
            pthread_setname_np(m_name.c_str());
#else
            pthread_setname_np(pthread_self(), m_name.c_str());
#endif
            run();
        });
    }

    /**
     * Run the Agent duty cycle until closed
     */
    inline void run()
    {
        bool expected = false;
        std::atomic_compare_exchange_strong(&m_isRunning, &expected, true);

        try
        {
            m_agent.onStart();
        }
        catch (const util::SourcedException &exception)
        {
            m_exceptionHandler(exception);
            m_isRunning = false;
        }

        while (m_isRunning)
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

    /**
     * Close the agent and stop the associated thread from running. This method waits for the thread to join.
     */
    inline void close()
    {
        bool expected = false;
        if (std::atomic_compare_exchange_strong(&m_isClosed, &expected, true))
        {
            m_isRunning = false;
            m_thread.join();
        }
    }

private:
    Agent& m_agent;
    IdleStrategy& m_idleStrategy;
    logbuffer::exception_handler_t& m_exceptionHandler;
    std::atomic<bool> m_isRunning;
    std::atomic<bool> m_isClosed;
    std::thread m_thread;
    const std::string m_name;
};

}}

#endif
