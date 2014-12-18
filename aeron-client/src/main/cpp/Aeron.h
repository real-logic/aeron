/*
 * Copyright 2014 Real Logic Ltd.
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

#ifndef INCLUDED_AERON_AERON__
#define INCLUDED_AERON_AERON__

#include <util/Exceptions.h>
#include <iostream>
#include <thread>
#include <concurrent/logbuffer/LogReader.h>
#include "ClientConductor.h"
#include "common/BusySpinIdleStrategy.h"
#include "common/AgentRunner.h"
#include "Publication.h"
#include "Subscription.h"

namespace aeron {

using namespace aeron::common::common;
using namespace aeron::common::concurrent::logbuffer;

inline static void defaultErrorHandler(util::SourcedException& exception)
{
    std::cerr << "ERROR: " << exception.what() << " : " << exception.where() << std::endl;
    ::exit(-1);
}

class Context
{
    friend class Aeron;
public:
    typedef Context this_t;

    Context() :
        m_exceptionHandler(defaultErrorHandler)
    {
    }

    this_t& conclude()
    {
        return *this;
    }

    this_t& useSharedMemoryOnLinux()
    {
        // TODO: set data dir
        // TODO: set admin dir
        // TODO: set counters dir
        return *this;
    }

private:
    exception_handler_t m_exceptionHandler;
};

class Aeron
{
public:
    typedef Aeron this_t;

    Aeron(Context& context) :
        m_conductor(),
        m_idleStrategy(),
        m_conductorRunner(m_conductor, m_idleStrategy, context.m_exceptionHandler),
        m_context(context)
    {
        m_conductorRunner.start();
    }

    virtual ~Aeron()
    {
        m_conductorRunner.close();
    }

    inline Publication* addPublication(const std::string& channel, std::int32_t streamId, std::int32_t sessionId = 0)
    {
        std::int32_t sessionIdToRequest = sessionId;

        if (0 == sessionIdToRequest)
        {
            // TODO: generate random sessionIdToRequest
        }

        return m_conductor.addPublication(channel, streamId, sessionIdToRequest);
    }

    inline Subscription* addSubscription(const std::string& channel, std::int32_t streamId, handler_t& handler)
    {
        return m_conductor.addSubscription(channel, streamId, handler);
    }

private:
    ClientConductor m_conductor;
    BusySpinIdleStrategy m_idleStrategy;
    AgentRunner<ClientConductor, BusySpinIdleStrategy> m_conductorRunner;
    Context& m_context;
};

}

#endif