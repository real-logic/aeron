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
#include "Context.h"

namespace aeron {

using namespace aeron::common::common;
using namespace aeron::common::concurrent;

class Aeron
{
public:
    typedef Aeron this_t;

    Aeron(Context& context);
    virtual ~Aeron();

    inline Publication* addPublication(const std::string& channel, std::int32_t streamId, std::int32_t sessionId = 0)
    {
        std::int32_t sessionIdToRequest = sessionId;

        if (0 == sessionIdToRequest)
        {
            // TODO: generate random sessionIdToRequest
        }

        return m_conductor.addPublication(channel, streamId, sessionIdToRequest);
    }

    inline Subscription* addSubscription(const std::string& channel, std::int32_t streamId, logbuffer::handler_t& handler)
    {
        return m_conductor.addSubscription(channel, streamId, handler);
    }

private:
    ClientConductor m_conductor;
    BusySpinIdleStrategy m_idleStrategy;
    AgentRunner<ClientConductor, BusySpinIdleStrategy> m_conductorRunner;
    Context& m_context;

    // items managed by Aeron

    DriverProxy& createDriverProxy(const Context& context);
};

}

#endif