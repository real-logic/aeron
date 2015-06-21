/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
#include <random>
#include <concurrent/logbuffer/TermReader.h>
#include <util/MemoryMappedFile.h>
#include <concurrent/broadcast/CopyBroadcastReceiver.h>
#include "ClientConductor.h"
#include "concurrent/SleepingIdleStrategy.h"
#include "concurrent/AgentRunner.h"
#include "Publication.h"
#include "Subscription.h"
#include "Context.h"

namespace aeron {

using namespace aeron::util;
using namespace aeron::concurrent;
using namespace aeron::concurrent::broadcast;

class Aeron
{
public:
    typedef Aeron this_t;

    Aeron(Context& context);
    virtual ~Aeron();

    inline std::int64_t addPublication(const std::string& channel, std::int32_t streamId, std::int32_t sessionId = 0)
    {
        std::int32_t sessionIdToRequest = sessionId;

        if (0 == sessionIdToRequest)
        {
            sessionIdToRequest = m_sessionIdDistribution(m_randomEngine);
        }

        return m_conductor.addPublication(channel, streamId, sessionIdToRequest);
    }

    inline std::shared_ptr<Publication> findPublication(std::int64_t id)
    {
        return m_conductor.findPublication(id);
    }

    inline std::int64_t addSubscription(const std::string& channel, std::int32_t streamId)
    {
        return m_conductor.addSubscription(channel, streamId);
    }

    inline std::shared_ptr<Subscription> findSubscription(std::int64_t id)
    {
        return m_conductor.findSubscription(id);
    }

    inline bool isPublicationClosed(std::int64_t id)
    {
        // TODO:
        return true;
    }

private:
    std::random_device m_randomDevice;
    std::default_random_engine m_randomEngine;
    std::uniform_int_distribution<std::int32_t> m_sessionIdDistribution;

    Context& m_context;

    MemoryMappedFile::ptr_t m_cncBuffer;

    AtomicBuffer m_toDriverAtomicBuffer;
    AtomicBuffer m_toClientsAtomicBuffer;
    AtomicBuffer m_countersValueBuffer;

    ManyToOneRingBuffer m_toDriverRingBuffer;
    DriverProxy m_driverProxy;

    BroadcastReceiver m_toClientsBroadcastReceiver;
    CopyBroadcastReceiver m_toClientsCopyReceiver;

    ClientConductor m_conductor;
    SleepingIdleStrategy m_idleStrategy;
    AgentRunner<ClientConductor, SleepingIdleStrategy> m_conductorRunner;

    MemoryMappedFile::ptr_t mapCncFile(Context& context);
};

}

#endif