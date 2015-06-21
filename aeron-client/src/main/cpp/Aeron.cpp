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

#include "Aeron.h"

namespace aeron {

static const std::chrono::duration<long, std::milli> IDLE_SLEEP_MS(4);

static long currentTimeMillis()
{
    using namespace std::chrono;

    system_clock::time_point now = system_clock::now();
    milliseconds ms = duration_cast<milliseconds>(now.time_since_epoch());

    return ms.count();
}

Aeron::Aeron(Context &context) :
    m_randomEngine(m_randomEngine()),
    m_sessionIdDistribution(-INT_MAX, INT_MAX),
    m_context(context.conclude()),
    m_cncBuffer(mapCncFile(context)),
    m_toDriverAtomicBuffer(CncFileDescriptor::createToDriverBuffer(m_cncBuffer)),
    m_toClientsAtomicBuffer(CncFileDescriptor::createToClientsBuffer(m_cncBuffer)),
    m_countersValueBuffer(CncFileDescriptor::createCounterValuesBuffer(m_cncBuffer)),
    m_toDriverRingBuffer(m_toDriverAtomicBuffer),
    m_driverProxy(m_toDriverRingBuffer),
    m_toClientsBroadcastReceiver(m_toClientsAtomicBuffer),
    m_toClientsCopyReceiver(m_toClientsBroadcastReceiver),
    m_conductor(
        currentTimeMillis,
        m_driverProxy,
        m_toClientsCopyReceiver,
        m_countersValueBuffer,
        context.m_onNewPublicationHandler,
        context.m_onNewSubscriptionHandler,
        context.m_onNewConnectionHandler,
        context.m_onInactiveConnectionHandler,
        context.m_mediaDriverTimeout,
        context.m_resourceLingerTimeout),
    m_idleStrategy(IDLE_SLEEP_MS),
    m_conductorRunner(m_conductor, m_idleStrategy, m_context.m_exceptionHandler)
{
    m_conductorRunner.start();
}

Aeron::~Aeron()
{
    m_conductorRunner.close();

    // memory mapped files should be free'd by the destructor of the shared_ptr

    // TODO: do cleanup of anything created
}

inline MemoryMappedFile::ptr_t Aeron::mapCncFile(Context &context)
{
    MemoryMappedFile::ptr_t cncBuffer = MemoryMappedFile::mapExisting(context.cncFileName().c_str());

    std::int32_t cncVersion = CncFileDescriptor::cncVersion(cncBuffer);

    if (CncFileDescriptor::CNC_VERSION != cncVersion)
    {
        throw util::IllegalStateException(
            util::strPrintf("aeron cnc file version not understood: version=%d", cncVersion), SOURCEINFO);
    }

    return cncBuffer;
}

}