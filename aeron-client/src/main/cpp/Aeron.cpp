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

using namespace aeron;

Aeron::Aeron(Context& context) :
    m_context(context.conclude()),
    m_conductor(
        createDriverProxy(m_context),
        createDriverReceiver(m_context),
        context.m_onNewPublicationHandler,
        context.m_onNewSubscriptionHandler),
    m_idleStrategy(),
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


inline void Aeron::mapCncFile(Context& context)
{
    if (!m_cncBuffer)
    {
        m_cncBuffer = MemoryMappedFile::mapExisting(context.cncFileName().c_str());

        std::int32_t cncVersion = CncFileDescriptor::cncVersion(m_cncBuffer);

        if (CncFileDescriptor::CNC_VERSION != cncVersion)
        {
            throw util::IllegalStateException(
                util::strPrintf("aeron cnc file version not understood: version=%d", cncVersion), SOURCEINFO);
        }
    }
}

DriverProxy& Aeron::createDriverProxy(Context& context)
{
    m_toDriverRingBuffer = context.toDriverBuffer();

    if (nullptr == m_toDriverRingBuffer)
    {
        mapCncFile(context);
        m_toDriverAtomicBuffer = CncFileDescriptor::createToDriverBuffer(m_cncBuffer);
        m_toDriverRingBuffer = std::unique_ptr<ManyToOneRingBuffer>(new ManyToOneRingBuffer(m_toDriverAtomicBuffer));
    }

    m_driverProxy = std::unique_ptr<DriverProxy>(new DriverProxy(*m_toDriverRingBuffer));

    return *m_driverProxy;
}

CopyBroadcastReceiver& Aeron::createDriverReceiver(Context &context)
{
    m_toClientsCopyReceiver = context.toClientsBuffer();

    if (nullptr == m_toClientsCopyReceiver)
    {
        mapCncFile(context);
        m_toClientsAtomicBuffer = CncFileDescriptor::createToClientsBuffer(m_cncBuffer);
        m_toClientsBroadcastReceiver = std::unique_ptr<BroadcastReceiver>(new BroadcastReceiver(m_toClientsAtomicBuffer));
        m_toClientsCopyReceiver = std::unique_ptr<CopyBroadcastReceiver>(new CopyBroadcastReceiver(*m_toClientsBroadcastReceiver));
    }

    return *m_toClientsCopyReceiver;
}