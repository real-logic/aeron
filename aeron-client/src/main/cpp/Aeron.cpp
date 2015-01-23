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

#include "Aeron.h"

using namespace aeron;

Aeron::Aeron(Context& context) :
    m_context(context.conclude()),
    m_conductor(createDriverProxy(m_context), createDriverReceiver(m_context)),
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

DriverProxy& Aeron::createDriverProxy(Context& context)
{
    m_toDriverRingBuffer = context.toDriverBuffer();

    if (nullptr == m_toDriverRingBuffer)
    {
        // create buffer from mapped file for location specified in context. Managed by shared_ptr.
        m_toDriverBuffer = util::MemoryMappedFile::mapExisting(context.toDriverFileName().c_str());

        // stack temporary for AtomicBuffer. AtomicBuffer within ring buffer will be copy-constructed
        AtomicBuffer buffer(m_toDriverBuffer->getMemoryPtr(), m_toDriverBuffer->getMemorySize());

        m_toDriverRingBuffer = std::unique_ptr<ManyToOneRingBuffer>(new ManyToOneRingBuffer(buffer));
    }

    m_driverProxy = std::unique_ptr<DriverProxy>(new DriverProxy(*m_toDriverRingBuffer));

    return *m_driverProxy;
}

CopyBroadcastReceiver& Aeron::createDriverReceiver(Context &context)
{
    m_toClientsCopyReceiver = context.toClientsBuffer();

    if (nullptr == m_toClientsCopyReceiver)
    {
        m_toClientsBuffer = util::MemoryMappedFile::mapExisting(context.toClientsFileName().c_str());

        AtomicBuffer buffer(m_toClientsBuffer->getMemoryPtr(), m_toClientsBuffer->getMemorySize());

        m_toClientsBroadcastReceiver = std::unique_ptr<BroadcastReceiver>(new BroadcastReceiver(buffer));
        m_toClientsCopyReceiver = std::unique_ptr<CopyBroadcastReceiver>(new CopyBroadcastReceiver(*m_toClientsBroadcastReceiver));
    }

    return *m_toClientsCopyReceiver;
}