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

    if (nullptr != m_driverProxy)
    {
        delete m_driverProxy;
        m_driverProxy = nullptr;
    }

    if (nullptr != m_toDriverRingBuffer)
    {
        delete m_toDriverRingBuffer;
        m_toDriverRingBuffer = nullptr;
    }

    if (nullptr != m_toClientsCopyReceiver)
    {
        delete m_toClientsCopyReceiver;
        m_toClientsCopyReceiver = nullptr;
    }

    if (nullptr != m_toClientsBroadcastReceiver)
    {
        delete m_toClientsBroadcastReceiver;
        m_toClientsBroadcastReceiver = nullptr;
    }

    // memory mapped files should be free'd by the destructor of the shared_ptr

    // TODO: do cleanup of anything created
}

DriverProxy& Aeron::createDriverProxy(Context& context)
{
    ManyToOneRingBuffer* toDriverRingBuffer = context.toDriverBuffer();

    if (nullptr == toDriverRingBuffer)
    {
        // create buffer from mapped file for location specified in context. Managed by shared_ptr.
        m_toDriverBuffer = util::MemoryMappedFile::mapExisting(context.toDriverFileName().c_str());

        // stack temporary for AtomicBuffer. AtomicBuffer within ring buffer will be copy-constructed
        AtomicBuffer buffer(m_toDriverBuffer->getMemoryPtr(), m_toDriverBuffer->getMemorySize());

        toDriverRingBuffer = new ManyToOneRingBuffer(buffer);
        m_toDriverRingBuffer = toDriverRingBuffer;  // keep for cleanup
    }

    m_driverProxy = new DriverProxy(*toDriverRingBuffer);  // keep for cleanup

    return *m_driverProxy;
}

CopyBroadcastReceiver& Aeron::createDriverReceiver(Context &context)
{
    CopyBroadcastReceiver* toClientsCopyReceiver = context.toClientsBuffer();

    if (nullptr == toClientsCopyReceiver)
    {
        m_toClientsBuffer = util::MemoryMappedFile::mapExisting(context.toClientsFileName().c_str());

        AtomicBuffer buffer(m_toClientsBuffer->getMemoryPtr(), m_toClientsBuffer->getMemorySize());

        m_toClientsBroadcastReceiver = new BroadcastReceiver(buffer);
        toClientsCopyReceiver = new CopyBroadcastReceiver(*m_toClientsBroadcastReceiver);
        m_toClientsCopyReceiver = toClientsCopyReceiver;
    }

    return *toClientsCopyReceiver;
}