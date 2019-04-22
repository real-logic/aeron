/*
 * Copyright 2014-2019 Real Logic Ltd.
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

#define _DISABLE_EXTENDED_ALIGNED_STORAGE
#include "Aeron.h"

namespace aeron {

static const std::chrono::duration<long, std::milli> IDLE_SLEEP_MS(4);
static const std::chrono::duration<long, std::milli> IDLE_SLEEP_MS_1(1);
static const std::chrono::duration<long, std::milli> IDLE_SLEEP_MS_16(16);
static const std::chrono::duration<long, std::milli> IDLE_SLEEP_MS_100(100);

static const char* AGENT_NAME = "client-conductor";

Aeron::Aeron(Context &context) :
    m_randomEngine(m_randomDevice()),
    m_sessionIdDistribution(-INT_MAX, INT_MAX),
    m_context(context.conclude()),
    m_cncBuffer(mapCncFile(m_context)),
    m_toDriverAtomicBuffer(CncFileDescriptor::createToDriverBuffer(m_cncBuffer)),
    m_toClientsAtomicBuffer(CncFileDescriptor::createToClientsBuffer(m_cncBuffer)),
    m_countersMetadataBuffer(CncFileDescriptor::createCounterMetadataBuffer(m_cncBuffer)),
    m_countersValueBuffer(CncFileDescriptor::createCounterValuesBuffer(m_cncBuffer)),
    m_toDriverRingBuffer(m_toDriverAtomicBuffer),
    m_driverProxy(m_toDriverRingBuffer),
    m_toClientsBroadcastReceiver(m_toClientsAtomicBuffer),
    m_toClientsCopyReceiver(m_toClientsBroadcastReceiver),
    m_conductor(
        currentTimeMillis,
        m_driverProxy,
        m_toClientsCopyReceiver,
        m_countersMetadataBuffer,
        m_countersValueBuffer,
        m_context.m_onNewPublicationHandler,
        m_context.m_onNewExclusivePublicationHandler,
        m_context.m_onNewSubscriptionHandler,
        m_context.m_exceptionHandler,
        m_context.m_onAvailableCounterHandler,
        m_context.m_onUnavailableCounterHandler,
        m_context.m_mediaDriverTimeout,
        m_context.m_resourceLingerTimeout,
        CncFileDescriptor::clientLivenessTimeout(m_cncBuffer)),
    m_idleStrategy(IDLE_SLEEP_MS),
    m_conductorRunner(m_conductor, m_idleStrategy, m_context.m_exceptionHandler, AGENT_NAME),
    m_conductorInvoker(m_conductor, m_context.m_exceptionHandler)
{
    if (m_context.m_useConductorAgentInvoker)
    {
        m_conductorInvoker.start();
    }
    else
    {
        m_conductorRunner.start();
    }
}

Aeron::~Aeron()
{
    if (m_context.m_useConductorAgentInvoker)
    {
        m_conductorInvoker.close();
    }
    else
    {
        m_conductorRunner.close();
    }

    // memory mapped files should be free'd by the destructor of the shared_ptr
}

inline MemoryMappedFile::ptr_t Aeron::mapCncFile(Context &context)
{
    const long long startMs = currentTimeMillis();
    MemoryMappedFile::ptr_t cncBuffer;

    while (true)
    {
        while (MemoryMappedFile::getFileSize(context.cncFileName().c_str()) <= 0)
        {
            if (currentTimeMillis() > (startMs + context.m_mediaDriverTimeout))
            {
                throw DriverTimeoutException("CnC file not created: " + context.cncFileName(), SOURCEINFO);
            }

            std::this_thread::sleep_for(IDLE_SLEEP_MS_16);
        }

        cncBuffer = MemoryMappedFile::mapExisting(context.cncFileName().c_str());

        std::int32_t cncVersion = 0;

        while (0 == (cncVersion = CncFileDescriptor::cncVersionVolatile(cncBuffer)))
        {
            if (currentTimeMillis() > (startMs + context.m_mediaDriverTimeout))
            {
                throw DriverTimeoutException(
                    "CnC file is created but not initialised: " + context.cncFileName(), SOURCEINFO);
            }

            std::this_thread::sleep_for(IDLE_SLEEP_MS_1);
        }

        if (CncFileDescriptor::CNC_VERSION != cncVersion)
        {
            throw util::IllegalStateException(
                "CnC file version not supported: " + std::to_string(cncVersion), SOURCEINFO);
        }

        AtomicBuffer toDriverBuffer(CncFileDescriptor::createToDriverBuffer(cncBuffer));
        ManyToOneRingBuffer ringBuffer(toDriverBuffer);

        while (0 == ringBuffer.consumerHeartbeatTime())
        {
            if (currentTimeMillis() > (startMs + context.m_mediaDriverTimeout))
            {
                throw DriverTimeoutException(std::string("no driver heartbeat detected"), SOURCEINFO);
            }

            std::this_thread::sleep_for(IDLE_SLEEP_MS_1);
        }

        const long long timeMs = currentTimeMillis();
        if (ringBuffer.consumerHeartbeatTime() < (timeMs - context.m_mediaDriverTimeout))
        {
            if (timeMs > (startMs + context.m_mediaDriverTimeout))
            {
                throw DriverTimeoutException(std::string("no driver heartbeat detected"), SOURCEINFO);
            }

            cncBuffer = nullptr;

            std::this_thread::sleep_for(IDLE_SLEEP_MS_100);
            continue;
        }

        break;
    }

    return cncBuffer;
}

std::string Aeron::version()
{
    return std::string("aeron version " AERON_VERSION_TXT " built " __DATE__ " " __TIME__);
}

}
