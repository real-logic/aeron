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

#define _DISABLE_EXTENDED_ALIGNED_STORAGE

#include "Aeron.h"

namespace aeron
{

static const std::chrono::duration<long, std::milli> IDLE_SLEEP_MS(4);
static const std::chrono::duration<long, std::milli> IDLE_SLEEP_MS_1(1);
static const std::chrono::duration<long, std::milli> IDLE_SLEEP_MS_16(16);
static const std::chrono::duration<long, std::milli> IDLE_SLEEP_MS_100(100);

static const char *AGENT_NAME = "client-conductor";

MemoryMappedFile::ptr_t mapCncFile(const std::string cncFileName, long mediaDriverTimeoutMs);

Aeron::Aeron(Context &context) :
    m_randomEngine(m_randomDevice()),
    m_sessionIdDistribution(-INT_MAX, INT_MAX),
    m_context(context.conclude()),
    m_cncBuffer(mapCncFile(m_context.cncFileName(), m_context.mediaDriverTimeout())),
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
        m_context.m_onCloseClientHandler,
        m_context.m_mediaDriverTimeout,
        m_context.m_resourceLingerTimeout,
        CncFileDescriptor::clientLivenessTimeout(m_cncBuffer),
        m_context.m_preTouchMappedMemory),
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

MemoryMappedFile::ptr_t mapCncFile(const std::string cncFileName, long mediaDriverTimeoutMs)
{
    const long long startMs = currentTimeMillis();
    MemoryMappedFile::ptr_t cncBuffer;
    bool heartBeatReaded = false;
    std::int64_t heartbeatTime = 0;

    while (cncBuffer == nullptr)
    {
        if (currentTimeMillis() > (startMs + mediaDriverTimeoutMs))
        {
            throw DriverTimeoutException("CnC file not map properly: " + cncFileName, SOURCEINFO);
        }

        if (MemoryMappedFile::getFileSize(cncFileName.c_str())
            <= static_cast<std::int64_t>(CncFileDescriptor::META_DATA_LENGTH))
        {
            std::this_thread::sleep_for(IDLE_SLEEP_MS_16);
            continue;
        }

        try
        {
            cncBuffer = MemoryMappedFile::mapExisting(cncFileName.c_str());
        }
        catch (IOException&)
        {
            cncBuffer = nullptr;
        }
        if (cncBuffer == nullptr || cncBuffer->getMemoryPtr() == nullptr)
        {
            cncBuffer = nullptr;
            std::this_thread::sleep_for(IDLE_SLEEP_MS_1);
            continue;
        }

        std::int32_t cncVersion = 0;

        if (0 == (cncVersion = CncFileDescriptor::cncVersionVolatile(cncBuffer)))
        {
            cncBuffer = nullptr;
            std::this_thread::sleep_for(IDLE_SLEEP_MS_1);
            continue;
        }

        if (semanticVersionMajor(cncVersion) != semanticVersionMajor(CncFileDescriptor::CNC_VERSION))
        {
            throw AeronException(
                "Aeron CnC version does not match: app=" + semanticVersionToString(CncFileDescriptor::CNC_VERSION) +
                " file=" + semanticVersionToString(cncVersion),
                SOURCEINFO);
        }

        AtomicBuffer metaDataBuffer(cncBuffer->getMemoryPtr(), convertSizeToIndex(cncBuffer->getMemorySize()));

        const CncFileDescriptor::MetaDataDefn &metaData = metaDataBuffer.overlayStruct<CncFileDescriptor::MetaDataDefn>(0);

        /* make sure the cnc.dat have valid file length before init mpsc */
        size_t totalLengthOfBuffers = (size_t)(metaData.toDriverBufferLength +
            metaData.toClientsBufferLength +
            metaData.counterValuesBufferLength +
            metaData.counterMetadataBufferLength +
            metaData.errorLogBufferLength);
        size_t cncFileSize = BitUtil::align(
            CncFileDescriptor::META_DATA_LENGTH + totalLengthOfBuffers,
            static_cast<size_t>(LogBufferDescriptor::AERON_PAGE_MIN_SIZE));
        if (cncBuffer->getMemorySize() < cncFileSize)
        {
            cncBuffer = nullptr;
            std::this_thread::sleep_for(IDLE_SLEEP_MS_1);
            continue;
        }

        AtomicBuffer toDriverBuffer(CncFileDescriptor::createToDriverBuffer(cncBuffer));
        ManyToOneRingBuffer ringBuffer(toDriverBuffer);

        if (!heartBeatReaded)
        {
            heartBeatReaded = true;
            heartbeatTime = ringBuffer.consumerHeartbeatTime();
            std::this_thread::sleep_for(IDLE_SLEEP_MS_1);
        }

        const long long timeMs = currentTimeMillis();
        std::int64_t heartbeatTimeCurrent = ringBuffer.consumerHeartbeatTime();

        if (heartbeatTimeCurrent == 0 ||
            heartbeatTime == heartbeatTimeCurrent ||
            heartbeatTimeCurrent < (timeMs - mediaDriverTimeoutMs))
        {
            cncBuffer = nullptr;
            std::this_thread::sleep_for(IDLE_SLEEP_MS_1);
        }
    }

    return cncBuffer;
}

std::string Aeron::version()
{
    return std::string("aeron version " AERON_VERSION_TXT " built " __DATE__ " " __TIME__);
}

}
