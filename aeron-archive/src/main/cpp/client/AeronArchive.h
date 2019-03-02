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
#ifndef AERON_ARCHIVE_AERONARCHIVE_H
#define AERON_ARCHIVE_AERONARCHIVE_H

#include "Aeron.h"
#include "ChannelUri.h"
#include "ArchiveConfiguration.h"
#include "ArchiveProxy.h"
#include "ControlResponsePoller.h"
#include "concurrent/BackOffIdleStrategy.h"
#include "concurrent/YieldingIdleStrategy.h"
#include "ArchiveException.h"

namespace aeron {
namespace archive {
namespace client {

class AeronArchive
{
public:
    using Context_t = aeron::archive::client::Context;

    AeronArchive(
        std::unique_ptr<Context_t> ctx,
        std::unique_ptr<ArchiveProxy> archiveProxy,
        std::unique_ptr<ControlResponsePoller> controlResponsePoller,
        std::shared_ptr<Aeron> aeron,
        std::int64_t controlSessionId);
    ~AeronArchive();

    enum SourceLocation: int
    {
        LOCAL = 0,
        REMOTE = 1
    };

    class AsyncConnect
    {
    public:
        AsyncConnect(
            Context_t& context, std::shared_ptr<Aeron> aeron, std::int64_t subscriptionId, std::int64_t publicationId);

        std::shared_ptr<AeronArchive> poll();
    private:
        std::unique_ptr<Context_t> m_ctx;
        std::unique_ptr<ArchiveProxy> m_archiveProxy;
        std::unique_ptr<ControlResponsePoller> m_controlResponsePoller;
        std::shared_ptr<Aeron> m_aeron;
        std::shared_ptr<Subscription> m_subscription;
        std::shared_ptr<ExclusivePublication> m_publication;
        const std::int64_t m_subscriptionId;
        const std::int64_t m_publicationId;
        std::int64_t m_connectCorrelationId = aeron::NULL_VALUE;
        std::uint8_t m_step = 0;
    };

    static std::shared_ptr<AsyncConnect> asyncConnect(Context_t& ctx);

    inline static std::shared_ptr<AsyncConnect> asyncConnect()
    {
        Context_t ctx;
        return AeronArchive::asyncConnect(ctx);
    }

    template<typename ConnectIdleStrategy = aeron::concurrent::YieldingIdleStrategy>
    inline static std::shared_ptr<AeronArchive> connect(Context_t& context)
    {
        std::shared_ptr<AsyncConnect> asyncConnect = AeronArchive::asyncConnect(context);
        std::shared_ptr<Aeron> aeron = context.aeron();
        ConnectIdleStrategy idle;

        std::shared_ptr<AeronArchive> archive = asyncConnect->poll();
        while (!archive)
        {
            if (aeron->usesAgentInvoker())
            {
                aeron->conductorAgentInvoker().invoke();
            }

            idle.idle();
            archive = asyncConnect->poll();
        }

        return archive;
    }

    inline static std::shared_ptr<AeronArchive> connect()
    {
        Context_t ctx;
        return AeronArchive::connect(ctx);
    }

    inline Context_t& context()
    {
        return *m_ctx;
    }

    inline std::int64_t controlSessionId()
    {
        return m_controlSessionId;
    }

    inline ArchiveProxy& archiveProxy()
    {
        return *m_archiveProxy;
    }

    inline ControlResponsePoller& controlResponsePoller()
    {
        return *m_controlResponsePoller;
    }

    inline std::string pollForErrorResponse()
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        if (m_controlResponsePoller->poll() != 0 && m_controlResponsePoller->isPollComplete())
        {
            if (m_controlResponsePoller->controlSessionId() == m_controlSessionId &&
                m_controlResponsePoller->isControlResponse() &&
                m_controlResponsePoller->isCodeError())
            {
                return m_controlResponsePoller->errorMessage();
            }
        }

        return std::string();
    }

    inline void checkForErrorResponse()
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        if (m_controlResponsePoller->poll() != 0 && m_controlResponsePoller->isPollComplete())
        {
            if (m_controlResponsePoller->controlSessionId() == m_controlSessionId &&
                m_controlResponsePoller->isControlResponse() &&
                m_controlResponsePoller->isCodeError())
            {
                if (m_ctx->errorHandler() != nullptr)
                {
                    ArchiveException ex(
                        static_cast<std::int32_t>(m_controlResponsePoller->relevantId()),
                        m_controlResponsePoller->errorMessage(),
                        SOURCEINFO);
                    m_ctx->errorHandler()(ex);
                }
                else
                {
                    throw ArchiveException(
                        static_cast<std::int32_t>(m_controlResponsePoller->relevantId()),
                        m_controlResponsePoller->errorMessage(),
                        SOURCEINFO);
                }
            }
        }
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::shared_ptr<Publication> addRecordedPublication(const std::string& channel, std::int32_t streamId)
    {
        std::shared_ptr<Publication> publication;
        IdleStrategy idle;

        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        const std::int64_t publicationId = m_aeron->addPublication(channel, streamId);
        publication = m_aeron->findPublication(publicationId);
        while (!publication)
        {
            idle.idle();
            publication = m_aeron->findPublication(publicationId);
        }

        if (!publication->isOriginal())
        {
            throw ArchiveException(
                "publication already added for channel=" + channel + " streamId=" + std::to_string(streamId),
                SOURCEINFO);
        }

        startRecording<IdleStrategy>(
            ChannelUri::addSessionId(channel, publication->sessionId()), streamId, SourceLocation::LOCAL);

        return publication;
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::shared_ptr<ExclusivePublication> addRecordedExclusivePublication(
        const std::string& channel, std::int32_t streamId)
    {
        std::shared_ptr<ExclusivePublication> publication;
        IdleStrategy idle;

        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        const std::int64_t publicationId = m_aeron->addExclusivePublication(channel, streamId);
        publication = m_aeron->findExclusivePublication(publicationId);
        while (!publication)
        {
            idle.idle();
            publication = m_aeron->findExclusivePublication(publicationId);
        }

        startRecording<IdleStrategy>(
            ChannelUri::addSessionId(channel, publication->sessionId()), streamId, SourceLocation::LOCAL);

        return publication;
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t startRecording(
        const std::string& channel,
        std::int32_t streamId,
        SourceLocation sourceLocation)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        const std::int64_t correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->startRecording<IdleStrategy>(
            channel, streamId, (sourceLocation == SourceLocation::LOCAL), correlationId, m_controlSessionId))
        {
            throw ArchiveException("fialed to send start recording request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>(correlationId);
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t startReplay(
        std::int64_t recordingId,
        std::int64_t position,
        std::int64_t length,
        const std::string& replayChannel,
        std::int32_t replayStreamId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        const std::int64_t correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->replay<IdleStrategy>(
            recordingId, position, length, replayChannel, replayStreamId, correlationId, m_controlSessionId))
        {
            throw ArchiveException("fialed to send replay request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>(correlationId);
    }

private:
    std::unique_ptr<Context_t> m_ctx;
    std::unique_ptr<ArchiveProxy> m_archiveProxy;
    std::unique_ptr<ControlResponsePoller> m_controlResponsePoller;
    std::shared_ptr<Aeron> m_aeron;

    std::recursive_mutex m_lock;

    const std::int64_t m_controlSessionId;
    bool m_isClosed = false;

    inline void ensureOpen()
    {
        if (m_isClosed)
        {
            throw ArchiveException("client is closed", SOURCEINFO);
        }
    }

    template<typename IdleStrategy>
    int pollForResponse(std::int64_t correlationId)
    {
        IdleStrategy idle;

        // TODO: finish

        return 0;
    }
};

}}}
#endif //AERON_ARCHIVE_AERONARCHIVE_H
