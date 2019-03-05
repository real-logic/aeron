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
#include "RecordingDescriptorPoller.h"
#include "RecordingSubscriptionDescriptorPoller.h"
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
        std::unique_ptr<RecordingDescriptorPoller> recordingDescriptorPoller,
        std::unique_ptr<RecordingSubscriptionDescriptorPoller> recordingSubscriptionDescriptorPoller,
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

    inline RecordingDescriptorPoller& recordingDescriptorPoller()
    {
        return *m_recordingDescriptorPoller;
    }

    inline RecordingSubscriptionDescriptorPoller& recordingSubscriptionDescriptorPoller()
    {
        return *m_recordingSubscriptionDescriptorPoller;
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
            throw ArchiveException("failed to send start recording request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>(correlationId);
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t extendRecording(
        std::int64_t recordingId,
        const std::string& channel,
        std::int32_t streamId,
        SourceLocation sourceLocation)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        const std::int64_t correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->extendRecording<IdleStrategy>(
            channel, streamId, (sourceLocation == SourceLocation::LOCAL), recordingId, correlationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send extend recording request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>(correlationId);
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void stopRecording(
        const std::string& channel,
        std::int32_t streamId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        const std::int64_t correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->stopRecording<IdleStrategy>(
            channel, streamId, correlationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send stop recording request", SOURCEINFO);
        }

        pollForResponse<IdleStrategy>(correlationId);
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void stopRecording(std::shared_ptr<Publication> publication)
    {
        const std::string& recordingChannel = ChannelUri::addSessionId(
            publication->channel(), publication->sessionId());

        stopRecording<IdleStrategy>(recordingChannel, publication->streamId());
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void stopRecording(std::shared_ptr<ExclusivePublication> publication)
    {
        const std::string& recordingChannel = ChannelUri::addSessionId(
            publication->channel(), publication->sessionId());

        stopRecording<IdleStrategy>(recordingChannel, publication->streamId());
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void stopRecording(std::int64_t subscriptionId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        const std::int64_t correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->stopRecording<IdleStrategy>(subscriptionId, correlationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send stop recording request", SOURCEINFO);
        }

        pollForResponse<IdleStrategy>(correlationId);
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
            throw ArchiveException("failed to send replay request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>(correlationId);
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void stopReplay(std::int64_t replaySessionId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        const std::int64_t correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->stopReplay<IdleStrategy>(replaySessionId, correlationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send stop replay request", SOURCEINFO);
        }

        pollForResponse<IdleStrategy>(correlationId);
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::shared_ptr<Subscription> replay(
        std::int64_t recordingId,
        std::int64_t position,
        std::int64_t length,
        const std::string& replayChannel,
        std::int32_t replayStreamId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        std::shared_ptr<ChannelUri> replayChannelUri = ChannelUri::parse(replayChannel);
        const std::int64_t correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->replay<IdleStrategy>(
            recordingId, position, length, replayChannel, replayStreamId, correlationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send replay request", SOURCEINFO);
        }

        auto replaySessionId = static_cast<std::int32_t>(pollForResponse<IdleStrategy>(correlationId));
        replayChannelUri->put(SESSION_ID_PARAM_NAME, std::to_string(replaySessionId));

        const std::int64_t subscriptionId = m_aeron->addSubscription(replayChannelUri->toString(), replayStreamId);
        IdleStrategy idle;

        std::shared_ptr<Subscription> subscription = m_aeron->findSubscription(subscriptionId);
        while (!subscription)
        {
            idle.idle();
            subscription = m_aeron->findSubscription(subscriptionId);
        }

        return subscription;
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::shared_ptr<Subscription> replay(
        std::int64_t recordingId,
        std::int64_t position,
        std::int64_t length,
        const std::string& replayChannel,
        std::int32_t replayStreamId,
        const on_available_image_t& availableImageHandler,
        const on_unavailable_image_t& unavailableImageHandler)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        std::shared_ptr<ChannelUri> replayChannelUri = ChannelUri::parse(replayChannel);
        const std::int64_t correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->replay<IdleStrategy>(
            recordingId, position, length, replayChannel, replayStreamId, correlationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send replay request", SOURCEINFO);
        }

        auto replaySessionId = static_cast<std::int32_t>(pollForResponse<IdleStrategy>(correlationId));
        replayChannelUri->put(SESSION_ID_PARAM_NAME, std::to_string(replaySessionId));

        const std::int64_t subscriptionId = m_aeron->addSubscription(
            replayChannelUri->toString(), replayStreamId, availableImageHandler, unavailableImageHandler);
        IdleStrategy idle;

        std::shared_ptr<Subscription> subscription = m_aeron->findSubscription(subscriptionId);
        while (!subscription)
        {
            idle.idle();
            subscription = m_aeron->findSubscription(subscriptionId);
        }

        return subscription;
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int32_t listRecordings(
        std::int64_t fromRecordingId, std::int32_t recordCount, const recording_descriptor_consumer_t& consumer)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        const std::int64_t correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->listRecordings<IdleStrategy>(
            fromRecordingId, recordCount, correlationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send list recordings request", SOURCEINFO);
        }

        return pollForDescriptors<IdleStrategy>(correlationId, recordCount, consumer);
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int32_t listRecordingsForUri(
        std::int64_t fromRecordingId,
        std::int32_t recordCount,
        const std::string& channelFragment,
        std::int32_t streamId,
        const recording_descriptor_consumer_t& consumer)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        const std::int64_t correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->listRecordingsForUri<IdleStrategy>(
            fromRecordingId, recordCount, channelFragment, streamId, correlationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send list recordings request", SOURCEINFO);
        }

        return pollForDescriptors<IdleStrategy>(correlationId, recordCount, consumer);
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int32_t listRecording(std::int64_t recordingId, const recording_descriptor_consumer_t& consumer)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        const std::int64_t correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->listRecording<IdleStrategy>(recordingId, correlationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send list recording request", SOURCEINFO);
        }

        return pollForDescriptors<IdleStrategy>(correlationId, 1, consumer);
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t getRecordingPosition(std::int64_t recordingId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        const std::int64_t correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->getRecordingPosition<IdleStrategy>(recordingId, correlationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send get recording position request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>(correlationId);
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t getStopPosition(std::int64_t recordingId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        const std::int64_t correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->getStopPosition<IdleStrategy>(recordingId, correlationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send get stop position request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>(correlationId);
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t findLastMatchingRecording(
        std::int64_t minRecordingId,
        const std::string& channelFragment,
        std::int32_t streamId,
        std::int32_t sessionId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        const std::int64_t correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->findLastMatchingRecording<IdleStrategy>(
            minRecordingId, channelFragment, streamId, sessionId, correlationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send find last matching recording request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>(correlationId);
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void truncateRecording(std::int64_t recordingId, std::int64_t position)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        const std::int64_t correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->truncateRecording<IdleStrategy>(recordingId, position, correlationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send truncate recording request", SOURCEINFO);
        }

        pollForResponse<IdleStrategy>(correlationId);
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int32_t listRecordingSubscriptions(
        std::int32_t pseudoIndex,
        std::int32_t subscriptionCount,
        const std::string& channelFragment,
        std::int32_t streamId,
        bool applyStreamId,
        const recording_subscription_descriptor_consumer_t& consumer)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        const std::int64_t correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->listRecordingSubscriptions<IdleStrategy>(
            pseudoIndex, subscriptionCount, channelFragment, streamId, applyStreamId, correlationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send list recording subscriptions request", SOURCEINFO);
        }

        return pollForSubscriptionDescriptors<IdleStrategy>(correlationId, subscriptionCount, consumer);
    }

private:
    std::unique_ptr<Context_t> m_ctx;
    std::unique_ptr<ArchiveProxy> m_archiveProxy;
    std::unique_ptr<ControlResponsePoller> m_controlResponsePoller;
    std::unique_ptr<RecordingDescriptorPoller> m_recordingDescriptorPoller;
    std::unique_ptr<RecordingSubscriptionDescriptorPoller> m_recordingSubscriptionDescriptorPoller;
    std::shared_ptr<Aeron> m_aeron;

    std::recursive_mutex m_lock;
    nano_clock_t m_nanoClock;

    const std::int64_t m_controlSessionId;
    const long long m_messageTimeoutNs;
    bool m_isClosed = false;

    inline void ensureOpen()
    {
        if (m_isClosed)
        {
            throw ArchiveException("client is closed", SOURCEINFO);
        }
    }

    inline void checkDeadline(long long deadlineNs, const std::string& errorMessage, std::int64_t correlationId)
    {
        if ((deadlineNs - m_nanoClock()) < 0)
        {
            throw TimeoutException(errorMessage + " - correlationId=" + std::to_string(correlationId), SOURCEINFO);
        }
    }

    inline void invokeAeronClient()
    {
        if (m_aeron->usesAgentInvoker())
        {
            m_aeron->conductorAgentInvoker().invoke();
        }
    }

    template<typename IdleStrategy>
    inline void pollNextResponse(std::int64_t correlationId, long long deadlineNs, ControlResponsePoller& poller)
    {
        IdleStrategy idle;

        while (true)
        {
            const int fragments = poller.poll();

            if (poller.isPollComplete())
            {
                break;
            }

            if (fragments > 0)
            {
                continue;
            }

            if (!poller.subscription()->isConnected())
            {
                throw ArchiveException("subscription to archive is not connected", SOURCEINFO);
            }

            checkDeadline(deadlineNs, "awaiting response", correlationId);
            idle.idle();
            invokeAeronClient();
        }
    }

    template<typename IdleStrategy>
    inline std::int64_t pollForResponse(std::int64_t correlationId)
    {
        const long long deadlineNs = m_nanoClock() + m_messageTimeoutNs;

        while (true)
        {
            pollNextResponse<IdleStrategy>(correlationId, deadlineNs, *m_controlResponsePoller);

            if (m_controlResponsePoller->controlSessionId() != controlSessionId() ||
                !m_controlResponsePoller->isControlResponse())
            {
                invokeAeronClient();
                continue;
            }

            if (m_controlResponsePoller->isCodeError())
            {
                if (m_controlResponsePoller->correlationId() == correlationId)
                {
                    throw ArchiveException(
                        static_cast<std::int32_t>(m_controlResponsePoller->relevantId()),
                        "response for correlationId=" + std::to_string(correlationId)
                            + ", error: " + m_controlResponsePoller->errorMessage(),
                        SOURCEINFO);
                }
                else if (m_ctx->errorHandler() != nullptr)
                {
                    ArchiveException ex(
                        static_cast<std::int32_t>(m_controlResponsePoller->relevantId()),
                        "response for correlationId=" + std::to_string(correlationId)
                            + ", error: " + m_controlResponsePoller->errorMessage(),
                        SOURCEINFO);
                    m_ctx->errorHandler()(ex);
                }
            }
            else if (m_controlResponsePoller->correlationId() == correlationId)
            {
                if (!m_controlResponsePoller->isCodeOk())
                {
                    throw ArchiveException(
                        "unexpected response code: " + std::to_string(m_controlResponsePoller->codeValue()),
                        SOURCEINFO);
                }

                return m_controlResponsePoller->relevantId();
            }
        }
    }

    template<typename IdleStrategy>
    std::int32_t pollForDescriptors(
        std::int64_t correlationId, std::int32_t recordCount, const recording_descriptor_consumer_t& consumer)
    {
        std::int32_t existingRemainCount = recordCount;
        long long deadlineNs = m_nanoClock() + m_messageTimeoutNs;
        IdleStrategy idle;

        m_recordingDescriptorPoller->reset(correlationId, recordCount, consumer);

        while (true)
        {
            const int fragments = m_recordingDescriptorPoller->poll();
            const std::int32_t remainingRecordCount = m_recordingDescriptorPoller->remainingRecordCount();

            if (m_recordingDescriptorPoller->isDispatchComplete())
            {
                return recordCount - remainingRecordCount;
            }

            if (remainingRecordCount != existingRemainCount)
            {
                existingRemainCount = remainingRecordCount;
                deadlineNs = m_nanoClock() + m_messageTimeoutNs;
            }

            invokeAeronClient();

            if (fragments > 0)
            {
                continue;
            }

            if (!m_recordingDescriptorPoller->subscription()->isConnected())
            {
                throw ArchiveException("subscription to archive is not connected", SOURCEINFO);
            }

            checkDeadline(deadlineNs, "awaiting recording descriptors", correlationId);
            idle.idle();
        }
    }

    template<typename IdleStrategy>
    std::int32_t pollForSubscriptionDescriptors(
        std::int64_t correlationId,
        std::int32_t subscriptionCount,
        const recording_subscription_descriptor_consumer_t& consumer)
    {
        std::int32_t existingRemainCount = subscriptionCount;
        long long deadlineNs = m_nanoClock() + m_messageTimeoutNs;
        IdleStrategy idle;

        m_recordingSubscriptionDescriptorPoller->reset(correlationId, subscriptionCount, consumer);

        while (true)
        {
            const int fragments = m_recordingSubscriptionDescriptorPoller->poll();
            const std::int32_t remainingSubscriptionCount =
                m_recordingSubscriptionDescriptorPoller->remainingSubscriptionCount();

            if (m_recordingSubscriptionDescriptorPoller->isDispatchComplete())
            {
                return subscriptionCount - remainingSubscriptionCount;
            }

            if (remainingSubscriptionCount != existingRemainCount)
            {
                existingRemainCount = remainingSubscriptionCount;
                deadlineNs = m_nanoClock() + m_messageTimeoutNs;
            }

            invokeAeronClient();

            if (fragments > 0)
            {
                continue;
            }

            if (!m_recordingSubscriptionDescriptorPoller->subscription()->isConnected())
            {
                throw ArchiveException("subscription to archive is not connected", SOURCEINFO);
            }

            checkDeadline(deadlineNs, "awaiting subscription descriptors", correlationId);
            idle.idle();
        }
    }
};

}}}
#endif //AERON_ARCHIVE_AERONARCHIVE_H
