/*
 * Copyright 2014-2024 Real Logic Limited.
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
#ifndef AERON_ARCHIVE_AERON_ARCHIVE_H
#define AERON_ARCHIVE_AERON_ARCHIVE_H

#include "ArchiveConfiguration.h"
#include "ArchiveProxy.h"
#include "ControlResponsePoller.h"
#include "RecordingDescriptorPoller.h"
#include "RecordingSubscriptionDescriptorPoller.h"
#include "concurrent/BackOffIdleStrategy.h"
#include "concurrent/YieldingIdleStrategy.h"
#include "ArchiveException.h"
#include "AeronCounters.h"

namespace aeron { namespace archive { namespace client
{

/**
 * Client is not currently connected to an active archive error message.
 */
constexpr const char NOT_CONNECTED_MSG[] = "not connected";

/**
 * Client for interacting with a local or remote Aeron Archive for requesting the recording and replay message streams.
 * <p>
 * This client provides a simple interaction model which is mostly synchronous and may not be optimal.
 * The underlying components such as the ArchiveProxy and the ControlResponsePoller or
 * RecordingDescriptorPoller may be used directly if a more asynchronous interaction is required.
 * <p>
 * Note: This class is threadsafe.
 */
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
        std::int64_t controlSessionId,
        std::int64_t archiveId);

    ~AeronArchive();

    /// Location of the source with respect to the archive.
    enum SourceLocation : int
    {
        /// Source is local to the archive and will be recorded using a spy Subscription.
        LOCAL = 0,

        /// Source is remote to the archive and will be recorded using a network Subscription.
        REMOTE = 1
    };

    /**
     * Allows for the async establishment of a archive session.
     */
    class AsyncConnect
    {
    public:
        AsyncConnect(
            Context_t &context,
            std::shared_ptr<Aeron> aeron,
            std::int64_t subscriptionId,
            std::int64_t publicationId,
            long long deadlineNs);

        /// Represents connection state
        enum State : std::uint8_t
        {
            ADD_PUBLICATION = 0,
            AWAIT_PUBLICATION_CONNECTED = 1,
            SEND_CONNECT_REQUEST = 2,
            AWAIT_SUBSCRIPTION_CONNECTED = 3,
            AWAIT_CONNECT_RESPONSE = 4,
            SEND_ARCHIVE_ID_REQUEST = 5,
            AWAIT_ARCHIVE_ID_RESPONSE = 6,
            DONE = 7,
            SEND_CHALLENGE_RESPONSE = 8,
            AWAIT_CHALLENGE_RESPONSE = 9
        };

        /**
         * Poll for a complete connection.
         *
         * @return a new AeronArchive if successfully connected otherwise null.
         */
        std::shared_ptr<AeronArchive> poll();

        /**
         * The step in the connect process this connect attempt has reached.
         *
         * @return the step in the connect process this connect attempt has reached.
         */
        inline std::uint8_t step() const
        {
            return m_state;
        }

        /**
         * The state in the connect process this connect attempt has reached.
         *
         * @return the connection state.
         */
        inline State state() const
        {
            return m_state;
        }

    private:
        nano_clock_t m_nanoClock;
        std::unique_ptr<Context_t> m_ctx;
        std::unique_ptr<ArchiveProxy> m_archiveProxy;
        std::unique_ptr<ControlResponsePoller> m_controlResponsePoller;
        std::shared_ptr<Aeron> m_aeron;
        std::shared_ptr<Subscription> m_subscription;
        std::shared_ptr<ExclusivePublication> m_publication;
        const std::int64_t m_subscriptionId;
        const std::int64_t m_publicationId;
        const long long m_deadlineNs;
        std::int64_t m_correlationId = aeron::NULL_VALUE;
        std::int64_t m_controlSessionId = aeron::NULL_VALUE;
        State m_state = State::ADD_PUBLICATION;
        std::pair<const char *, std::uint32_t> m_encodedCredentialsFromChallenge = { nullptr, 0 };

        std::shared_ptr<AeronArchive> transitionToDone(std::int64_t archiveId)
        {
            if (!m_archiveProxy->keepAlive(aeron::NULL_VALUE, m_controlSessionId))
            {
                m_archiveProxy->closeSession(m_controlSessionId);
                throw ArchiveException("failed to send keep alive after archive connect",SOURCEINFO);
            }

            m_state = State::DONE;

            std::unique_ptr<RecordingDescriptorPoller> recordingDescriptorPoller(
                new RecordingDescriptorPoller(
                    m_subscription, m_ctx->errorHandler(), m_ctx->recordingSignalConsumer(), m_controlSessionId));
            std::unique_ptr<RecordingSubscriptionDescriptorPoller> recordingSubscriptionDescriptorPoller(
                new RecordingSubscriptionDescriptorPoller(
                    m_subscription, m_ctx->errorHandler(), m_ctx->recordingSignalConsumer(), m_controlSessionId));

            return std::make_shared<AeronArchive>(
                std::move(m_ctx),
                std::move(m_archiveProxy),
                std::move(m_controlResponsePoller),
                std::move(recordingDescriptorPoller),
                std::move(recordingSubscriptionDescriptorPoller),
                m_aeron,
                m_controlSessionId,
                archiveId);
        }
    };

    /**
     * Begin an attempt at creating a connection which can be completed by calling AsyncConnect#poll.
     *
     * @param ctx for the archive connection.
     * @return the AsyncConnect that can be polled for completion.
     */
    static std::shared_ptr<AsyncConnect> asyncConnect(Context_t &ctx);

    /**
     * Begin an attempt at creating a connection which can be completed by calling AsyncConnect#poll.
     *
     * @return the AsyncConnect that can be polled for completion.
     */
    inline static std::shared_ptr<AsyncConnect> asyncConnect()
    {
        Context_t ctx;
        return AeronArchive::asyncConnect(ctx);
    }

    /**
     * Connect to an Aeron archive by providing a Context. This will create a control session.
     * <p>
     * Before connecting Context#conclude will be called.
     *
     * @param context for connection configuration.
     * @tparam ConnectIdleStrategy to use between polling calls.
     * @return the newly created Aeron Archive client.
     */
    template<typename ConnectIdleStrategy = aeron::concurrent::YieldingIdleStrategy>
    inline static std::shared_ptr<AeronArchive> connect(Context_t &context)
    {
        std::shared_ptr<AsyncConnect> asyncConnect = AeronArchive::asyncConnect(context);
        std::shared_ptr<Aeron> aeron = context.aeron();
        delegating_invoker_t &delegatingInvoker = context.delegatingInvoker();
        ConnectIdleStrategy idleStrategy;
        std::uint8_t previousStep = asyncConnect->step();

        std::shared_ptr<AeronArchive> archive = asyncConnect->poll();
        while (!archive)
        {
            if (asyncConnect->step() == previousStep)
            {
                idleStrategy.idle();
            }
            else
            {
                idleStrategy.reset();
                previousStep = asyncConnect->step();
            }

            if (aeron->usesAgentInvoker())
            {
                aeron->conductorAgentInvoker().invoke();
            }

            delegatingInvoker();

            archive = asyncConnect->poll();
        }

        return archive;
    }

    /**
     * Connect to an Aeron archive using a default Context. This will create a control session.
     *
     * @return the newly created AeronArchive client.
     */
    inline static std::shared_ptr<AeronArchive> connect()
    {
        Context_t ctx;

        return AeronArchive::connect(ctx);
    }

    /**
     * Get the Context used to connect this archive client.
     *
     * @return the Context used to connect this archive client.
     */
    inline Context_t &context()
    {
        return *m_ctx;
    }

    /**
     * The id of the archive.
     *
     * @return id of the archive this client is connected to.
     */
    inline std::int64_t archiveId() const
    {
        return m_archiveId;
    }

    /**
     * The control session id allocated for this control session to the archive.
     *
     * @return control session id allocated for this control session to the archive.
     */
    inline std::int64_t controlSessionId() const
    {
        return m_controlSessionId;
    }

    /**
     * The last used correlation id for this control session to the archive.
     *
     * @return last used correlation id for this connection to the archive.
     */
    inline std::int64_t lastCorrelationId() const
    {
        return m_lastCorrelationId;
    }

    /**
     * The ArchiveProxy for send asynchronous messages to the connected archive.
     *
     * @return the ArchiveProxy for send asynchronous messages to the connected archive.
     */
    inline ArchiveProxy &archiveProxy() const
    {
        return *m_archiveProxy;
    }

    /**
     * Get the ControlResponsePoller for polling additional events on the control channel.
     *
     * @return the ControlResponsePoller for polling additional events on the control channel.
     */
    inline ControlResponsePoller &controlResponsePoller() const
    {
        return *m_controlResponsePoller;
    }

    /**
     * Get the RecordingDescriptorPoller for polling recording descriptors on the control channel.
     *
     * @return the RecordingDescriptorPoller for polling recording descriptors on the control channel.
     */
    inline RecordingDescriptorPoller &recordingDescriptorPoller() const
    {
        return *m_recordingDescriptorPoller;
    }

    /**
     * The RecordingSubscriptionDescriptorPoller for polling subscription descriptors on the control channel.
     *
     * @return the RecordingSubscriptionDescriptorPoller for polling subscription descriptors on the control
     * channel.
     */
    inline RecordingSubscriptionDescriptorPoller &recordingSubscriptionDescriptorPoller() const
    {
        return *m_recordingSubscriptionDescriptorPoller;
    }

    /**
     * Poll the response stream once for an error. If another message is present then it will be skipped over
     * so only call when not expecting another response. If not connected then it will return NOT_CONNECTED.
     *
     * @return the error String otherwise an empty string is returned if no error is found.
     */
    inline std::string pollForErrorResponse()
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();

        if (!m_controlResponsePoller->subscription()->isConnected())
        {
            return { NOT_CONNECTED_MSG };
        }

        if (m_controlResponsePoller->poll() != 0 && m_controlResponsePoller->isPollComplete())
        {
            if (m_controlResponsePoller->controlSessionId() == m_controlSessionId)
            {
                if (m_controlResponsePoller->isControlResponse() && m_controlResponsePoller->isCodeError())
                {
                    return m_controlResponsePoller->errorMessage();
                }
                else if (m_controlResponsePoller->isRecordingSignal())
                {
                    dispatchRecordingSignal();
                }
            }
        }

        return {};
    }

    /**
     * Check if an error has been returned for the control session, or if it is no longer connected, and throw
     * an ArchiveException if Context#errorHandler is not set.
     * <p>
     * To check for an error response without raising an exception then try #pollForErrorResponse.
     *
     * @see #pollForErrorResponse
     */
    inline void checkForErrorResponse()
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();

        if (!m_controlResponsePoller->subscription()->isConnected())
        {
            if (m_ctx->errorHandler() != nullptr)
            {
                ArchiveException ex(NOT_CONNECTED_MSG, SOURCEINFO);
                m_ctx->errorHandler()(ex);
            }
            else
            {
                throw ArchiveException(NOT_CONNECTED_MSG, SOURCEINFO);
            }
        }
        else if (m_controlResponsePoller->poll() != 0 && m_controlResponsePoller->isPollComplete())
        {
            if (m_controlResponsePoller->controlSessionId() == m_controlSessionId)
            {
                if (m_controlResponsePoller->isControlResponse() && m_controlResponsePoller->isCodeError())
                {
                    if (m_ctx->errorHandler() != nullptr)
                    {
                        ArchiveException ex(
                            static_cast<std::int32_t>(m_controlResponsePoller->relevantId()),
                            m_controlResponsePoller->correlationId(),
                            m_controlResponsePoller->errorMessage(),
                            SOURCEINFO);
                        m_ctx->errorHandler()(ex);
                    }
                    else
                    {
                        throw ArchiveException(
                            static_cast<std::int32_t>(m_controlResponsePoller->relevantId()),
                            m_controlResponsePoller->correlationId(),
                            m_controlResponsePoller->errorMessage(),
                            SOURCEINFO);
                    }
                }
                else if (m_controlResponsePoller->isRecordingSignal())
                {
                    dispatchRecordingSignal();
                }
            }
        }
    }

    /**
     * Poll for RecordingSignals and dispatch them to Context#recordingSignalConsumer.
     *
     * @return the number of RecordingSignals dispatched.
     */
    inline std::int32_t pollForRecordingSignals()
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();

        if (m_controlResponsePoller->poll() != 0 && m_controlResponsePoller->isPollComplete())
        {
            if (m_controlResponsePoller->controlSessionId() == m_controlSessionId)
            {
                if (m_controlResponsePoller->isControlResponse() && m_controlResponsePoller->isCodeError())
                {
                    if (m_ctx->errorHandler() != nullptr)
                    {
                        ArchiveException ex(
                            static_cast<std::int32_t>(m_controlResponsePoller->relevantId()),
                            m_controlResponsePoller->correlationId(),
                            m_controlResponsePoller->errorMessage(),
                            SOURCEINFO);
                        m_ctx->errorHandler()(ex);
                    }
                    else
                    {
                        throw ArchiveException(
                            static_cast<std::int32_t>(m_controlResponsePoller->relevantId()),
                            m_controlResponsePoller->correlationId(),
                            m_controlResponsePoller->errorMessage(),
                            SOURCEINFO);
                    }
                }
                else if (m_controlResponsePoller->isRecordingSignal())
                {
                    dispatchRecordingSignal();
                    return 1;
                }
            }
        }

        return 0;
    }

    /**
     * Add a Publication and set it up to be recorded. If this is not the first,
     * i.e. Publication#isOriginal is true, then an ArchiveException
     * will be thrown and the recording not initiated.
     * <p>
     * This is a sessionId specific recording.
     *
     * @param channel  for the publication.
     * @param streamId for the publication.
     * @tparam IdleStrategy to use for polling operations.
     * @return the Publication ready for use.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::shared_ptr<Publication> addRecordedPublication(const std::string &channel, std::int32_t streamId)
    {
        std::shared_ptr<Publication> publication;
        IdleStrategy idle;

        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

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

    /**
     * Add an ExclusivePublication and set it up to be recorded.
     * <p>
     * This is a sessionId specific recording.
     *
     * @param channel  for the publication.
     * @param streamId for the publication.
     * @tparam IdleStrategy to use for polling operations.
     * @return the ExclusivePublication ready for use.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::shared_ptr<ExclusivePublication> addRecordedExclusivePublication(
        const std::string &channel, std::int32_t streamId)
    {
        std::shared_ptr<ExclusivePublication> publication;
        IdleStrategy idle;

        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

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

    /**
     * Start recording a channel and stream pairing.
     * <p>
     * Channels that include sessionId parameters are considered different than channels without sessionIds. If a
     * publication matches both a sessionId specific channel recording and a non-sessionId specific recording, it will
     * be recorded twice.
     *
     * @param channel        to be recorded.
     * @param streamId       to be recorded.
     * @param sourceLocation of the publication to be recorded.
     * @tparam IdleStrategy  to use for polling operations.
     * @return the subscriptionId, i.e. Subscription#registrationId, of the recording.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t startRecording(
        const std::string &channel, std::int32_t streamId, SourceLocation sourceLocation)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->startRecording<IdleStrategy>(
            channel, streamId, sourceLocation == SourceLocation::LOCAL, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send start recording request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("AeronArchive::startRecording", m_lastCorrelationId);
    }

    /**
     * Start recording a channel and stream pairing.
     * <p>
     * Channels that include sessionId parameters are considered different than channels without sessionIds. If a
     * publication matches both a sessionId specific channel recording and a non-sessionId specific recording, it will
     * be recorded twice.
     *
     * @param channel        to be recorded.
     * @param streamId       to be recorded.
     * @param sourceLocation of the publication to be recorded.
     * @param autoStop       if the recording should be automatically stopped when complete.
     * @tparam IdleStrategy  to use for polling operations.
     * @return the subscriptionId, i.e. Subscription#registrationId, of the recording.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t startRecording(
        const std::string &channel, std::int32_t streamId, SourceLocation sourceLocation, bool autoStop)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->startRecording<IdleStrategy>(
            channel,
            streamId,
            sourceLocation == SourceLocation::LOCAL,
            autoStop,
            m_lastCorrelationId,
            m_controlSessionId))
        {
            throw ArchiveException("failed to send start recording request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("AeronArchive::startRecording", m_lastCorrelationId);
    }

    /**
     * Extend an existing, non-active recording for a channel and stream pairing.
     * <p>
     * The channel must be configured for the initial position from which it will be extended. This can be done
     * with ChannelUriStringBuilder#initialPosition(std::int64_t, std::int32_t, std::int32_t). The details required
     * to initialise can be found by calling #listRecording(std::int64_t, RecordingDescriptorConsumer).
     *
     * @param recordingId    of the existing recording.
     * @param channel        to be recorded.
     * @param streamId       to be recorded.
     * @param sourceLocation of the publication to be recorded.
     * @tparam IdleStrategy  to use for polling operations.
     * @return the subscriptionId, i.e. Subscription#registrationId, of the recording.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t extendRecording(
        std::int64_t recordingId, const std::string &channel, std::int32_t streamId, SourceLocation sourceLocation)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->extendRecording<IdleStrategy>(
            channel,
            streamId,
            sourceLocation == SourceLocation::LOCAL,
            recordingId,
            m_lastCorrelationId,
            m_controlSessionId))
        {
            throw ArchiveException("failed to send extend recording request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("AeronArchive::extendRecording", m_lastCorrelationId);
    }

    /**
     * Extend an existing, non-active recording for a channel and stream pairing.
     * <p>
     * The channel must be configured for the initial position from which it will be extended. This can be done
     * with ChannelUriStringBuilder#initialPosition(std::int64_t, std::int32_t, std::int32_t). The details required
     * to initialise can be found by calling #listRecording(std::int64_t, RecordingDescriptorConsumer).
     *
     * @param recordingId    of the existing recording.
     * @param channel        to be recorded.
     * @param streamId       to be recorded.
     * @param sourceLocation of the publication to be recorded.
     * @param autoStop       if the recording should be automatically stopped when complete.
     * @tparam IdleStrategy  to use for polling operations.
     * @return the subscriptionId, i.e. Subscription#registrationId, of the recording.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t extendRecording(
        std::int64_t recordingId,
        const std::string &channel,
        std::int32_t streamId,
        SourceLocation sourceLocation,
        bool autoStop)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->extendRecording<IdleStrategy>(
            channel,
            streamId,
            sourceLocation == SourceLocation::LOCAL,
            autoStop,
            recordingId,
            m_lastCorrelationId,
            m_controlSessionId))
        {
            throw ArchiveException("failed to send extend recording request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("AeronArchive::extendRecording", m_lastCorrelationId);
    }

    /**
     * Stop recording for a channel and stream pairing.
     * <p>
     * Channels that include sessionId parameters are considered different than channels without sessionIds. Stopping
     * a recording on a channel without a sessionId parameter will not stop the recording of any sessionId specific
     * recordings that use the same channel and streamId.
     *
     * @param channel  to stop recording for.
     * @param streamId to stop recording for.
     * @tparam IdleStrategy to use for polling operations.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void stopRecording(const std::string &channel, std::int32_t streamId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->stopRecording<IdleStrategy>(channel, streamId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send stop recording request", SOURCEINFO);
        }

        pollForResponse<IdleStrategy>("AeronArchive::stopRecording", m_lastCorrelationId);
    }

    /**
     * Try to stop a recording for a channel and stream pairing.
     * <p>
     * Channels that include sessionId parameters are considered different than channels without sessionIds. Stopping
     * a recording on a channel without a sessionId parameter will not stop the recording of any sessionId specific
     * recordings that use the same channel and streamId.
     *
     * @param channel  to stop recording for.
     * @param streamId to stop recording for.
     * @return true if the recording was stopped or false if the subscription is not currently active.
     * @tparam IdleStrategy to use for polling operations.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline bool tryStopRecording(const std::string &channel, std::int32_t streamId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->stopRecording<IdleStrategy>(channel, streamId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send stop recording request", SOURCEINFO);
        }

        return pollForResponseAllowingError<IdleStrategy>("AeronArchive::tryStopRecording", m_lastCorrelationId);
    }

    /**
     * Stop recording a sessionId specific recording that pertains to the given Publication.
     *
     * @param publication to stop recording for.
     * @tparam IdleStrategy to use for polling operations.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void stopRecording(std::shared_ptr<Publication> publication)
    {
        const std::string &recordingChannel = ChannelUri::addSessionId(
            publication->channel(), publication->sessionId());

        stopRecording<IdleStrategy>(recordingChannel, publication->streamId());
    }

    /**
     * Stop recording a sessionId specific recording that pertains to the given ExclusivePublication.
     *
     * @param publication to stop recording for.
     * @tparam IdleStrategy  to use for polling operations.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void stopRecording(std::shared_ptr<ExclusivePublication> publication)
    {
        const std::string &recordingChannel = ChannelUri::addSessionId(
            publication->channel(), publication->sessionId());

        stopRecording<IdleStrategy>(recordingChannel, publication->streamId());
    }

    /**
     * Stop recording for a subscriptionId that has been returned from
     * #startRecording(String, int, SourceLocation) or #extendRecording(long, String, int, SourceLocation).
     *
     * @param subscriptionId is the Subscription#registrationId for the recording in the archive.
     * @tparam IdleStrategy to use for polling operations.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void stopRecording(std::int64_t subscriptionId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->stopRecording<IdleStrategy>(subscriptionId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send stop recording request", SOURCEINFO);
        }

        pollForResponse<IdleStrategy>("AeronArchive::stopRecording", m_lastCorrelationId);
    }

    /**
     * Try stop a recording for a subscriptionId that has been returned from
     * #startRecording(String, int, SourceLocation) or #extendRecording(long, String, int, SourceLocation).
     *
     * @param subscriptionId is the Subscription#registrationId for the recording in the archive.
     * @return true if the recording was stopped or false if the subscription is not currently active.
     * @tparam IdleStrategy to use for polling operations.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline bool tryStopRecording(std::int64_t subscriptionId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->stopRecording<IdleStrategy>(subscriptionId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send stop recording request", SOURCEINFO);
        }

        return pollForResponseAllowingError<IdleStrategy>("AeronArchive::tryStopRecording", m_lastCorrelationId);
    }

    /**
     * Try stop a recording for an existing recording id.
     *
     * @param recordingId of the existing recording.
     * @return true if the recording was stopped or false if the recording is not currently active.
     * @tparam IdleStrategy to use for polling operations.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline bool tryStopRecordingByIdentity(std::int64_t recordingId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->stopRecordingByIdentity<IdleStrategy>(
            recordingId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send stop recording request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("AeronArchive::tryStopRecordingByIdentity", m_lastCorrelationId) != 0;
    }

    /**
     * Start a replay using the ReplayParams to govern its behaviour.
     * <p>
     * The lower 32-bits of the returned value contains the Image#sessionId of the received replay. All
     * 64-bits are required to uniquely identify the replay when calling #stopReplay. The lower 32-bits
     * can be obtained by casting the std::int64_t value to an std::int32_t.
     *
     * @param recordingId    to be replayed.
     * @param replayChannel  to which the replay should be sent.
     * @param replayStreamId to which the replay should be sent.
     * @param replayParams   to control the behaviour of the replay.
     * @tparam IdleStrategy  to use for polling operations.
     * @return the id of the replay session which will be the same as the Image#sessionId of the received
     *         replay for correlation with the matching channel and stream id in the lower 32 bits.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t startReplay(
        std::int64_t recordingId,
        const std::string &replayChannel,
        std::int32_t replayStreamId,
        const ReplayParams &replayParams)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        std::shared_ptr<ChannelUri> replayChannelUri = ChannelUri::parse(replayChannel);

        if (replayChannelUri->hasControlModeResponse())
        {
            return startReplayViaResponseChannel<IdleStrategy>(
                recordingId, replayChannel, replayStreamId, replayParams);
        }

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->replay<IdleStrategy>(
            recordingId, replayChannel, replayStreamId, replayParams, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send replay request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("AeronArchive::startReplay", m_lastCorrelationId);
    }

    /**
     * Start a replay for a length in bytes of a recording from a position. If the position is #NULL_POSITION
     * then the stream will be replayed from the start.
     * <p>
     * The lower 32-bits of the returned value contains the Image#sessionId of the received replay. All
     * 64-bits are required to uniquely identify the replay when calling #stopReplay. The lower 32-bits
     * can be obtained by casting the std::int64_t value to an std::int32_t.
     *
     * @param recordingId    to be replayed.
     * @param position       from which the replay should begin or #NULL_POSITION if from the start.
     * @param length         of the stream to be replayed. Use std::numeric_limits<std::int64_t>::max to follow a live
     *                       recording or #NULL_LENGTH to replay the whole stream of unknown length.
     * @param replayChannel  to which the replay should be sent.
     * @param replayStreamId to which the replay should be sent.
     * @tparam IdleStrategy  to use for polling operations.
     * @return the id of the replay session which will be the same as the Image#sessionId of the received
     *         replay for correlation with the matching channel and stream id in the lower 32 bits.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t startReplay(
        std::int64_t recordingId,
        std::int64_t position,
        std::int64_t length,
        const std::string &replayChannel,
        std::int32_t replayStreamId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->replay<IdleStrategy>(
            recordingId, position, length, replayChannel, replayStreamId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send replay request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("AeronArchive::startReplay", m_lastCorrelationId);
    }

    /**
     * Start a bound replay for a length in bytes of a recording from a position. If the position is #NULL_POSITION
     * then the stream will be replayed from the start. The replay is bounded by the limit counter's position value.
     * <p>
     * The lower 32-bits of the returned value contains the Image#sessionId of the received replay. All
     * 64-bits are required to uniquely identify the replay when calling #stopReplay. The lower 32-bits
     * can be obtained by casting the std::int64_t value to an std::int32_t.
     *
     * @param recordingId    to be replayed.
     * @param position       from which the replay should begin or #NULL_POSITION if from the start.
     * @param length         of the stream to be replayed. Use std::numeric_limits<std::int64_t>::max to follow a live
     *                       recording or #NULL_LENGTH to replay the whole stream of unknown length.
     * @param limitCounterId for the counter which bounds the replay by the position it contains.
     * @param replayChannel  to which the replay should be sent.
     * @param replayStreamId to which the replay should be sent.
     * @tparam IdleStrategy  to use for polling operations.
     * @return the id of the replay session which will be the same as the Image#sessionId of the received
     *         replay for correlation with the matching channel and stream id in the lower 32 bits.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t startBoundedReplay(
        std::int64_t recordingId,
        std::int64_t position,
        std::int64_t length,
        std::int32_t limitCounterId,
        const std::string &replayChannel,
        std::int32_t replayStreamId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->boundedReplay<IdleStrategy>(
            recordingId,
            position,
            length,
            limitCounterId,
            replayChannel,
            replayStreamId,
            m_lastCorrelationId,
            m_controlSessionId))
        {
            throw ArchiveException("failed to send bounded replay request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("AeronArchive::startBoundedReplay", m_lastCorrelationId);
    }

    /**
     * Stop a replay session.
     *
     * @param replaySessionId to stop replay for.
     * @tparam IdleStrategy   to use for polling operations.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void stopReplay(std::int64_t replaySessionId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->stopReplay<IdleStrategy>(replaySessionId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send stop replay request", SOURCEINFO);
        }

        pollForResponse<IdleStrategy>("AeronArchive::stopReplay", m_lastCorrelationId);
    }

    /**
     * Stop all replays matching a recording id. If recording id is #NULL_VALUE then match all replays.
     *
     * @param recordingId   to stop replays for.
     * @tparam IdleStrategy to use for polling operations.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void stopAllReplays(std::int64_t recordingId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->stopAllReplays<IdleStrategy>(recordingId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send stop all replays request", SOURCEINFO);
        }

        pollForResponse<IdleStrategy>("stopAllReplays::stopAllReplays", m_lastCorrelationId);
    }

    /**
     * Start a replay using the ReplayParams to govern its behaviour.
     *
     * @param recordingId    to be replayed.
     * @param replayChannel  to which the replay should be sent.
     * @param replayStreamId to which the replay should be sent.
     * @param replayParams   to govern the behaviour of the replay.
     * @tparam IdleStrategy  to use for polling operations.
     * @return the Subscription for consuming the replay.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::shared_ptr<Subscription> replay(
        std::int64_t recordingId,
        const std::string &replayChannel,
        std::int32_t replayStreamId,
        const ReplayParams &replayParams)
    {
        IdleStrategy idle;
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        std::shared_ptr<ChannelUri> replayChannelUri = ChannelUri::parse(replayChannel);

        if (replayChannelUri->hasControlModeResponse())
        {
            return replayViaResponseChannel<IdleStrategy>(recordingId, replayChannel, replayStreamId, replayParams);
        }

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->replay<IdleStrategy>(
            recordingId, replayChannel, replayStreamId, replayParams, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send replay request", SOURCEINFO);
        }

        auto replaySessionId = static_cast<std::int32_t>(pollForResponse<IdleStrategy>(
            "AeronArchive::replay", m_lastCorrelationId));
        replayChannelUri->put(SESSION_ID_PARAM_NAME, std::to_string(replaySessionId));

        const std::int64_t subscriptionId = m_aeron->addSubscription(replayChannelUri->toString(), replayStreamId);

        std::shared_ptr<Subscription> subscription = m_aeron->findSubscription(subscriptionId);
        while (!subscription)
        {
            idle.idle();
            subscription = m_aeron->findSubscription(subscriptionId);
        }

        return subscription;
    }

    /**
     * Replay a length in bytes of a recording from a position and for convenience create a Subscription
     * to receive the replay. If the position is #NULL_POSITION then the stream will be replayed from the start.
     *
     * @param recordingId    to be replayed.
     * @param position       from which the replay should begin or #NULL_POSITION if from the start.
     * @param length         of the stream to be replayed or std::numeric_limits<std::int64_t>::max to follow a live
                             recording.
     * @param replayChannel  to which the replay should be sent.
     * @param replayStreamId to which the replay should be sent.
     * @tparam IdleStrategy  to use for polling operations.
     * @return the Subscription for consuming the replay.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::shared_ptr<Subscription> replay(
        std::int64_t recordingId,
        std::int64_t position,
        std::int64_t length,
        const std::string &replayChannel,
        std::int32_t replayStreamId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        std::shared_ptr<ChannelUri> replayChannelUri = ChannelUri::parse(replayChannel);
        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->replay<IdleStrategy>(
            recordingId, position, length, replayChannel, replayStreamId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send replay request", SOURCEINFO);
        }

        auto replaySessionId = static_cast<std::int32_t>(pollForResponse<IdleStrategy>(
            "AeronArchive::replay", m_lastCorrelationId));
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

    /**
     * Replay a length in bytes of a recording from a position and for convenience create a Subscription
     * to receive the replay. If the position is #NULL_POSITION then the stream will be replayed from the start.
     *
     * @param recordingId             to be replayed.
     * @param position                from which the replay should begin or #NULL_POSITION if from the start.
     * @param length                  of the stream to be replayed or std::numeric_limits<std::int64_t>::max to follow
     *                                a live recording.
     * @param replayChannel           to which the replay should be sent.
     * @param replayStreamId          to which the replay should be sent.
     * @param availableImageHandler   to be called when the replay image becomes available.
     * @param unavailableImageHandler to be called when the replay image goes unavailable.
     * @tparam IdleStrategy  to use for polling operations.
     * @return the Subscription for consuming the replay.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::shared_ptr<Subscription> replay(
        std::int64_t recordingId,
        std::int64_t position,
        std::int64_t length,
        const std::string &replayChannel,
        std::int32_t replayStreamId,
        const on_available_image_t &availableImageHandler,
        const on_unavailable_image_t &unavailableImageHandler)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        std::shared_ptr<ChannelUri> replayChannelUri = ChannelUri::parse(replayChannel);
        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->replay<IdleStrategy>(
            recordingId, position, length, replayChannel, replayStreamId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send replay request", SOURCEINFO);
        }

        auto replaySessionId = static_cast<std::int32_t>(pollForResponse<IdleStrategy>(
            "AeronArchive::replay", m_lastCorrelationId));
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

    /**
     * List all recording descriptors from a recording id with a limit of record count.
     * <p>
     * If the recording id is greater than the largest known id then nothing is returned.
     *
     * @param fromRecordingId at which to begin the listing.
     * @param recordCount     to limit for each query.
     * @param consumer        to which the descriptors are dispatched.
     * @tparam IdleStrategy  to use for polling operations.
     * @return the number of descriptors found and consumed.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int32_t listRecordings(
        std::int64_t fromRecordingId, std::int32_t recordCount, const recording_descriptor_consumer_t &consumer)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();
        CallbackGuard callbackGuard(m_isInCallback);

        if (!m_archiveProxy->listRecordings<IdleStrategy>(
            fromRecordingId, recordCount, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send list recordings request", SOURCEINFO);
        }

        return pollForDescriptors<IdleStrategy>(
            "AeronArchive::listRecordings", m_lastCorrelationId, recordCount, consumer);
    }

    /**
     * List recording descriptors from a recording id with a limit of record count for a given channelFragment and
     * stream id.
     * <p>
     * If the recording id is greater than the largest known id then nothing is returned.
     *
     * @param fromRecordingId at which to begin the listing.
     * @param recordCount     to limit for each query.
     * @param channelFragment for a contains match on the original channel stored with the archive descriptor.
     * @param streamId        to match.
     * @param consumer        to which the descriptors are dispatched.
     * @tparam IdleStrategy  to use for polling operations.
     * @return the number of descriptors found and consumed.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int32_t listRecordingsForUri(
        std::int64_t fromRecordingId,
        std::int32_t recordCount,
        const std::string &channelFragment,
        std::int32_t streamId,
        const recording_descriptor_consumer_t &consumer)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();
        CallbackGuard callbackGuard(m_isInCallback);

        if (!m_archiveProxy->listRecordingsForUri<IdleStrategy>(
            fromRecordingId, recordCount, channelFragment, streamId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send list recordings request", SOURCEINFO);
        }

        return pollForDescriptors<IdleStrategy>(
            "AeronArchive::listRecordingsForUri", m_lastCorrelationId, recordCount, consumer);
    }

    /**
     * List a recording descriptor for a single recording id.
     * <p>
     * If the recording id is greater than the largest known id then nothing is returned.
     *
     * @param recordingId   at which to begin the listing.
     * @param consumer      to which the descriptors are dispatched.
     * @tparam IdleStrategy to use for polling operations.
     * @return the number of descriptors found and consumed.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int32_t listRecording(std::int64_t recordingId, const recording_descriptor_consumer_t &consumer)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();
        CallbackGuard callbackGuard(m_isInCallback);

        if (!m_archiveProxy->listRecording<IdleStrategy>(recordingId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send list recording request", SOURCEINFO);
        }

        return pollForDescriptors<IdleStrategy>(
            "AeronArchive::listRecording", m_lastCorrelationId, 1, consumer);
    }

    /**
     * Get the start position for a recording.
     *
     * @param recordingId   of the active recording for which the position is required.
     * @tparam IdleStrategy to use for polling operations.
     * @return the start position
     * @see #getRecordingPosition
     * @see #getStopPosition
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t getStartPosition(std::int64_t recordingId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->getStartPosition<IdleStrategy>(recordingId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send get start position request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("AeronArchive::getStartPosition", m_lastCorrelationId);
    }

    /**
     * Get the position recorded for an active recording. If no active recording then return #NULL_POSITION.
     *
     * @param recordingId   of the active recording for which the position is required.
     * @tparam IdleStrategy to use for polling operations.
     * @return the recorded position for the active recording or #NULL_POSITION if recording not active.
     * @see #getStopPosition
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t getRecordingPosition(std::int64_t recordingId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->getRecordingPosition<IdleStrategy>(recordingId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send get recording position request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("AeronArchive::getRecordingPosition", m_lastCorrelationId);
    }

    /**
     * Get the stop position for a recording.
     *
     * @param recordingId   of the active recording for which the position is required.
     * @tparam IdleStrategy to use for polling operations.
     * @return the stop position, or #NULL_POSITION if still active.
     * @see #getRecordingPosition
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t getStopPosition(std::int64_t recordingId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->getStopPosition<IdleStrategy>(recordingId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send get stop position request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("AeronArchive::getStopPosition", m_lastCorrelationId);
    }

    /**
     * Get the stop or active recorded position of a recording.
     *
     * @param recordingId of the recording that the stop of active recording position is being requested for.
     * @tparam IdleStrategy to use for polling operations.
     * @return the recorded length in bytes.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t getMaxRecordedPosition(std::int64_t recordingId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->getMaxRecordedPosition<IdleStrategy>(
            recordingId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send MaxRecordedPosition request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("AeronArchive::getMaxRecordedPosition", m_lastCorrelationId);
    }

    /**
     * Find the last recording that matches the given criteria.
     *
     * @param minRecordingId  to search back to.
     * @param channelFragment for a contains match on the original channel stored with the archive descriptor.
     * @param streamId        of the recording to match.
     * @param sessionId       of the recording to match.
     * @tparam IdleStrategy to use for polling operations.
     * @return the recordingId if found otherwise Aeron#NULL_VALUE if not found.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t findLastMatchingRecording(
        std::int64_t minRecordingId,
        const std::string &channelFragment,
        std::int32_t streamId,
        std::int32_t sessionId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->findLastMatchingRecording<IdleStrategy>(
            minRecordingId, channelFragment, streamId, sessionId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send find last matching recording request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("AeronArchive::findLastMatchingRecording", m_lastCorrelationId);
    }

    /**
     * Truncate a stopped recording to a given position that is less than the stopped position. The provided position
     * must be on a fragment boundary. Truncating a recording to the start position effectively deletes the recording.
     *
     * @param recordingId   of the stopped recording to be truncated.
     * @param position      to which the recording will be truncated.
     * @return the count of segments deleted.
     * @tparam IdleStrategy to use for polling operations.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t truncateRecording(std::int64_t recordingId, std::int64_t position)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->truncateRecording<IdleStrategy>(
            recordingId, position, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send truncate recording request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("AeronArchive::truncateRecording", m_lastCorrelationId);
    }

    /**
     * Purge a stopped recording, i.e. mark recording as 'RecordingState#INVALID' and delete the corresponding segment
     * files. The space in the Catalog will be reclaimed upon compaction.
     *
     * @param recordingId   of the stopped recording to be purged.
     * @tparam IdleStrategy to use for polling operations.
     * @return the count of segments deleted.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int64_t purgeRecording(std::int64_t recordingId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->purgeRecording<IdleStrategy>(recordingId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send purge recording request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("AeronArchive::purgeRecording", m_lastCorrelationId);
    }

    /**
     * List active recording subscriptions in the archive. These are the result of requesting one of
     * #startRecording(String, int, SourceLocation) or a
     * #extendRecording(long, String, int, SourceLocation). The returned subscription id can be used for
     * passing to #stopRecording(std::int64_t).
     *
     * @param pseudoIndex       in the active list at which to begin for paging.
     * @param subscriptionCount to get in a listing.
     * @param channelFragment   to do a contains match on the stripped channel URI. Empty string is match all.
     * @param streamId          to match on the subscription.
     * @param applyStreamId     true if the stream id should be matched.
     * @param consumer          for the matched subscription descriptors.
     * @tparam IdleStrategy to use for polling operations.
     * @return the count of matched subscriptions.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::int32_t listRecordingSubscriptions(
        std::int32_t pseudoIndex,
        std::int32_t subscriptionCount,
        const std::string &channelFragment,
        std::int32_t streamId,
        bool applyStreamId,
        const recording_subscription_descriptor_consumer_t &consumer)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();
        CallbackGuard callbackGuard(m_isInCallback);

        if (!m_archiveProxy->listRecordingSubscriptions<IdleStrategy>(
            pseudoIndex,
            subscriptionCount,
            channelFragment,
            streamId,
            applyStreamId,
            m_lastCorrelationId,
            m_controlSessionId))
        {
            throw ArchiveException("failed to send list recording subscriptions request", SOURCEINFO);
        }

        return pollForSubscriptionDescriptors<IdleStrategy>(
            "AeronArchive::listRecordingSubscriptions", m_lastCorrelationId, subscriptionCount, consumer);
    }

    /**
     * Replicate a recording from a source archive to a destination which can be considered a backup for a primary
     * archive. The source recording will be replayed via the provided replay channel and use the original stream id.
     * If the destination recording id is Aeron#NULL_VALUE then a new destination recording is created,
     * otherwise the provided destination recording id will be extended. The details of the source recording
     * descriptor will be replicated.
     * <p>
     * For a source recording that is still active the replay can merge with the live stream and then follow it
     * directly and no longer require the replay from the source. This would require a multicast live destination.
     * <p>
     * Errors will be reported asynchronously and can be checked for with AeronArchive#pollForErrorResponse()
     * or AeronArchive#checkForErrorResponse(). Follow progress with RecordingSignalAdapter.
     *
     * @param srcRecordingId     recording id which must exist in the source archive.
     * @param dstRecordingId     recording to extend in the destination, otherwise Aeron#NULL_VALUE.
     * @param srcControlStreamId remote control stream id for the source archive to instruct the replay on.
     * @param srcControlChannel  remote control channel for the source archive to instruct the replay on.
     * @param liveDestination    destination for the live stream if merge is required. Empty string for no merge.
     * @tparam IdleStrategy      to use for polling operations.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void replicate(
        std::int64_t srcRecordingId,
        std::int64_t dstRecordingId,
        std::int32_t srcControlStreamId,
        const std::string &srcControlChannel,
        const std::string &liveDestination)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->replicate<IdleStrategy>(
            srcRecordingId,
            dstRecordingId,
            srcControlStreamId,
            srcControlChannel,
            liveDestination,
            m_lastCorrelationId,
            m_controlSessionId))
        {
            throw ArchiveException("failed to send replicate request", SOURCEINFO);
        }

        pollForResponse<IdleStrategy>("AeronArchive::replicate", m_lastCorrelationId);
    }

    /**
     * Replicate a recording from a source archive to a destination which can be considered a backup for a primary
     * archive. The source recording will be replayed via the provided replay channel and use the original stream id.
     * If the destination recording id is Aeron#NULL_VALUE then a new destination recording is created,
     * otherwise the provided destination recording id will be extended. The details of the source recording
     * descriptor will be replicated.
     * <p>
     * For a source recording that is still active the replay can merge with the live stream and then follow it
     * directly and no longer require the replay from the source. This would require a multicast live destination.
     * <p>
     * Errors will be reported asynchronously and can be checked for with AeronArchive#pollForErrorResponse()
     * or AeronArchive#checkForErrorResponse(). Follow progress with RecordingSignalAdapter.
     *
     * @param srcRecordingId     recording id which must exist in the source archive.
     * @param dstRecordingId     recording to extend in the destination, otherwise Aeron#NULL_VALUE.
     * @param stopPosition       position to stop the replication. NULL_POSITION to stop at end of current recording.
     * @param srcControlStreamId remote control stream id for the source archive to instruct the replay on.
     * @param srcControlChannel  remote control channel for the source archive to instruct the replay on.
     * @param liveDestination    destination for the live stream if merge is required. Empty string for no merge.
     * @param replicationChannel channel over which the replication will occur. Empty or null for default channel.
     * @tparam IdleStrategy      to use for polling operations.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void replicate(
        std::int64_t srcRecordingId,
        std::int64_t dstRecordingId,
        std::int64_t stopPosition,
        std::int32_t srcControlStreamId,
        const std::string &srcControlChannel,
        const std::string &liveDestination,
        const std::string &replicationChannel)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->replicate<IdleStrategy>(
            srcRecordingId,
            dstRecordingId,
            stopPosition,
            srcControlStreamId,
            srcControlChannel,
            liveDestination,
            replicationChannel,
            m_lastCorrelationId,
            m_controlSessionId))
        {
            throw ArchiveException("failed to send replicate request", SOURCEINFO);
        }

        pollForResponse<IdleStrategy>("AeronArchive::replicate", m_lastCorrelationId);
    }

    /**
     * Replicate a recording from a source archive to a destination which can be considered a backup for a primary
     * archive. The source recording will be replayed via the provided replay channel and use the original stream id.
     * The behaviour of the replication will be governed by the values specified in the ReplicationParams.
     * <p>
     * For a source recording that is still active the replay can merge with the live stream and then follow it
     * directly and no longer require the replay from the source. This would require a multicast live destination.
     * <p>
     * Errors will be reported asynchronously and can be checked for with AeronArchive#pollForErrorResponse()
     * or AeronArchive#checkForErrorResponse(). Follow progress with RecordingSignalAdapter.
     *
     * @param srcRecordingId     recording id which must exist in the source archive.
     * @param srcControlStreamId remote control stream id for the source archive to instruct the replay on.
     * @param srcControlChannel  remote control channel for the source archive to instruct the replay on.
     * @param replicationParams  optional parameters to configure the behaviour of the replication.
     * @tparam IdleStrategy      to use for polling operations.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void replicate(
        std::int64_t srcRecordingId,
        std::int32_t srcControlStreamId,
        const std::string &srcControlChannel,
        const ReplicationParams &replicationParams)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->replicate<IdleStrategy>(
            srcRecordingId,
            srcControlStreamId,
            srcControlChannel,
            replicationParams,
            m_lastCorrelationId,
            m_controlSessionId))
        {
            throw ArchiveException("failed to send replicate request", SOURCEINFO);
        }

        pollForResponse<IdleStrategy>("AeronArchive::replicate", m_lastCorrelationId);
    }

    /**
     * Replicate a recording from a source archive to a destination which can be considered a backup for a primary
     * archive. The source recording will be replayed via the provided replay channel and use the original stream id.
     * If the destination recording id is Aeron#NULL_VALUE then a new destination recording is created,
     * otherwise the provided destination recording id will be extended. The details of the source recording
     * descriptor will be replicated. The subscription used in the archive will be tagged with the provided tags.
     * <p>
     * For a source recording that is still active the replay can merge with the live stream and then follow it
     * directly and no longer require the replay from the source. This would require a multicast live destination.
     * <p>
     * Errors will be reported asynchronously and can be checked for with AeronArchive#pollForErrorResponse()
     * or AeronArchive#checkForErrorResponse(). Follow progress with RecordingSignalAdapter.
     *
     * @param srcRecordingId     recording id which must exist in the source archive.
     * @param dstRecordingId     recording to extend in the destination, otherwise Aeron#NULL_VALUE.
     * @param channelTagId       used to tag the replication subscription.
     * @param subscriptionTagId  used to tag the replication subscription.
     * @param srcControlStreamId remote control stream id for the source archive to instruct the replay on.
     * @param srcControlChannel  remote control channel for the source archive to instruct the replay on.
     * @param liveDestination    destination for the live stream if merge is required. Empty string for no merge.
     * @tparam IdleStrategy      to use for polling operations.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void taggedReplicate(
        std::int64_t srcRecordingId,
        std::int64_t dstRecordingId,
        std::int64_t channelTagId,
        std::int64_t subscriptionTagId,
        std::int32_t srcControlStreamId,
        const std::string &srcControlChannel,
        const std::string &liveDestination)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->taggedReplicate<IdleStrategy>(
            srcRecordingId,
            dstRecordingId,
            channelTagId,
            subscriptionTagId,
            srcControlStreamId,
            srcControlChannel,
            liveDestination,
            m_lastCorrelationId,
            m_controlSessionId))
        {
            throw ArchiveException("failed to send tagged replicate request", SOURCEINFO);
        }

        pollForResponse<IdleStrategy>("AeronArchive::taggedReplicate", m_lastCorrelationId);
    }

    /**
     * Replicate a recording from a source archive to a destination which can be considered a backup for a primary
     * archive. The source recording will be replayed via the provided replay channel and use the original stream id.
     * If the destination recording id is Aeron#NULL_VALUE then a new destination recording is created,
     * otherwise the provided destination recording id will be extended. The details of the source recording
     * descriptor will be replicated. The subscription used in the archive will be tagged with the provided tags.
     * <p>
     * For a source recording that is still active the replay can merge with the live stream and then follow it
     * directly and no longer require the replay from the source. This would require a multicast live destination.
     * <p>
     * Errors will be reported asynchronously and can be checked for with AeronArchive#pollForErrorResponse()
     * or AeronArchive#checkForErrorResponse(). Follow progress with RecordingSignalAdapter.
     *
     * @param srcRecordingId     recording id which must exist in the source archive.
     * @param dstRecordingId     recording to extend in the destination, otherwise Aeron#NULL_VALUE.
     * @param stopPosition       position to stop the replication. NULL_POSITION to stop at end of current recording.
     * @param channelTagId       used to tag the replication subscription.
     * @param subscriptionTagId  used to tag the replication subscription.
     * @param srcControlStreamId remote control stream id for the source archive to instruct the replay on.
     * @param srcControlChannel  remote control channel for the source archive to instruct the replay on.
     * @param liveDestination    destination for the live stream if merge is required. Empty string for no merge.
     * @param replicationChannel channel over which the replication will occur. Empty or null for default channel.
     * @tparam IdleStrategy      to use for polling operations.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void taggedReplicate(
        std::int64_t srcRecordingId,
        std::int64_t dstRecordingId,
        std::int64_t stopPosition,
        std::int64_t channelTagId,
        std::int64_t subscriptionTagId,
        std::int32_t srcControlStreamId,
        const std::string &srcControlChannel,
        const std::string &liveDestination,
        const std::string &replicationChannel)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->taggedReplicate<IdleStrategy>(
            srcRecordingId,
            dstRecordingId,
            stopPosition,
            channelTagId,
            subscriptionTagId,
            srcControlStreamId,
            srcControlChannel,
            liveDestination,
            replicationChannel,
            m_lastCorrelationId,
            m_controlSessionId))
        {
            throw ArchiveException("failed to send tagged replicate request", SOURCEINFO);
        }

        pollForResponse<IdleStrategy>("AeronArchive::taggedReplicate", m_lastCorrelationId);
    }

    /**
     * Stop a replication session by id.
     *
     * @param replicationId of replication session to be stopped.
     * @tparam IdleStrategy to use for polling operations.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void stopReplication(std::int64_t replicationId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->stopReplication<IdleStrategy>(replicationId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send stop replication request", SOURCEINFO);
        }

        pollForResponse<IdleStrategy>("AeronArchive::stopReplication", m_lastCorrelationId);
    }

    /**
     * Try stopping a replication session by id.
     *
     * @param replicationId of replication session to be stopped.
     * @tparam IdleStrategy to use for polling operations.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline bool tryStopReplication(std::int64_t replicationId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->stopReplication<IdleStrategy>(replicationId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send stop replication request", SOURCEINFO);
        }

        return pollForResponseAllowingError<IdleStrategy>("AeronArchive::tryStopReplication", m_lastCorrelationId);
    }

    /**
     * Detach segments from the beginning of a recording up to the provided new start position.
     * <p>
     * The new start position must be first byte position of a segment after the existing start position.
     * <p>
     * It is not possible to detach segments which are active for recording or being replayed.
     *
     * @param recordingId      of the recording to detach segments from.
     * @param newStartPosition for the recording after segments are detached.
     * @tparam IdleStrategy    to use for polling operations.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void detachSegments(std::int64_t recordingId, std::int64_t newStartPosition)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->detachSegments<IdleStrategy>(
            recordingId, newStartPosition, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send detach segments request", SOURCEINFO);
        }

        pollForResponse<IdleStrategy>("AeronArchive::detachSegments", m_lastCorrelationId);
    }

    /**
     * Delete detached segments which have been previously detached from a recording.
     *
     * @param recordingId   of the recording to delete previously detached segments from.
     * @tparam IdleStrategy to use for polling operations.
     * @return the count of segments deleted.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::uint64_t deleteDetachedSegments(std::int64_t recordingId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->deleteDetachedSegments<IdleStrategy>(recordingId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send delete segments request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("AeronArchive::deleteDetachedSegments", m_lastCorrelationId);
    }

    /**
     * Purge (detach and delete) segments from the beginning of a recording up to the provided new start position.
     * <p>
     * The new start position must be first byte position of a segment after the existing start position.
     * <p>
     * It is not possible to detach segments which are active for recording or being replayed.
     *
     * @param recordingId      of the recording to purge segments from.
     * @param newStartPosition for the recording after segments are purged.
     * @tparam IdleStrategy    to use for polling operations.
     * @return the count of segments purged.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::uint64_t purgeSegments(std::int64_t recordingId, std::int64_t newStartPosition)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->purgeSegments<IdleStrategy>(
            recordingId, newStartPosition, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send purge segments request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("AeronArchive::purgeSegments", m_lastCorrelationId);
    }

    /**
     * Attach segments to the beginning of a recording to restore history that was previously detached.
     * <p>
     * Segment files must match the existing recording and join exactly to the start position of the recording
     * they are being attached to.
     *
     * @param recordingId   of the recording to attach segments to.
     * @tparam IdleStrategy to use for polling operations.
     * @return the count of segments attached.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::uint64_t attachSegments(std::int64_t recordingId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->attachSegments<IdleStrategy>(recordingId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send attach segments request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("AeronArchive::attachSegments", m_lastCorrelationId);
    }

    /**
     * Migrate segments from a source recording and attach them to the beginning of a destination recording.
     * <p>
     * The source recording must match the destination recording for segment length, term length, mtu length,
     * stream id, plus the stop position and term id of the source must join with the start position of the destination
     * and be on a segment boundary.
     * <p>
     * The source recording will be effectively truncated back to its start position after the migration.
     *
     * @param srcRecordingId source recording from which the segments will be migrated.
     * @param dstRecordingId destination recording to which the segments will be attached.
     * @tparam IdleStrategy  to use for polling operations.
     * @return the count of segments purged.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline std::uint64_t migrateSegments(std::int64_t srcRecordingId, std::int64_t dstRecordingId)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);
        ensureOpen();
        ensureNotReentrant();

        m_lastCorrelationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->migrateSegments<IdleStrategy>(
            srcRecordingId, dstRecordingId, m_lastCorrelationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send migrate segments request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("migrateSegments::migrateSegments", m_lastCorrelationId);
    }

    /**
     * Return the static version string for the binary library.
     *
     * @return static version and build string
     */
    static std::string version();

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
    const std::int64_t m_archiveId;
    std::int64_t m_lastCorrelationId = NULL_VALUE;
    const long long m_messageTimeoutNs;
    bool m_isClosed = false;
    bool m_isInCallback = false;

    inline void ensureOpen() const
    {
        if (m_isClosed)
        {
            throw ArchiveException("client is closed", SOURCEINFO);
        }
    }

    inline void ensureNotReentrant() const
    {
        if (m_isInCallback)
        {
            throw ReentrantException("client cannot be invoked within callback", SOURCEINFO);
        }
    }

    inline void checkDeadline(
        long long deadlineNs,
        const char *operationName,
        const char *errorMessage,
        std::int64_t correlationId)
    {
        if ((deadlineNs - m_nanoClock()) < 0)
        {
            std::string msg(operationName);

            msg += " ";
            msg += errorMessage;
            msg += " - correlationId=";
            msg += std::to_string(correlationId);

            throw TimeoutException(msg, SOURCEINFO);
        }
    }

    inline void invokeAeronClient()
    {
        if (m_aeron->usesAgentInvoker())
        {
            m_aeron->conductorAgentInvoker().invoke();
        }

        m_ctx->delegatingInvoker()();
    }

    inline void dispatchRecordingSignal() const
    {
        m_ctx->recordingSignalConsumer()(
            m_controlResponsePoller->controlSessionId(),
            m_controlResponsePoller->recordingId(),
            m_controlResponsePoller->subscriptionId(),
            m_controlResponsePoller->position(),
            m_controlResponsePoller->recordingSignalCode());
    }

    template<typename IdleStrategy>
    inline void pollNextResponse(
        const char *operationName, std::int64_t correlationId, long long deadlineNs, ControlResponsePoller &poller)
    {
        IdleStrategy idle;

        while (true)
        {
            const int fragments = poller.poll();

            if (poller.isPollComplete())
            {
                if (m_controlResponsePoller->isRecordingSignal() &&
                    m_controlResponsePoller->controlSessionId() == m_controlSessionId)
                {
                    dispatchRecordingSignal();
                    continue;
                }

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

            checkDeadline(deadlineNs, operationName, "awaiting response", correlationId);
            idle.idle();
            invokeAeronClient();
        }
    }

    template<typename IdleStrategy>
    inline std::int64_t pollForResponse(const char *operationName, std::int64_t correlationId)
    {
        const long long deadlineNs = m_nanoClock() + m_messageTimeoutNs;

        while (true)
        {
            pollNextResponse<IdleStrategy>(operationName, correlationId, deadlineNs, *m_controlResponsePoller);

            if (m_controlResponsePoller->controlSessionId() != controlSessionId())
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
                        m_controlResponsePoller->correlationId(),
                        "response for correlationId=" + std::to_string(correlationId) +
                        ", error: " + m_controlResponsePoller->errorMessage(),
                        SOURCEINFO);
                }
                else if (m_ctx->errorHandler() != nullptr)
                {
                    ArchiveException ex(
                        static_cast<std::int32_t>(m_controlResponsePoller->relevantId()),
                        m_controlResponsePoller->correlationId(),
                        "response for correlationId=" + std::to_string(correlationId) +
                        ", error: " + m_controlResponsePoller->errorMessage(),
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
    inline bool pollForResponseAllowingError(const char *operationName, std::int64_t correlationId)
    {
        const long long deadlineNs = m_nanoClock() + m_messageTimeoutNs;

        while (true)
        {
            pollNextResponse<IdleStrategy>(operationName, correlationId, deadlineNs, *m_controlResponsePoller);

            if (m_controlResponsePoller->controlSessionId() != controlSessionId())
            {
                invokeAeronClient();
                continue;
            }

            if (m_controlResponsePoller->isCodeError())
            {
                if (m_controlResponsePoller->correlationId() == correlationId)
                {
                    if (m_controlResponsePoller->relevantId() == ARCHIVE_ERROR_CODE_UNKNOWN_SUBSCRIPTION)
                    {
                        return false;
                    }

                    throw ArchiveException(
                        static_cast<std::int32_t>(m_controlResponsePoller->relevantId()),
                        m_controlResponsePoller->correlationId(),
                        "response for correlationId=" + std::to_string(correlationId) +
                        ", error: " + m_controlResponsePoller->errorMessage(),
                        SOURCEINFO);
                }
                else if (m_ctx->errorHandler() != nullptr)
                {
                    ArchiveException ex(
                        static_cast<std::int32_t>(m_controlResponsePoller->relevantId()),
                        m_controlResponsePoller->correlationId(),
                        "response for correlationId=" + std::to_string(correlationId) +
                        ", error: " + m_controlResponsePoller->errorMessage(),
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

                return true;
            }
        }
    }

    template<typename IdleStrategy>
    std::int32_t pollForDescriptors(
        const char *operationName,
        std::int64_t correlationId,
        std::int32_t recordCount,
        const recording_descriptor_consumer_t &consumer)
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
                throw ArchiveException("response channel from archive is not connected", SOURCEINFO);
            }

            checkDeadline(deadlineNs, operationName, "awaiting recording descriptors", correlationId);
            idle.idle();
        }
    }

    template<typename IdleStrategy>
    std::int32_t pollForSubscriptionDescriptors(
        const char *operationName,
        std::int64_t correlationId,
        std::int32_t subscriptionCount,
        const recording_subscription_descriptor_consumer_t &consumer)
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
                throw ArchiveException("response channel from archive is not connected", SOURCEINFO);
            }

            checkDeadline(deadlineNs, operationName, "awaiting subscription descriptors", correlationId);
            idle.idle();
        }
    }

    static void checkAndSetupResponseChannel(AeronArchive::Context_t &ctx, std::int64_t subscriptionId);

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    std::shared_ptr<Subscription> replayViaResponseChannel(
        std::int64_t recordingId,
        const std::string &replayChannel,
        std::int32_t replayStreamId,
        const ReplayParams &replayParams)
    {
        m_lastCorrelationId = m_aeron->nextCorrelationId();
        ReplayParams responseChannelReplayParams{replayParams};
        IdleStrategy idle;

        if (!m_archiveProxy->requestReplayToken(m_lastCorrelationId, m_controlSessionId, recordingId))
        {
            throw ArchiveException("failed to send replay token request", SOURCEINFO);
        }

        const std::int64_t replayToken = pollForResponse<IdleStrategy>(
            "AeronArchive::replayToken", m_lastCorrelationId);

        responseChannelReplayParams.replayToken(replayToken);
        int64_t subscriptionId = m_aeron->addSubscription(replayChannel, replayStreamId);
        std::int64_t deadlineNs = m_nanoClock() + m_ctx->messageTimeoutNs();

        std::shared_ptr<Subscription> replaySubscription = m_aeron->findSubscription(subscriptionId);
        while (nullptr == replaySubscription)
        {
            replaySubscription = m_aeron->findSubscription(subscriptionId);
            checkDeadline<IdleStrategy>(deadlineNs, "timed out waiting for subscription from driver");
        }

        const std::shared_ptr<ChannelUri> controlChannelUri = ChannelUri::parse(m_ctx->controlRequestChannel());
        controlChannelUri->remove(TERM_OFFSET_PARAM_NAME);
        controlChannelUri->remove(TERM_ID_PARAM_NAME);
        controlChannelUri->remove(INITIAL_TERM_ID_PARAM_NAME);
        controlChannelUri->put(RESPONSE_CORRELATION_ID_PARAM_NAME, std::to_string(subscriptionId));
        controlChannelUri->put(TERM_LENGTH_PARAM_NAME, "64k");
        controlChannelUri->put(SPIES_SIMULATE_CONNECTION_PARAM_NAME, "false");

        int64_t publicationId = m_aeron->addExclusivePublication(
            controlChannelUri->toString(), m_ctx->controlRequestStreamId());

        std::shared_ptr<ExclusivePublication> publication = m_aeron->findExclusivePublication(publicationId);
        while (!publication)
        {
            publication = m_aeron->findExclusivePublication(publicationId);
            checkDeadline<IdleStrategy>(deadlineNs, "timed out waiting for publication from driver");
        }

        ArchiveProxy archiveProxy{publication};

        while (!publication->isConnected())
        {
            checkDeadline<IdleStrategy>(deadlineNs, "timed out waiting to establish replay connection");
        }

        while (0 == publication->publicationLimit())
        {
            checkDeadline<IdleStrategy>(
                deadlineNs, "timed out waiting for replay connection to have available publication limit");
        }

        m_lastCorrelationId = m_aeron->nextCorrelationId();
        if (!archiveProxy.replay(
            recordingId,
            replayChannel,
            replayStreamId,
            responseChannelReplayParams,
            m_lastCorrelationId,
            m_controlSessionId))
        {
            throw ArchiveException("failed to send replay request", SOURCEINFO);
        }

        pollForResponse<IdleStrategy>("AeronArchiveProxy::replay", m_lastCorrelationId);

        while (!replaySubscription->isConnected())
        {
            checkDeadline<IdleStrategy>(deadlineNs, "timed out wait for replay subscription to connect");
        }

        return replaySubscription;
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    std::int64_t startReplayViaResponseChannel(
        std::int64_t recordingId,
        const std::string &replayChannel,
        std::int32_t replayStreamId,
        const ReplayParams &replayParams)
    {
        IdleStrategy idle;

        if (NULL_VALUE == replayParams.subscriptionRegistrationId())
        {
            throw ArchiveException(
                "when using startReplay with a response channel, ReplayParams::subscriptionRegistrationId must be set",
                SOURCEINFO);
        }

        m_lastCorrelationId = m_aeron->nextCorrelationId();
        if (!m_archiveProxy->requestReplayToken(m_lastCorrelationId, m_controlSessionId, recordingId))
        {
            throw ArchiveException("failed to send replay token request", SOURCEINFO);
        }

        ReplayParams responseChannelReplayParams{replayParams};

        const std::int64_t replayToken = pollForResponse<IdleStrategy>(
            "AeronArchive::replayToken", m_lastCorrelationId);

        responseChannelReplayParams.replayToken(replayToken);
        std::int64_t deadlineNs = m_nanoClock() + m_ctx->messageTimeoutNs();

        const std::shared_ptr<ChannelUri> controlChannelUri = ChannelUri::parse(m_ctx->controlRequestChannel());
        controlChannelUri->remove(TERM_OFFSET_PARAM_NAME);
        controlChannelUri->remove(TERM_ID_PARAM_NAME);
        controlChannelUri->remove(INITIAL_TERM_ID_PARAM_NAME);
        controlChannelUri->put(
            RESPONSE_CORRELATION_ID_PARAM_NAME, std::to_string(replayParams.subscriptionRegistrationId()));
        controlChannelUri->put(TERM_LENGTH_PARAM_NAME, "64k");
        controlChannelUri->put(SPIES_SIMULATE_CONNECTION_PARAM_NAME, "false");

        int64_t publicationId = m_aeron->addExclusivePublication(
            controlChannelUri->toString(), m_ctx->controlRequestStreamId());

        std::shared_ptr<ExclusivePublication> publication = m_aeron->findExclusivePublication(publicationId);
        while (!publication)
        {
            publication = m_aeron->findExclusivePublication(publicationId);
            checkDeadline<IdleStrategy>(deadlineNs, "timed out waiting for publication from driver");
        }

        ArchiveProxy archiveProxy{publication};

        const int pubLmtCounterId = m_aeron->countersReader().findByTypeIdAndRegistrationId(
            AeronCounters::DRIVER_PUBLISHER_LIMIT_TYPE_ID, publicationId);

        while (!publication->isConnected())
        {
            checkDeadline<IdleStrategy>(deadlineNs, "timed out waiting for replay publication to connect");
        }

        while (0 == m_aeron->countersReader().getCounterValue(pubLmtCounterId))
        {
            checkDeadline<IdleStrategy>(deadlineNs, "timed out waiting for replay publication to have available limit");
        }

        m_lastCorrelationId = m_aeron->nextCorrelationId();
        if (!archiveProxy.replay(
            recordingId,
            replayChannel,
            replayStreamId,
            responseChannelReplayParams,
            m_lastCorrelationId,
            m_controlSessionId))
        {
            throw ArchiveException("failed to send replay request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>("AeronArchiveProxy::replay", m_lastCorrelationId);
    }

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void checkDeadline(const std::int64_t deadlineNs, const std::string message)
    {
        IdleStrategy idle;

        if (deadlineNs <= m_nanoClock())
        {
            throw ArchiveException(message, SOURCEINFO);
        }

        idle.idle();
    }
};

}}}

#endif //AERON_ARCHIVE_AERON_ARCHIVE_H
