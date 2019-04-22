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
#ifndef AERON_ARCHIVE_AERON_ARCHIVE_H
#define AERON_ARCHIVE_AERON_ARCHIVE_H

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

namespace aeron { namespace archive { namespace client {

/**
 * Client for interacting with a local or remote Aeron Archive that records and replays message streams.
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
        std::int64_t controlSessionId);
    ~AeronArchive();

    /// Location of the source with respect to the archive.
    enum SourceLocation: int
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
            Context_t& context, std::shared_ptr<Aeron> aeron, std::int64_t subscriptionId, std::int64_t publicationId);

        /**
         * Poll for a complete connection.
         *
         * @return a new AeronArchive if successfully connected otherwise null.
         */
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
        std::int64_t m_correlationId = aeron::NULL_VALUE;
        std::uint8_t m_step = 0;
    };

    /**
     * Begin an attempt at creating a connection which can be completed by calling AsyncConnect#poll.
     *
     * @param ctx for the archive connection.
     * @return the AsyncConnect that can be polled for completion.
     */
    static std::shared_ptr<AsyncConnect> asyncConnect(Context_t& ctx);

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
    inline Context_t& context()
    {
        return *m_ctx;
    }

    /**
     * The control session id allocated for this connection to the archive.
     *
     * @return control session id allocated for this connection to the archive.
     */
    inline std::int64_t controlSessionId()
    {
        return m_controlSessionId;
    }

    /**
     * The ArchiveProxy for send asynchronous messages to the connected archive.
     *
     * @return the ArchiveProxy for send asynchronous messages to the connected archive.
     */
    inline ArchiveProxy& archiveProxy()
    {
        return *m_archiveProxy;
    }

    /**
     * Get the ControlResponsePoller for polling additional events on the control channel.
     *
     * @return the ControlResponsePoller for polling additional events on the control channel.
     */
    inline ControlResponsePoller& controlResponsePoller()
    {
        return *m_controlResponsePoller;
    }

    /**
     * Get the RecordingDescriptorPoller for polling recording descriptors on the control channel.
     *
     * @return the RecordingDescriptorPoller for polling recording descriptors on the control channel.
     */
    inline RecordingDescriptorPoller& recordingDescriptorPoller()
    {
        return *m_recordingDescriptorPoller;
    }

    /**
     * The RecordingSubscriptionDescriptorPoller for polling subscription descriptors on the control channel.
     *
     * @return the RecordingSubscriptionDescriptorPoller for polling subscription descriptors on the control
     * channel.
     */
    inline RecordingSubscriptionDescriptorPoller& recordingSubscriptionDescriptorPoller()
    {
        return *m_recordingSubscriptionDescriptorPoller;
    }

    /**
     * Poll the response stream once for an error. If another message is present then it will be skipped over
     * so only call when not expecting another response.
     *
     * @return the error String otherwise an empty string is returned if no error is found.
     */
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

    /**
     * Check if an error has been returned for the control session and throw a ArchiveException if
     * Context#errorHandler is not set.
     * <p>
     * To check for an error response without raising an exception then try #pollForErrorResponse.
     *
     * @see #pollForErrorResponse
     */
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
        const std::string& channel,
        std::int32_t streamId,
        SourceLocation sourceLocation)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        const std::int64_t correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->startRecording<IdleStrategy>(
            channel, streamId, sourceLocation == SourceLocation::LOCAL, correlationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send start recording request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>(correlationId);
    }

    /**
     * Extend an existing, non-active recording of a channel and stream pairing.
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
        std::int64_t recordingId,
        const std::string& channel,
        std::int32_t streamId,
        SourceLocation sourceLocation)
    {
        std::lock_guard<std::recursive_mutex> lock(m_lock);

        ensureOpen();

        const std::int64_t correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->extendRecording<IdleStrategy>(
            channel, streamId, sourceLocation == SourceLocation::LOCAL, recordingId, correlationId, m_controlSessionId))
        {
            throw ArchiveException("failed to send extend recording request", SOURCEINFO);
        }

        return pollForResponse<IdleStrategy>(correlationId);
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
     * @tparam IdleStrategy  to use for polling operations.
     */
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

    /**
     * Stop recording a sessionId specific recording that pertains to the given Publication.
     *
     * @param publication to stop recording for.
     * @tparam IdleStrategy  to use for polling operations.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    inline void stopRecording(std::shared_ptr<Publication> publication)
    {
        const std::string& recordingChannel = ChannelUri::addSessionId(
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
        const std::string& recordingChannel = ChannelUri::addSessionId(
            publication->channel(), publication->sessionId());

        stopRecording<IdleStrategy>(recordingChannel, publication->streamId());
    }

    /**
     * Stop recording for a subscriptionId that has been returned from
     * #startRecording(String, int, SourceLocation) or
     * #extendRecording(long, String, int, SourceLocation).
     *
     * @param subscriptionId is the Subscription#registrationId for the recording in the archive.
     * @tparam IdleStrategy  to use for polling operations.
     */
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
     * replay for correlation with the matching channel and stream id in the lower 32 bits.
     */
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

    /**
     * Stop a replay session.
     *
     * @param replaySessionId to stop replay for.
     * @tparam IdleStrategy  to use for polling operations.
     */
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

    /**
     * List a recording descriptor for a single recording id.
     * <p>
     * If the recording id is greater than the largest known id then nothing is returned.
     *
     * @param recordingId at which to begin the listing.
     * @param consumer    to which the descriptors are dispatched.
     * @tparam IdleStrategy to use for polling operations.
     * @return the number of descriptors found and consumed.
     */
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

    /**
     * Get the position recorded for an active recording. If no active recording then return #NULL_POSITION.
     *
     * @param recordingId of the active recording for which the position is required.
     * @tparam IdleStrategy to use for polling operations.
     * @return the recorded position for the active recording or #NULL_POSITION if recording not active.
     * @see #getStopPosition
     */
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

    /**
     * Get the stop position for a recording.
     *
     * @param recordingId of the active recording for which the position is required.
     * @tparam IdleStrategy to use for polling operations.
     * @return the stop position, or #NULL_POSITION if still active.
     * @see #getRecordingPosition
     */
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

    /**
     * Truncate a stopped recording to a given position that is less than the stopped position. The provided position
     * must be on a fragment boundary. Truncating a recording to the start position effectively deletes the recording.
     *
     * @param recordingId of the stopped recording to be truncated.
     * @param position    to which the recording will be truncated.
     * @tparam IdleStrategy to use for polling operations.
     */
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
                        "response for correlationId=" + std::to_string(correlationId) +
                        ", error: " + m_controlResponsePoller->errorMessage(),
                        SOURCEINFO);
                }
                else if (m_ctx->errorHandler() != nullptr)
                {
                    ArchiveException ex(
                        static_cast<std::int32_t>(m_controlResponsePoller->relevantId()),
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
#endif //AERON_ARCHIVE_AERON_ARCHIVE_H
