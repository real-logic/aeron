/*
 * Copyright 2014-2025 Real Logic Limited.
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

#ifndef AERON_ARCHIVE_ARCHIVE_PROXY_H
#define AERON_ARCHIVE_ARCHIVE_PROXY_H

#include <array>
#include <utility>

#include "Aeron.h"
#include "concurrent/BackOffIdleStrategy.h"
#include "ArchiveException.h"

namespace aeron { namespace archive { namespace client
{

/// Length of buffer to use in proxy to hold messages for construction.
constexpr const std::size_t PROXY_REQUEST_BUFFER_LENGTH = 8 * 1024;

/**
 * Contains the optional parameters that can be passed to a Replication Request. Controls the behaviour of the
 * replication including tagging, stop position, extending destination recordings, live merging, and setting the
 * maximum length of the file I/O operations.
 */
class ReplicationParams
{
public:
    ReplicationParams() :
        m_stopPosition(NULL_POSITION),
        m_dstRecordingId(NULL_VALUE),
        m_liveDestination(),
        m_replicationChannel(),
        m_channelTagId(NULL_VALUE),
        m_subscriptionTagId(NULL_VALUE),
        m_fileIoMaxLength(NULL_VALUE),
        m_replicationSessionId(NULL_VALUE)
    {
    }

    /**
     * The stop position for this replication request.
     * @return stop position
     */
    std::int64_t stopPosition() const
    {
        return m_stopPosition;
    }

    /**
     * Set the stop position for replication, default is aeron::NULL_POSITION, which will continuously replicate.
     *
     * @param stopPosition position to stop the replication at.
     * @return this for a fluent API
     */
    ReplicationParams& stopPosition(std::int64_t stopPosition)
    {
        m_stopPosition = stopPosition;
        return *this;
    }

    /**
     * Destination recording id to extend.
     *
     * @return destination recording id.
     */
    int64_t dstRecordingId() const
    {
        return m_dstRecordingId;
    }

    /**
     * The recording in the local archive to extend. Default is aeron::NULL_VALUE which will trigger the creation
     * of a new recording in the destination archive.
     *
     * @param dstRecordingId destination recording to extend.
     * @return this for a fluent API.
     */
    ReplicationParams &dstRecordingId(int64_t dstRecordingId)
    {
        m_dstRecordingId = dstRecordingId;
        return *this;
    }

    /**
     * Gets the destination for the live stream merge.
     *
     * @return destination for live stream merge.
     */
    const std::string &liveDestination() const
    {
        return m_liveDestination;
    }

    /**
     * Destination for the live stream if merge is required. Default is empty string for no merge.
     *
     * @param liveChannel for the live stream merge
     * @return this for a fluent API.
     */
    ReplicationParams &liveDestination(const std::string &liveDestination)
    {
        m_liveDestination = liveDestination;
        return *this;
    }

    /**
     * Channel to use for replicating the recording, empty string will mean that the default channel is used.
     * @return channel to replicate the recording.
     */
    const std::string &replicationChannel() const
    {
        return m_replicationChannel;
    }

    /**
     * Channel use to replicate the recording. Default is empty string which will use the context's default replication
     * channel
     *
     * @param replicationChannel to use for replicating the recording.
     * @return this for a fluent API.
     */
    ReplicationParams &replicationChannel(const std::string &replicationChannel)
    {
        m_replicationChannel = replicationChannel;
        return *this;
    }

    /**
     * Gets channel tag id for the archive subscription.
     *
     * @return channel tag id.
     */
    int64_t channelTagId() const
    {
        return m_channelTagId;
    }

    /**
     * The channel used by the archive's subscription for replication will have the supplied channel tag applied to it.
     * The default value for channelTagId is aeron::NULL_VALUE
     *
     * @param channelTagId tag to apply to the archive's subscription.
     * @return this for a fluent API
     */
    ReplicationParams &channelTagId(int64_t channelTagId)
    {
        m_channelTagId = channelTagId;
        return *this;
    }

    /**
     * Gets subscription tag id for the archive subscription.
     *
     *  @return subscription tag id.
     */
    int64_t subscriptionTagId() const
    {
        return m_subscriptionTagId;
    }

    /**
     * The channel used by the archive's subscription for replication will have the supplied subscription tag applied to
     * it. The default value for subscriptionTagId is aeron::NULL_VALUE
     *
     * @param subscriptionTagId tag to apply to the archive's subscription.
     * @return this for a fluent API
     */
    ReplicationParams &subscriptionTagId(int64_t subscriptionTagId)
    {
        m_subscriptionTagId = subscriptionTagId;
        return *this;
    }

    /**
     * Gets the maximum length for file IO operations in the replay. Defaults to {@link Aeron#NULL_VALUE} if not
     * set, which will trigger the use of the Archive.Context default.
     *
     * @return maximum length of a file I/O operation.
     */
    int32_t fileIoMaxLength() const
    {
        return m_fileIoMaxLength;
    }

    /**
     * The maximum size of a file operation when reading from the archive to execute the replication. Will use the value
     * defined in the context otherwise. This can be used reduce the size of file IO operations to lower the
     * priority of some replays. Setting it to a value larger than the context value will have no affect.
     *
     * @param fileIoMaxLength maximum length of a file I/O operation.
     * @return this for a fluent API
     */
    ReplicationParams &fileIoMaxLength(int32_t fileIoMaxLength)
    {
        m_fileIoMaxLength = fileIoMaxLength;
        return *this;
    }

    /**
     * Sets the session-id to be used for the replicated file instead of the session id from the source archive. This
     * is useful in cases where we are replicating the same recording in multiple stages.
     *
     * @param replicationSessionId the session-id to be set for the received recording.
     * @return this for fluent API
     */
    ReplicationParams &replicationSessionId(std::int32_t replicationSessionId)
    {
        m_replicationSessionId = replicationSessionId;
        return *this;
    }

    /**
     * The session-id to be used for the replicated recording.
     *
     * @return session-id to be useful for the replicated recording.
     */
    std::int32_t replicationSessionId() const
    {
        return m_replicationSessionId;
    }

    /**
     * Sets the encoded credentials that will be passed to the source archive for authentication. Currently only simple
     * authentication (i.e. not challenge/response) is supported for replication.
     *
     * @param encodedCredentials credentials to be passed to the source archive.
     * @return this for a fluent API.
     */
    ReplicationParams &encodedCredentials(std::pair<const char *, std::uint32_t> encodedCredentials)
    {
        m_encodedCredentials = encodedCredentials;
        return *this;
    }

    /**
     * Gets the encoded credentials that will be used to authenticate against the source archive.
     *
     * @return encoded credentials used for authentication.
     */
    std::pair<const char *, std::uint32_t> encodedCredentials() const
    {
        return m_encodedCredentials;
    }

private:
    std::int64_t m_stopPosition;
    std::int64_t m_dstRecordingId;
    std::string m_liveDestination;
    std::string m_replicationChannel;
    std::int64_t m_channelTagId;
    std::int64_t m_subscriptionTagId;
    std::int32_t m_fileIoMaxLength;
    std::int32_t m_replicationSessionId;
    std::pair<const char *, std::uint32_t> m_encodedCredentials;
};

/**
 * Fluent API for setting optional replay parameters. Allows the user to configure starting position,
 * replay length, bounding counter (for a bounded replay) and the max length for file I/O operations.
 */
class ReplayParams
{
public:
    ReplayParams() :
        m_boundingLimitCounterId(NULL_VALUE),
        m_fileIoMaxLength(NULL_VALUE),
        m_position(NULL_POSITION),
        m_length(NULL_LENGTH),
        m_replayToken(NULL_VALUE),
        m_subscriptionRegistrationId(NULL_VALUE)
    {
    }

    /**
     * Gets the counterId specified for the bounding the replay. Returns aeron::NULL_VALUE if unspecified.
     *
     * @return the counter id to bound the replay.
     */
    std::int32_t boundingLimitCounterId() const
    {
        return m_boundingLimitCounterId;
    }

    /**
     * Sets the counter id to be used for bounding the replay. Setting this value will trigger the sending of a
     * bounded replay request instead of a normal replay. Default is aeron::NULL_VALUE, which will mean that a
     * bound will not be applied.
     *
     * @param boundingLimitCounterId counter to use to bound the replay
     * @return this for a fluent API
     */
    ReplayParams &boundingLimitCounterId(std::int32_t mBoundingLimitCounterId)
    {
        m_boundingLimitCounterId = mBoundingLimitCounterId;
        return *this;
    }

    /**
     * Gets the maximum length for file IO operations in the replay. Defaults to aeron::NULL_VALUE if not
     * set, which will trigger the use of the Archive.Context default.
     *
     * @return maximum file length for IO operations during replay.
     */
    std::int32_t fileIoMaxLength() const
    {
        return m_fileIoMaxLength;
    }

    /**
     * The maximum size of a file operation when reading from the archive to execute the replay. Will use the value
     * defined in the context otherwise. This can be used reduce the size of file IO operations to lower the
     * priority of some replays. Setting it to a value larger than the context value will have no affect.
     *
     * @param fileIoMaxLength maximum length of a replay file operation
     * @return this for a fluent API
     */
    ReplayParams &fileIoMaxLength(std::int32_t mFileIoMaxLength)
    {
        m_fileIoMaxLength = mFileIoMaxLength;
        return *this;
    }

    /**
     * Position to start the replay at.
     *
     * @return position for the start of the replay.
     */
    std::int64_t position() const
    {
        return m_position;
    }

    /**
     * Set the position to start the replay. If set to aeron::NULL_POSITION (which is the default) then
     * the stream will be replayed from the start.
     *
     * @param position to start the replay from.
     * @return this for a fluent API.
     */
    ReplayParams &position(std::int64_t mPosition)
    {
        m_position = mPosition;
        return *this;
    }

    /**
     * Length of the recording to replay.
     *
     * @return length of the recording to replay.
     */
    std::int64_t length() const
    {
        return m_length;
    }

    /**
     * The length of the recorded stream to replay. If set to aeron::NULL_POSITION (the default) will
     * replay a whole stream of unknown length. If set to INT64_MAX it will follow a live recording.
     *
     * @param length of the recording to be replayed.
     * @return this for a fluent API.
     */
    ReplayParams &length(std::int64_t mLength)
    {
        m_length = mLength;
        return *this;
    }

    /**
     * Determines if the parameter setup has requested a bounded replay.
     *
     * @return true if the replay should be bounded, false otherwise.
     */
    bool isBounded() const
    {
        return NULL_VALUE != boundingLimitCounterId();
    }

    /**
     * Set a token used for replays when the initiating image is not the one used to create the archive
     * connection/session.
     *
     * @param replayToken token to identify the replay
     * @return this for a fluent API.
     */
    ReplayParams &replayToken(std::int64_t replayToken)
    {
        m_replayToken = replayToken;
        return *this;
    }

    /**
     * Get a token used for replays when the initiating image is not the one used to create the archive
     * connection/session.
     *
     * @return the replay token
     */
    std::int64_t replayToken() const
    {
        return m_replayToken;
    }

    /**
     * Get the subscription registration id to be used when doing a start replay using response channels and the
     * response subscription is already created.
     *
     * @return registrationId of the subscription to receive the replay (should be set up with control-mode=response).
     */
    ReplayParams &subscriptionRegistrationId(std::int64_t registrationId)
    {
        m_subscriptionRegistrationId = registrationId;
        return *this;
    }

    /**
     * Set the subscription registration id to be used when doing a start replay using response channels and the
     * response subscription is already created.
     *
     * @param registrationId of the subscription to receive the replay (should be set up with control-mode=response).
     */
    std::int64_t subscriptionRegistrationId() const
    {
        return m_subscriptionRegistrationId;
    }

private:
    std::int32_t m_boundingLimitCounterId;
    std::int32_t m_fileIoMaxLength;
    std::int64_t m_position;
    std::int64_t m_length;
    std::int64_t m_replayToken;
    std::int64_t m_subscriptionRegistrationId;
};

/**
 * Proxy class for encapsulating encoding and sending of control protocol messages to an archive.
 */
class ArchiveProxy
{
public:
    explicit ArchiveProxy(std::shared_ptr<ExclusivePublication> publication, int retryAttempts = 3) :
        m_buffer(m_array.data(), m_array.size()),
        m_publication(std::move(publication)),
        m_retryAttempts(retryAttempts)
    {
    }

    /**
     * Get the Publication used for sending control messages.
     *
     * @return the Publication used for sending control messages.
     */
    inline std::shared_ptr<ExclusivePublication> publication()
    {
        return m_publication;
    }

    /**
     * Try Connect to an archive on its control interface providing the response stream details. Only one attempt will
     * be made to offer the request.
     *
     * @param responseChannel  for the control message responses.
     * @param responseStreamId for the control message responses.
     * @param encodedCredentials for the connect request.
     * @param correlationId    for this request.
     * @return true if successfully offered otherwise false.
     */
    bool tryConnect(
        const std::string &responseChannel,
        std::int32_t responseStreamId,
        std::pair<const char *, std::uint32_t> encodedCredentials,
        std::int64_t correlationId)
    {
        const util::index_t length = connectRequest(
            m_buffer,
            responseChannel,
            responseStreamId,
            encodedCredentials,
            correlationId);

        return m_publication->offer(m_buffer, 0, length) > 0;
    }

    /**
     * Try send a challenge response to an archive on its control interface providing the response details.
     * Only one attempt will be made to offer the response.
     *
     * @param encodedCredentials for the response.
     * @param correlationId      for this response.
     * @param controlSessionId   for this response.
     * @return true if successfully offered otherwise false.
     */
    bool tryChallengeResponse(
        std::pair<const char *, std::uint32_t> encodedCredentials,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = challengeResponse(
            m_buffer,
            encodedCredentials,
            correlationId,
            controlSessionId);

        return m_publication->offer(m_buffer, 0, length) > 0;
    }

    /**
     * Keep this archive session alive by notifying the archive.
     *
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool keepAlive(std::int64_t correlationId, std::int64_t controlSessionId)
    {
        const util::index_t length = keepAlive(m_buffer, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Close this control session with the archive.
     *
     * @param controlSessionId with the archive.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool closeSession(std::int64_t controlSessionId)
    {
        const util::index_t length = closeSession(m_buffer, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Resolve id the archive.
     *
     * @param controlSessionId with the archive.
     * @param correlationId with the archive.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool archiveId(std::int64_t controlSessionId, std::int64_t correlationId)
    {
        const util::index_t length = archiveId(m_buffer, controlSessionId, correlationId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Start recording streams for a given channel and stream id pairing.
     *
     * @param channel          to be recorded.
     * @param streamId         to be recorded.
     * @param sourceLocation   of the publication to be recorded.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool startRecording(
        const std::string &channel,
        std::int32_t streamId,
        bool localSource,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = startRecording(
            m_buffer, channel, streamId, localSource, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Start recording streams for a given channel and stream id pairing.
     *
     * @param channel          to be recorded.
     * @param streamId         to be recorded.
     * @param sourceLocation   of the publication to be recorded.
     * @param autoStop         if the recording should be automatically stopped when complete.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool startRecording(
        const std::string &channel,
        std::int32_t streamId,
        bool localSource,
        bool autoStop,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = startRecording(
            m_buffer, channel, streamId, localSource, autoStop, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Extend an existing, non-active, recorded stream for a the same channel and stream id.
     *
     * The channel must be configured for the initial position from which it will be extended. This can be done
     * with ChannelUriStringBuilder#initialPosition(std::int64_t, std::int32_t, std::int32_t). The details required
     * to initialise can be found by calling #listRecording(std::int64_t, std::int64_t, std::int64_t).
     *
     * @param channel          to be recorded.
     * @param streamId         to be recorded.
     * @param sourceLocation   of the publication to be recorded.
     * @param recordingId      to be extended.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool extendRecording(
        const std::string &channel,
        std::int32_t streamId,
        bool localSource,
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = extendRecording(
            m_buffer, channel, streamId, localSource, recordingId, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Extend an existing, non-active, recorded stream for a the same channel and stream id.
     *
     * The channel must be configured for the initial position from which it will be extended. This can be done
     * with ChannelUriStringBuilder#initialPosition(std::int64_t, std::int32_t, std::int32_t). The details required
     * to initialise can be found by calling #listRecording(std::int64_t, std::int64_t, std::int64_t).
     *
     * @param channel          to be recorded.
     * @param streamId         to be recorded.
     * @param sourceLocation   of the publication to be recorded.
     * @param autoStop         if the recording should be automatically stopped when complete.
     * @param recordingId      to be extended.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool extendRecording(
        const std::string &channel,
        std::int32_t streamId,
        bool localSource,
        bool autoStop,
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = extendRecording(
            m_buffer, channel, streamId, localSource, autoStop, recordingId, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Stop an active recording.
     *
     * @param channel          to be stopped.
     * @param streamId         to be stopped.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool stopRecording(
        const std::string &channel, std::int32_t streamId, std::int64_t correlationId, std::int64_t controlSessionId)
    {
        const util::index_t length = stopRecording(m_buffer, channel, streamId, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Stop an active recording by the Subscription#registrationId it was registered with.
     *
     * @param subscriptionId   that identifies the subscription in the archive doing the recording.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool stopRecording(std::int64_t subscriptionId, std::int64_t correlationId, std::int64_t controlSessionId)
    {
        const util::index_t length = stopRecording(m_buffer, subscriptionId, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Stop an active recording by the recording id.
     *
     * @param recordingId      that identifies an existing recording.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool stopRecordingByIdentity(std::int64_t recordingId, std::int64_t correlationId, std::int64_t controlSessionId)
    {
        const util::index_t length = stopRecordingByIdentity(m_buffer, recordingId, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
 * Replay a recording from a given position.
 *
 * @param recordingId      to be replayed.
 * @param position         from which the replay should be started.
 * @param length           of the stream to be replayed. Use std::numeric_limits<std::int64_t>::max to follow a live stream.
 * @param replayChannel    to which the replay should be sent.
 * @param replayStreamId   to which the replay should be sent.
 * @param correlationId    for this request.
 * @param controlSessionId for this request.
 * @tparam IdleStrategy to use between Publication::offer attempts.
 * @return true if successfully offered otherwise false.
 */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool replay(
        std::int64_t recordingId,
        const std::string &replayChannel,
        std::int32_t replayStreamId,
        const ReplayParams &replyParams,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        util::index_t msgLength;
        if (replyParams.isBounded())
        {
            msgLength = boundedReplay(
                m_buffer,
                recordingId,
                replyParams.position(),
                replyParams.length(),
                replyParams.boundingLimitCounterId(),
                replayChannel,
                replayStreamId,
                replyParams.fileIoMaxLength(),
                replyParams.replayToken(),
                correlationId,
                controlSessionId);
        }
        else
        {
            msgLength = replay(
                m_buffer,
                recordingId,
                replyParams.position(),
                replyParams.length(),
                replayChannel,
                replayStreamId,
                replyParams.fileIoMaxLength(),
                replyParams.replayToken(),
                correlationId,
                controlSessionId);
        }

        return offer<IdleStrategy>(m_buffer, 0, msgLength);
    }

    /**
     * Replay a recording from a given position.
     *
     * @param recordingId      to be replayed.
     * @param position         from which the replay should be started.
     * @param length           of the stream to be replayed. Use std::numeric_limits<std::int64_t>::max to follow a live stream.
     * @param replayChannel    to which the replay should be sent.
     * @param replayStreamId   to which the replay should be sent.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool replay(
        std::int64_t recordingId,
        std::int64_t position,
        std::int64_t length,
        const std::string &replayChannel,
        std::int32_t replayStreamId,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t msgLength = replay(
            m_buffer,
            recordingId,
            position,
            length,
            replayChannel,
            replayStreamId,
            NULL_VALUE,
            NULL_VALUE,
            correlationId,
            controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, msgLength);
    }

    /**
     * Replay a recording from a given position and bounded by a counter containing position to limit replay.
     *
     * @param recordingId      to be replayed.
     * @param position         from which the replay should be started.
     * @param length           of the stream to be replayed. Use std::numeric_limits<std::int64_t>::max to follow a live stream.
     * @param limitCounterId   used to bound the replay.
     * @param replayChannel    to which the replay should be sent.
     * @param replayStreamId   to which the replay should be sent.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool boundedReplay(
        std::int64_t recordingId,
        std::int64_t position,
        std::int64_t length,
        std::int32_t limitCounterId,
        const std::string &replayChannel,
        std::int32_t replayStreamId,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t msgLength = boundedReplay(
            m_buffer,
            recordingId,
            position,
            length,
            limitCounterId,
            replayChannel,
            replayStreamId,
            NULL_VALUE,
            NULL_VALUE,
            correlationId,
            controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, msgLength);
    }

    /**
     * Stop an existing replay session.
     *
     * @param replaySessionId  that should be stopped.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool stopReplay(std::int64_t replaySessionId, std::int64_t correlationId, std::int64_t controlSessionId)
    {
        const util::index_t length = stopReplay(m_buffer, replaySessionId, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Stop existing replays matching a recording id. If recording id is #NULL_VALUE then match all replays.
     *
     * @param recordingId      that should match replays to be stopped.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool stopAllReplays(std::int64_t recordingId, std::int64_t correlationId, std::int64_t controlSessionId)
    {
        const util::index_t length = stopAllReplays(m_buffer, recordingId, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * List a range of recording descriptors.
     *
     * @param fromRecordingId  at which to begin listing.
     * @param recordCount      for the number of descriptors to be listed.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool listRecordings(
        std::int64_t fromRecordingId,
        std::int32_t recordCount,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = listRecordings(
            m_buffer, fromRecordingId, recordCount, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * List a range of recording descriptors which match a channel URI fragment and stream id.
     *
     * @param fromRecordingId  at which to begin listing.
     * @param recordCount      for the number of descriptors to be listed.
     * @param channelFragment  to match recordings on from the original channel URI in the archive descriptor.
     * @param streamId         to match recordings on.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool listRecordingsForUri(
        std::int64_t fromRecordingId,
        std::int32_t recordCount,
        const std::string &channelFragment,
        std::int32_t streamId,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = listRecordingsForUri(
            m_buffer, fromRecordingId, recordCount, channelFragment, streamId, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * List a recording descriptor for a given recording id.
     *
     * @param recordingId      at which to begin listing.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool listRecording(std::int64_t recordingId, std::int64_t correlationId, std::int64_t controlSessionId)
    {
        const util::index_t length = listRecording(m_buffer, recordingId, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Get the start position of a recording.
     *
     * @param recordingId      of the recording that the start position is being requested for.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool getStartPosition(std::int64_t recordingId, std::int64_t correlationId, std::int64_t controlSessionId)
    {
        const util::index_t length = getStartPosition(m_buffer, recordingId, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Get the recorded position of an active recording.
     *
     * @param recordingId      of the active recording that the position is being requested for.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool getRecordingPosition(std::int64_t recordingId, std::int64_t correlationId, std::int64_t controlSessionId)
    {
        const util::index_t length = getRecordingPosition(m_buffer, recordingId, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Get the stop or active recorded position of a recording.
     *
     * @param recordingId      of the recording that the stop of active recording position is being requested for.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool getMaxRecordedPosition(std::int64_t recordingId, std::int64_t correlationId, std::int64_t controlSessionId)
    {
        const util::index_t length = getMaxRecordedPosition(m_buffer, recordingId, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Get the stop position of a recording.
     *
     * @param recordingId      of the recording that the stop position is being requested for.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool getStopPosition(std::int64_t recordingId, std::int64_t correlationId, std::int64_t controlSessionId)
    {
        const util::index_t length = getStopPosition(m_buffer, recordingId, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Find the last recording that matches the given criteria.
     *
     * @param minRecordingId   to search back to.
     * @param channelFragment  for a contains match on the original channel stored with the archive descriptor.
     * @param streamId         of the recording to match.
     * @param sessionId        of the recording to match.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool findLastMatchingRecording(
        std::int64_t minRecordingId,
        const std::string &channelFragment,
        std::int32_t streamId,
        std::int32_t sessionId,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = findLastMatchingRecording(
            m_buffer, minRecordingId, channelFragment, streamId, sessionId, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Truncate a stopped recording to a given position that is less than the stopped position. The provided position
     * must be on a fragment boundary. Truncating a recording to the start position effectively deletes the recording.
     *
     * @param recordingId      of the stopped recording to be truncated.
     * @param position         to which the recording will be truncated.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool truncateRecording(
        std::int64_t recordingId,
        std::int64_t position,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = truncateRecording(
            m_buffer, recordingId, position, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Purge a stopped recording, i.e. mark recording as 'RecordingState#INVALID' and delete the corresponding segment
     * files. The space in the Catalog will be reclaimed upon compaction.
     *
     * @param recordingId      of the stopped recording to be purged.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool purgeRecording(
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = purgeRecording(m_buffer, recordingId, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * List registered subscriptions in the archive which have been used to record streams.
     *
     * @param pseudoIndex       in the list of active recording subscriptions.
     * @param subscriptionCount for the number of descriptors to be listed.
     * @param channelFragment   for a contains match on the stripped channel used with the registered subscription.
     * @param streamId          for the subscription.
     * @param applyStreamId     when matching.
     * @param correlationId     for this request.
     * @param controlSessionId  for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool listRecordingSubscriptions(
        std::int32_t pseudoIndex,
        std::int32_t subscriptionCount,
        const std::string &channelFragment,
        std::int32_t streamId,
        bool applyStreamId,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = listRecordingSubscriptions(
            m_buffer,
            pseudoIndex,
            subscriptionCount,
            channelFragment,
            streamId,
            applyStreamId,
            correlationId,
            controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
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
     * @param correlationId      for this request.
     * @param controlSessionId   for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool replicate(
        std::int64_t srcRecordingId,
        std::int64_t dstRecordingId,
        std::int32_t srcControlStreamId,
        const std::string &srcControlChannel,
        const std::string &liveDestination,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = replicate(
            m_buffer,
            srcRecordingId,
            dstRecordingId,
            srcControlStreamId,
            srcControlChannel,
            liveDestination,
            correlationId,
            controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
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
     * @param stopPosition       position to stop the replication. {@link AeronArchive#NULL_POSITION} to stop at end
     *                           of current recording.
     * @param srcControlStreamId remote control stream id for the source archive to instruct the replay on.
     * @param srcControlChannel  remote control channel for the source archive to instruct the replay on.
     * @param liveDestination    destination for the live stream if merge is required. Empty string for no merge.
     * @param replicationChannel channel over which the replication will occur. Empty or null for default channel.
     * @param correlationId      for this request.
     * @param controlSessionId   for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool replicate(
        std::int64_t srcRecordingId,
        std::int64_t dstRecordingId,
        std::int64_t stopPosition,
        std::int32_t srcControlStreamId,
        const std::string &srcControlChannel,
        const std::string &liveDestination,
        const std::string &replicationChannel,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = replicate(
            m_buffer,
            srcRecordingId,
            dstRecordingId,
            stopPosition,
            srcControlStreamId,
            srcControlChannel,
            liveDestination,
            replicationChannel,
            correlationId,
            controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Replicate a recording from a source archive to a destination which can be considered a backup for a primary
     * archive. The source recording will be replayed via the provided replay channel and use the original stream id.
     * The behaviour of the replication is defined by the ReplicationParam.
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
     * @param correlationId      for this request.
     * @param controlSessionId   for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool replicate(
        std::int64_t srcRecordingId,
        std::int32_t srcControlStreamId,
        const std::string &srcControlChannel,
        const ReplicationParams &replicationParams,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = replicate(
            m_buffer,
            srcRecordingId,
            replicationParams.dstRecordingId(),
            replicationParams.stopPosition(),
            replicationParams.channelTagId(),
            replicationParams.subscriptionTagId(),
            srcControlStreamId,
            srcControlChannel,
            replicationParams.liveDestination(),
            replicationParams.replicationChannel(),
            replicationParams.fileIoMaxLength(),
            replicationParams.replicationSessionId(),
            replicationParams.encodedCredentials(),
            correlationId,
            controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
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
     * @param correlationId      for this request.
     * @param controlSessionId   for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool taggedReplicate(
        std::int64_t srcRecordingId,
        std::int64_t dstRecordingId,
        std::int64_t channelTagId,
        std::int64_t subscriptionTagId,
        std::int32_t srcControlStreamId,
        const std::string &srcControlChannel,
        const std::string &liveDestination,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = taggedReplicate(
            m_buffer,
            srcRecordingId,
            dstRecordingId,
            channelTagId,
            subscriptionTagId,
            srcControlStreamId,
            srcControlChannel,
            liveDestination,
            correlationId,
            controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
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
     * @param correlationId      for this request.
     * @param controlSessionId   for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool taggedReplicate(
        std::int64_t srcRecordingId,
        std::int64_t dstRecordingId,
        std::int64_t stopPosition,
        std::int64_t channelTagId,
        std::int64_t subscriptionTagId,
        std::int32_t srcControlStreamId,
        const std::string &srcControlChannel,
        const std::string &liveDestination,
        const std::string &replicationChannel,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = taggedReplicate(
            m_buffer,
            srcRecordingId,
            dstRecordingId,
            stopPosition,
            channelTagId,
            subscriptionTagId,
            srcControlStreamId,
            srcControlChannel,
            liveDestination,
            replicationChannel,
            correlationId,
            controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Stop a replication session by id.
     *
     * @param replicationId    of replication session to be stopped.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool stopReplication(std::int64_t replicationId, std::int64_t correlationId, std::int64_t controlSessionId)
    {
        const util::index_t length = stopReplication(m_buffer, replicationId, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
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
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool detachSegments(
        std::int64_t recordingId,
        std::int64_t newStartPosition,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = detachSegments(
            m_buffer, recordingId, newStartPosition, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Delete detached segments which have been previously detached from a recording.
     *
     * @param recordingId      of the recording to purge segments from.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool deleteDetachedSegments(std::int64_t recordingId, std::int64_t correlationId, std::int64_t controlSessionId)
    {
        const util::index_t length = deleteDetachedSegments(m_buffer, recordingId, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
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
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool purgeSegments(
        std::int64_t recordingId,
        std::int64_t newStartPosition,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = purgeSegments(
            m_buffer, recordingId, newStartPosition, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Attach segments to the beginning of a recording to restore history that was previously detached.
     * <p>
     * Segment files must match the existing recording and join exactly to the start position of the recording
     * they are being attached to.
     *
     * @param recordingId      of the recording to attach segments to.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool attachSegments(std::int64_t recordingId, std::int64_t correlationId, std::int64_t controlSessionId)
    {
        const util::index_t length = attachSegments(m_buffer, recordingId, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
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
     * @param srcRecordingId   source recording from which the segments will be migrated.
     * @param dstRecordingId   destination recording to which the segments will be attached.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool migrateSegments(
        std::int64_t srcRecordingId,
        std::int64_t dstRecordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = migrateSegments(
            m_buffer, srcRecordingId, dstRecordingId, correlationId, controlSessionId);

        return offer<IdleStrategy>(m_buffer, 0, length);
    }

    /**
     * Request a token from the archive to use when doing a replay over a response channel.
     *
     * @param correlationId     for this request
     * @param controlSessionId  for this request
     * @param recordingId       to be replayed
     * @tparam IdleStrategy to use between Publication::offer attempts.
     * @return true if successfully offered otherwise false.
     */
    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool requestReplayToken(std::int64_t correlationId, std::int64_t controlSessionId, std::int64_t recordingId)
    {
        const util::index_t length = replayTokenRequest(m_buffer, correlationId, controlSessionId, recordingId);
        return offer<IdleStrategy>(m_buffer, 0, length);
    }

private:
    std::array<std::uint8_t, PROXY_REQUEST_BUFFER_LENGTH> m_array = {};
    AtomicBuffer m_buffer;
    std::shared_ptr<ExclusivePublication> m_publication;
    const int m_retryAttempts;

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool offer(AtomicBuffer &buffer, util::index_t offset, util::index_t length)
    {
        IdleStrategy idle;

        int attempts = m_retryAttempts;
        while (true)
        {
            const std::int64_t result = m_publication->offer(m_buffer, offset, length);
            if (result > 0)
            {
                return true;
            }

            if (result == PUBLICATION_CLOSED)
            {
                throw ArchiveException("connection to the archive has been closed", SOURCEINFO);
            }

            if (result == NOT_CONNECTED)
            {
                throw ArchiveException("connection to the archive is no longer available", SOURCEINFO);
            }

            if (result == MAX_POSITION_EXCEEDED)
            {
                throw ArchiveException("offer failed due to max position being reached", SOURCEINFO);
            }

            if (--attempts <= 0)
            {
                return false;
            }

            idle.idle();
        }
    }

    static util::index_t connectRequest(
        AtomicBuffer &buffer,
        const std::string &responseChannel,
        std::int32_t responseStreamId,
        std::pair<const char *, std::uint32_t> encodedCredentials,
        std::int64_t correlationId);

    static util::index_t closeSession(AtomicBuffer &buffer, std::int64_t controlSessionId);

    static util::index_t archiveId(AtomicBuffer &buffer, std::int64_t correlationId, std::int64_t controlSessionId);

    static util::index_t startRecording(
        AtomicBuffer &buffer,
        const std::string &channel,
        std::int32_t streamId,
        bool localSource,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t startRecording(
        AtomicBuffer &buffer,
        const std::string &channel,
        std::int32_t streamId,
        bool localSource,
        bool autoStop,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t extendRecording(
        AtomicBuffer &buffer,
        const std::string &channel,
        std::int32_t streamId,
        bool localSource,
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t extendRecording(
        AtomicBuffer &buffer,
        const std::string &channel,
        std::int32_t streamId,
        bool localSource,
        bool autoStop,
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t stopRecording(
        AtomicBuffer &buffer,
        const std::string &channel,
        std::int32_t streamId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t stopRecording(
        AtomicBuffer &buffer,
        std::int64_t subscriptionId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t stopRecordingByIdentity(
        AtomicBuffer &buffer,
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static index_t replay(
        AtomicBuffer &buffer,
        std::int64_t recordingId,
        std::int64_t position,
        std::int64_t length,
        const std::string &replayChannel,
        std::int32_t replayStreamId,
        std::int32_t fileIoMaxLength,
        std::int64_t replayToken,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t boundedReplay(
        AtomicBuffer &buffer,
        std::int64_t recordingId,
        std::int64_t position,
        std::int64_t length,
        std::int32_t limitCounterId,
        const std::string &replayChannel,
        std::int32_t replayStreamId,
        std::int32_t fileIoMaxLength,
        std::int64_t replayToken,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t stopReplay(
        AtomicBuffer &buffer,
        std::int64_t replaySessionId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t stopAllReplays(
        AtomicBuffer &buffer,
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t listRecordings(
        AtomicBuffer &buffer,
        std::int64_t fromRecordingId,
        std::int32_t recordCount,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t listRecordingsForUri(
        AtomicBuffer &buffer,
        std::int64_t fromRecordingId,
        std::int32_t recordCount,
        const std::string &channelFragment,
        std::int32_t streamId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t listRecording(
        AtomicBuffer &buffer,
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t getStartPosition(
        AtomicBuffer &buffer,
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t getRecordingPosition(
        AtomicBuffer &buffer,
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t getStopPosition(
        AtomicBuffer &buffer,
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t getMaxRecordedPosition(
        AtomicBuffer &buffer,
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t findLastMatchingRecording(
        AtomicBuffer &buffer,
        std::int64_t minRecordingId,
        const std::string &channelFragment,
        std::int32_t streamId,
        std::int32_t sessionId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t truncateRecording(
        AtomicBuffer &buffer,
        std::int64_t recordingId,
        std::int64_t position,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t purgeRecording(
        AtomicBuffer &buffer,
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t listRecordingSubscriptions(
        AtomicBuffer &buffer,
        std::int32_t pseudoIndex,
        std::int32_t subscriptionCount,
        const std::string &channelFragment,
        std::int32_t streamId,
        bool applyStreamId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t replicate(
        AtomicBuffer &buffer,
        std::int64_t srcRecordingId,
        std::int64_t dstRecordingId,
        std::int32_t srcControlStreamId,
        const std::string &srcControlChannel,
        const std::string &liveDestination,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    util::index_t replicate(
        AtomicBuffer &buffer,
        std::int64_t srcRecordingId,
        std::int64_t dstRecordingId,
        std::int64_t stopPosition,
        std::int32_t srcControlStreamId,
        const std::string &srcControlChannel,
        const std::string &liveDestination,
        const std::string &replicationChannel,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t taggedReplicate(
        AtomicBuffer &buffer,
        std::int64_t srcRecordingId,
        std::int64_t dstRecordingId,
        std::int64_t channelTagId,
        std::int64_t subscriptionTagId,
        std::int32_t srcControlStreamId,
        const std::string &srcControlChannel,
        const std::string &liveDestination,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t taggedReplicate(
        AtomicBuffer &buffer,
        std::int64_t srcRecordingId,
        std::int64_t dstRecordingId,
        std::int64_t stopPosition,
        std::int64_t channelTagId,
        std::int64_t subscriptionTagId,
        std::int32_t srcControlStreamId,
        const std::string &srcControlChannel,
        const std::string &liveDestination,
        const std::string &replicationChannel,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    util::index_t replicate(
        AtomicBuffer &buffer,
        std::int64_t srcRecordingId,
        std::int64_t dstRecordingId,
        std::int64_t stopPosition,
        std::int64_t channelTagId,
        std::int64_t subscriptionTagId,
        std::int32_t srcControlStreamId,
        const std::string &srcControlChannel,
        const std::string &liveDestination,
        const std::string &replicationChannel,
        std::int32_t fileIoMaxLength,
        std::int32_t replicationSessionId,
        std::pair<const char *, std::uint32_t> encodedCredentials,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t stopReplication(
        AtomicBuffer &buffer,
        std::int64_t replicationId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t detachSegments(
        AtomicBuffer &buffer,
        std::int64_t recordingId,
        std::int64_t newStartPosition,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t deleteDetachedSegments(
        AtomicBuffer &buffer,
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t purgeSegments(
        AtomicBuffer &buffer,
        std::int64_t recordingId,
        std::int64_t newStartPosition,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t attachSegments(
        AtomicBuffer &buffer,
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t migrateSegments(
        AtomicBuffer &buffer,
        std::int64_t srcRecordingId,
        std::int64_t dstRecordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t keepAlive(
        AtomicBuffer &buffer,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t challengeResponse(
        AtomicBuffer &buffer,
        std::pair<const char *, std::uint32_t> encodedCredentials,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t replayTokenRequest(
        AtomicBuffer &buffer,
        std::int64_t correlationId,
        std::int64_t controlSessionId,
        std::int64_t recordingId);
};

}}}

#endif
