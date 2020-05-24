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
 * Proxy class for encapsulating encoding and sending of control protocol messages to an archive.
 */
class ArchiveProxy
{
public:
    explicit ArchiveProxy(std::shared_ptr<ExclusivePublication> publication, int retryAttempts = 3) :
        m_array(),
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
        std::int64_t position,
        std::int64_t length,
        const std::string &replayChannel,
        std::int32_t replayStreamId,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t msgLength = replay(
            m_buffer, recordingId, position, length, replayChannel, replayStreamId, correlationId, controlSessionId);

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

private:
    std::array<std::uint8_t, PROXY_REQUEST_BUFFER_LENGTH> m_array;
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
            const int64_t result = m_publication->offer(m_buffer, offset, length);
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

    static util::index_t replay(
        AtomicBuffer &buffer,
        std::int64_t recordingId,
        std::int64_t position,
        std::int64_t length,
        const std::string &replayChannel,
        std::int32_t replayStreamId,
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
};

}}}

#endif //AERON_ARCHIVE_PROXY_H
