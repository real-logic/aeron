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

#ifndef AERON_ARCHIVE_ARCHIVE_PROXY_H
#define AERON_ARCHIVE_ARCHIVE_PROXY_H

#include <array>

#include "Aeron.h"
#include "concurrent/BackOffIdleStrategy.h"
#include "ArchiveException.h"

namespace aeron { namespace archive { namespace client {

/// Length of buffer to use in proxy to hold messages for construction.
constexpr const std::size_t PROXY_REQUEST_BUFFER_LENGTH = 8 * 1024;

/**
 * Proxy class for encapsulating encoding and sending of control protocol messages to an archive.
 */
class ArchiveProxy
{
public:
    explicit ArchiveProxy(
        std::shared_ptr<ExclusivePublication> publication,
        int retryAttempts = 3) :
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
     * @param correlationId    for this request.
     * @return true if successfully offered otherwise false.
     */
    bool tryConnect(const std::string& responseChannel, std::int32_t responseStreamId, std::int64_t correlationId)
    {
        const util::index_t length = connectRequest(m_buffer, responseChannel, responseStreamId, correlationId);
        return (m_publication->offer(m_buffer, 0, length) > 0);
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
        const std::string& channel,
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
     * Extend a recorded stream for a given channel and stream id pairing.
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
        const std::string& channel,
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
        const std::string& channel,
        std::int32_t streamId,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
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
    bool stopRecording(
        std::int64_t subscriptionId,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = stopRecording(m_buffer, subscriptionId, correlationId, controlSessionId);
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
        const std::string& replayChannel,
        std::int32_t replayStreamId,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t msgLength = replay(
            m_buffer, recordingId, position, length, replayChannel, replayStreamId, correlationId, controlSessionId);
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
    bool stopReplay(
        std::int64_t replaySessionId,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = stopReplay(m_buffer, replaySessionId, correlationId, controlSessionId);
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
        const std::string& channelFragment,
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
    bool listRecording(
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
    {
        const util::index_t length = listRecording(m_buffer, recordingId, correlationId, controlSessionId);
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
    bool getRecordingPosition(
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
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
    bool getStopPosition(
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId)
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
        const std::string& channelFragment,
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
        const std::string& channelFragment,
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

private:
    std::array<std::uint8_t, PROXY_REQUEST_BUFFER_LENGTH> m_array;
    AtomicBuffer m_buffer;
    std::shared_ptr<ExclusivePublication> m_publication;
    const int m_retryAttempts;

    template<typename IdleStrategy = aeron::concurrent::BackoffIdleStrategy>
    bool offer(AtomicBuffer& buffer, util::index_t offset, util::index_t length)
    {
        IdleStrategy idle;

        int attempts = m_retryAttempts;
        while (true)
        {
            const long result = m_publication->offer(m_buffer, offset, length);
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
        AtomicBuffer& buffer,
        const std::string& responseChannel,
        std::int32_t responseStreamId,
        std::int64_t correlationId);

    static util::index_t closeSession(AtomicBuffer& buffer, std::int64_t controlSessionId);

    static util::index_t startRecording(
        AtomicBuffer& buffer,
        const std::string& channel,
        std::int32_t streamId,
        bool localSource,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t extendRecording(
        AtomicBuffer& buffer,
        const std::string& channel,
        std::int32_t streamId,
        bool localSource,
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t stopRecording(
        AtomicBuffer& buffer,
        const std::string& channel,
        std::int32_t streamId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t stopRecording(
        AtomicBuffer& buffer,
        std::int64_t subscriptionId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t replay(
        AtomicBuffer& buffer,
        std::int64_t recordingId,
        std::int64_t position,
        std::int64_t length,
        const std::string& replayChannel,
        std::int32_t replayStreamId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t stopReplay(
        AtomicBuffer& buffer,
        std::int64_t replaySessionId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t listRecordings(
        AtomicBuffer& buffer,
        std::int64_t fromRecordingId,
        std::int32_t recordCount,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t listRecordingsForUri(
        AtomicBuffer& buffer,
        std::int64_t fromRecordingId,
        std::int32_t recordCount,
        const std::string& channelFragment,
        std::int32_t streamId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t listRecording(
        AtomicBuffer& buffer,
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t getRecordingPosition(
        AtomicBuffer& buffer,
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t getStopPosition(
        AtomicBuffer& buffer,
        std::int64_t recordingId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t findLastMatchingRecording(
        AtomicBuffer& buffer,
        std::int64_t minRecordingId,
        const std::string& channelFragment,
        std::int32_t streamId,
        std::int32_t sessionId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t truncateRecording(
        AtomicBuffer& buffer,
        std::int64_t recordingId,
        std::int64_t position,
        std::int64_t correlationId,
        std::int64_t controlSessionId);

    static util::index_t listRecordingSubscriptions(
        AtomicBuffer& buffer,
        std::int32_t pseudoIndex,
        std::int32_t subscriptionCount,
        const std::string& channelFragment,
        std::int32_t streamId,
        bool applyStreamId,
        std::int64_t correlationId,
        std::int64_t controlSessionId);
};

}}}

#endif //AERON_ARCHIVE_PROXY_H
