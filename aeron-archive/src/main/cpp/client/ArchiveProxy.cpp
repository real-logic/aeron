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

#include "ArchiveException.h"
#include "ArchiveConfiguration.h"
#include "ArchiveProxy.h"
#include "concurrent/YieldingIdleStrategy.h"
#include "aeron_archive_client/ConnectRequest.h"
#include "aeron_archive_client/CloseSessionRequest.h"
#include "aeron_archive_client/StartRecordingRequest.h"
#include "aeron_archive_client/ExtendRecordingRequest.h"
#include "aeron_archive_client/StopRecordingRequest.h"
#include "aeron_archive_client/StopRecordingSubscriptionRequest.h"
#include "aeron_archive_client/ReplayRequest.h"
#include "aeron_archive_client/StopReplayRequest.h"

using namespace aeron::concurrent;
using namespace aeron::archive::client;

bool ArchiveProxy::tryConnect(
    const std::string& responseChannel, std::int32_t responseStreamId, std::int64_t correlationId)
{
    const std::size_t length = MessageHeader::encodedLength()
        + ConnectRequest::sbeBlockLength()
        + ConnectRequest::responseChannelHeaderLength()
        + responseChannel.size();

    BufferClaim bufferClaim;

    if (m_publication->tryClaim(static_cast<std::int32_t>(length), bufferClaim) > 0)
    {
        ConnectRequest connectRequest;

        connectRequest
            .wrapAndApplyHeader(reinterpret_cast<char *>(bufferClaim.buffer().buffer()), 0, length)
            .correlationId(correlationId)
            .responseStreamId(responseStreamId)
            .version(Configuration::ARCHIVE_SEMANTIC_VERSION)
            .putResponseChannel(responseChannel);

        bufferClaim.commit();
        return true;
    }

    return false;
}

template<typename IdleStrategy>
bool ArchiveProxy::closeSession(std::int64_t controlSessionId)
{
    const std::uint64_t closeSessionLength = MessageHeader::encodedLength() + CloseSessionRequest::sbeBlockLength();

    BufferClaim bufferClaim;

    if (tryClaim<IdleStrategy>(static_cast<std::int32_t>(closeSessionLength), bufferClaim) > 0)
    {
        CloseSessionRequest request;

        request
            .wrapAndApplyHeader(reinterpret_cast<char *>(bufferClaim.buffer().buffer()), 0, closeSessionLength)
            .controlSessionId(controlSessionId);

        bufferClaim.commit();
        return true;
    }

    return false;
}

template<typename IdleStrategy>
bool ArchiveProxy::startRecording(
    const std::string& channel,
    std::int32_t streamId,
    bool localSource,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    const std::uint64_t startRecordingRequestLength = MessageHeader::encodedLength()
        + StartRecordingRequest::sbeBlockLength()
        + StartRecordingRequest::channelHeaderLength()
        + channel.size();

    BufferClaim bufferClaim;

    if (tryClaim<IdleStrategy>(static_cast<std::int32_t>(startRecordingRequestLength), bufferClaim) > 0)
    {
        StartRecordingRequest request;

        request
            .wrapAndApplyHeader(reinterpret_cast<char *>(bufferClaim.buffer().buffer()), 0, startRecordingRequestLength)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .streamId(streamId)
            .sourceLocation(localSource ? SourceLocation::LOCAL : SourceLocation::REMOTE)
            .putChannel(channel);

        bufferClaim.commit();
        return true;
    }

    return false;
}

template<typename IdleStrategy>
bool ArchiveProxy::extendRecording(
    const std::string& channel,
    std::int32_t streamId,
    bool localSource,
    std::int64_t recordingId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    const std::uint64_t extendRecordingRequestLength = MessageHeader::encodedLength()
        + ExtendRecordingRequest::sbeBlockLength()
        + ExtendRecordingRequest::channelHeaderLength()
        + channel.size();

    BufferClaim bufferClaim;

    if (tryClaim<IdleStrategy>(static_cast<std::int32_t>(extendRecordingRequestLength), bufferClaim) > 0)
    {
        ExtendRecordingRequest request;

        request
            .wrapAndApplyHeader(reinterpret_cast<char *>(bufferClaim.buffer().buffer()), 0, extendRecordingRequestLength)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId)
            .streamId(streamId)
            .sourceLocation(localSource ? SourceLocation::LOCAL : SourceLocation::REMOTE)
            .putChannel(channel);

        bufferClaim.commit();
        return true;
    }

    return false;
}

template<typename IdleStrategy>
bool ArchiveProxy::stopRecording(
    const std::string& channel,
    std::int32_t streamId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    const std::uint64_t stopRecordingRequestLength = MessageHeader::encodedLength()
        + StopRecordingRequest::sbeBlockLength()
        + StopRecordingRequest::channelHeaderLength()
        + channel.size();

    BufferClaim bufferClaim;

    if (tryClaim<IdleStrategy>(static_cast<std::int32_t>(stopRecordingRequestLength), bufferClaim) > 0)
    {
        StopRecordingRequest request;

        request
            .wrapAndApplyHeader(reinterpret_cast<char *>(bufferClaim.buffer().buffer()), 0, stopRecordingRequestLength)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .streamId(streamId)
            .putChannel(channel);

        bufferClaim.commit();
        return true;
    }

    return false;
}

template<typename IdleStrategy>
bool ArchiveProxy::stopRecording(
    std::int64_t subscriptionId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    const std::uint64_t stopRecordingSubscriptionRequestLength = MessageHeader::encodedLength()
        + StopRecordingSubscriptionRequest::sbeBlockLength();

    BufferClaim bufferClaim;

    if (tryClaim<IdleStrategy>(static_cast<std::int32_t>(stopRecordingSubscriptionRequestLength), bufferClaim) > 0)
    {
        StopRecordingSubscriptionRequest request;

        request
            .wrapAndApplyHeader(reinterpret_cast<char *>(bufferClaim.buffer().buffer()), 0, stopRecordingSubscriptionRequestLength)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .subscriptionId(subscriptionId);

        bufferClaim.commit();
        return true;
    }

    return false;
}

template<typename IdleStrategy>
bool ArchiveProxy::replay(
    std::int64_t recordingId,
    std::int64_t position,
    std::int64_t length,
    const std::string& replayChannel,
    std::int32_t replayStreamId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    const std::uint64_t replayRequestLength = MessageHeader::encodedLength()
        + ReplayRequest::sbeBlockLength()
        + ReplayRequest::replayChannelHeaderLength()
        + replayChannel.size();

    BufferClaim bufferClaim;

    if (tryClaim<IdleStrategy>(static_cast<std::int32_t>(replayRequestLength), bufferClaim) > 0)
    {
        ReplayRequest request;

        request
            .wrapAndApplyHeader(reinterpret_cast<char *>(bufferClaim.buffer().buffer()), 0, replayRequestLength)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId)
            .position(position)
            .length(length)
            .replayStreamId(replayStreamId)
            .putReplayChannel(replayChannel);

        bufferClaim.commit();
        return true;
    }

    return false;
}

template<typename IdleStrategy>
bool ArchiveProxy::stopReplay(
    std::int64_t replaySessionId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    const std::uint64_t stopReplayRequestLength = MessageHeader::encodedLength()
        + StopReplayRequest::sbeBlockLength();

    BufferClaim bufferClaim;

    if (tryClaim<IdleStrategy>(static_cast<std::int32_t>(stopReplayRequestLength), bufferClaim) > 0)
    {
        StopReplayRequest request;

        request
            .wrapAndApplyHeader(reinterpret_cast<char *>(bufferClaim.buffer().buffer()), 0, stopReplayRequestLength)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .replaySessionId(replaySessionId);

        bufferClaim.commit();
        return true;
    }

    return false;
}

template<typename IdleStrategy>
bool ArchiveProxy::tryClaim(std::int32_t length, BufferClaim& bufferClaim)
{
    IdleStrategy idle;

    int attempts = m_retryAttempts;
    while (true)
    {
        const long result = m_publication->tryClaim(length, bufferClaim);
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
            throw ArchiveException("tryClaim failed due to max position being reached", SOURCEINFO);
        }

        if (--attempts <= 0)
        {
            return false;
        }

        idle.idle();
    }
}