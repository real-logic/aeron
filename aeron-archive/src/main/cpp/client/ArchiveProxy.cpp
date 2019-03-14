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
#include "aeron_archive_client/ListRecordingsRequest.h"
#include "aeron_archive_client/ListRecordingsForUriRequest.h"
#include "aeron_archive_client/ListRecordingRequest.h"
#include "aeron_archive_client/RecordingPositionRequest.h"
#include "aeron_archive_client/StopPositionRequest.h"
#include "aeron_archive_client/FindLastMatchingRecordingRequest.h"
#include "aeron_archive_client/TruncateRecordingRequest.h"
#include "aeron_archive_client/ListRecordingSubscriptionsRequest.h"

using namespace aeron;
using namespace aeron::concurrent;
using namespace aeron::archive::client;

template<typename Codec>
inline static Codec& wrapAndApplyHeader(Codec& codec, AtomicBuffer& buffer)
{
    return codec.wrapAndApplyHeader(buffer.sbeData(), 0, static_cast<std::uint64_t>(buffer.capacity()));
}

template<typename Codec>
inline static util::index_t messageAndHeaderLength(Codec& codec)
{
    return static_cast<util::index_t>(MessageHeader::encodedLength() + codec.encodedLength());
}

util::index_t ArchiveProxy::connectRequest(
    AtomicBuffer& buffer,
    const std::string& responseChannel,
    std::int32_t responseStreamId,
    std::int64_t correlationId)
{
    ConnectRequest request;

    wrapAndApplyHeader(request, buffer)
        .correlationId(correlationId)
        .responseStreamId(responseStreamId)
        .version(Configuration::ARCHIVE_SEMANTIC_VERSION)
        .putResponseChannel(responseChannel);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::closeSession(AtomicBuffer& buffer, std::int64_t controlSessionId)
{
    CloseSessionRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::startRecording(
    AtomicBuffer& buffer,
    const std::string& channel,
    std::int32_t streamId,
    bool localSource,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    StartRecordingRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .streamId(streamId)
        .sourceLocation(localSource ? SourceLocation::LOCAL : SourceLocation::REMOTE)
        .putChannel(channel);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::extendRecording(
    AtomicBuffer& buffer,
    const std::string& channel,
    std::int32_t streamId,
    bool localSource,
    std::int64_t recordingId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    ExtendRecordingRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .recordingId(recordingId)
        .streamId(streamId)
        .sourceLocation(localSource ? SourceLocation::LOCAL : SourceLocation::REMOTE)
        .putChannel(channel);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::stopRecording(
    AtomicBuffer& buffer,
    const std::string& channel,
    std::int32_t streamId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    StopRecordingRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .streamId(streamId)
        .putChannel(channel);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::stopRecording(
    AtomicBuffer& buffer,
    std::int64_t subscriptionId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    StopRecordingSubscriptionRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .subscriptionId(subscriptionId);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::replay(
    AtomicBuffer& buffer,
    std::int64_t recordingId,
    std::int64_t position,
    std::int64_t length,
    const std::string& replayChannel,
    std::int32_t replayStreamId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    ReplayRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .recordingId(recordingId)
        .position(position)
        .length(length)
        .replayStreamId(replayStreamId)
        .putReplayChannel(replayChannel);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::stopReplay(
    AtomicBuffer& buffer,
    std::int64_t replaySessionId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    StopReplayRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .replaySessionId(replaySessionId);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::listRecordings(
    AtomicBuffer& buffer,
    std::int64_t fromRecordingId,
    std::int32_t recordCount,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    ListRecordingsRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .fromRecordingId(fromRecordingId)
        .recordCount(recordCount);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::listRecordingsForUri(
    AtomicBuffer& buffer,
    std::int64_t fromRecordingId,
    std::int32_t recordCount,
    const std::string& channelFragment,
    std::int32_t streamId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    ListRecordingsForUriRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .fromRecordingId(fromRecordingId)
        .recordCount(recordCount)
        .streamId(streamId)
        .putChannel(channelFragment);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::listRecording(
    AtomicBuffer& buffer,
    std::int64_t recordingId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    ListRecordingRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .recordingId(recordingId);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::getRecordingPosition(
    AtomicBuffer& buffer,
    std::int64_t recordingId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    RecordingPositionRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .recordingId(recordingId);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::getStopPosition(
    AtomicBuffer& buffer,
    std::int64_t recordingId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    StopPositionRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .recordingId(recordingId);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::findLastMatchingRecording(
    AtomicBuffer& buffer,
    std::int64_t minRecordingId,
    const std::string& channelFragment,
    std::int32_t streamId,
    std::int32_t sessionId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    FindLastMatchingRecordingRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .minRecordingId(minRecordingId)
        .sessionId(sessionId)
        .streamId(streamId)
        .putChannel(channelFragment);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::truncateRecording(
    AtomicBuffer& buffer,
    std::int64_t recordingId,
    std::int64_t position,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    TruncateRecordingRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .recordingId(recordingId)
        .position(position);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::listRecordingSubscriptions(
    AtomicBuffer& buffer,
    std::int32_t pseudoIndex,
    std::int32_t subscriptionCount,
    const std::string& channelFragment,
    std::int32_t streamId,
    bool applyStreamId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    ListRecordingSubscriptionsRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .pseudoIndex(pseudoIndex)
        .subscriptionCount(subscriptionCount)
        .applyStreamId(applyStreamId ? BooleanType::Value::TRUE : BooleanType::Value::FALSE)
        .streamId(streamId)
        .putChannel(channelFragment);

    return messageAndHeaderLength(request);
}
