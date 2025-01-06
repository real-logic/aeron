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

#include "ArchiveConfiguration.h"
#include "ArchiveException.h"
#include "ArchiveProxy.h"
#include "concurrent/YieldingIdleStrategy.h"
#include "aeron_archive_client/BoundedReplayRequest.h"
#include "aeron_archive_client/AuthConnectRequest.h"
#include "aeron_archive_client/ArchiveIdRequest.h"
#include "aeron_archive_client/CloseSessionRequest.h"
#include "aeron_archive_client/StartRecordingRequest.h"
#include "aeron_archive_client/StartRecordingRequest2.h"
#include "aeron_archive_client/ExtendRecordingRequest.h"
#include "aeron_archive_client/ExtendRecordingRequest2.h"
#include "aeron_archive_client/StopRecordingRequest.h"
#include "aeron_archive_client/StopRecordingSubscriptionRequest.h"
#include "aeron_archive_client/StopRecordingByIdentityRequest.h"
#include "aeron_archive_client/ReplayRequest.h"
#include "aeron_archive_client/StopReplayRequest.h"
#include "aeron_archive_client/StopAllReplaysRequest.h"
#include "aeron_archive_client/ListRecordingsRequest.h"
#include "aeron_archive_client/ListRecordingsForUriRequest.h"
#include "aeron_archive_client/ListRecordingRequest.h"
#include "aeron_archive_client/RecordingPositionRequest.h"
#include "aeron_archive_client/StartPositionRequest.h"
#include "aeron_archive_client/StopPositionRequest.h"
#include "aeron_archive_client/FindLastMatchingRecordingRequest.h"
#include "aeron_archive_client/TruncateRecordingRequest.h"
#include "aeron_archive_client/ListRecordingSubscriptionsRequest.h"
#include "aeron_archive_client/ReplicateRequest2.h"
#include "aeron_archive_client/StopReplicationRequest.h"
#include "aeron_archive_client/DetachSegmentsRequest.h"
#include "aeron_archive_client/DeleteDetachedSegmentsRequest.h"
#include "aeron_archive_client/PurgeSegmentsRequest.h"
#include "aeron_archive_client/AttachSegmentsRequest.h"
#include "aeron_archive_client/MigrateSegmentsRequest.h"
#include "aeron_archive_client/KeepAliveRequest.h"
#include "aeron_archive_client/ChallengeResponse.h"
#include "aeron_archive_client/PurgeRecordingRequest.h"
#include "aeron_archive_client/MaxRecordedPositionRequest.h"
#include "aeron_archive_client/ReplayTokenRequest.h"

using namespace aeron;
using namespace aeron::concurrent;
using namespace aeron::archive::client;

template<typename Codec>
inline static Codec &wrapAndApplyHeader(Codec &codec, AtomicBuffer &buffer)
{
    return codec.wrapAndApplyHeader(buffer.sbeData(), 0, static_cast<std::uint64_t>(buffer.capacity()));
}

template<typename Codec>
inline static util::index_t messageAndHeaderLength(Codec &codec)
{
    return static_cast<util::index_t>(MessageHeader::encodedLength() + codec.encodedLength());
}

util::index_t ArchiveProxy::connectRequest(
    AtomicBuffer &buffer,
    const std::string &responseChannel,
    std::int32_t responseStreamId,
    std::pair<const char *, std::uint32_t> encodedCredentials,
    std::int64_t correlationId)
{
    AuthConnectRequest request;

    wrapAndApplyHeader(request, buffer)
        .correlationId(correlationId)
        .responseStreamId(responseStreamId)
        .version(Configuration::ARCHIVE_SEMANTIC_VERSION)
        .putResponseChannel(responseChannel)
        .putEncodedCredentials(encodedCredentials.first, encodedCredentials.second);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::closeSession(AtomicBuffer &buffer, std::int64_t controlSessionId)
{
    CloseSessionRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::archiveId(
    aeron::concurrent::AtomicBuffer &buffer,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    ArchiveIdRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::startRecording(
    AtomicBuffer &buffer,
    const std::string &channel,
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

util::index_t ArchiveProxy::startRecording(
    AtomicBuffer &buffer,
    const std::string &channel,
    std::int32_t streamId,
    bool localSource,
    bool autoStop,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    StartRecordingRequest2 request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .streamId(streamId)
        .sourceLocation(localSource ? SourceLocation::LOCAL : SourceLocation::REMOTE)
        .autoStop(autoStop ? BooleanType::TRUE : BooleanType::FALSE)
        .putChannel(channel);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::extendRecording(
    AtomicBuffer &buffer,
    const std::string &channel,
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

util::index_t ArchiveProxy::extendRecording(
    AtomicBuffer &buffer,
    const std::string &channel,
    std::int32_t streamId,
    bool localSource,
    bool autoStop,
    std::int64_t recordingId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    ExtendRecordingRequest2 request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .recordingId(recordingId)
        .streamId(streamId)
        .sourceLocation(localSource ? SourceLocation::LOCAL : SourceLocation::REMOTE)
        .autoStop(autoStop ? BooleanType::TRUE : BooleanType::FALSE)
        .putChannel(channel);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::stopRecording(
    AtomicBuffer &buffer,
    const std::string &channel,
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
    AtomicBuffer &buffer,
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

util::index_t ArchiveProxy::stopRecordingByIdentity(
    AtomicBuffer &buffer,
    std::int64_t recordingId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    StopRecordingByIdentityRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .recordingId(recordingId);

    return messageAndHeaderLength(request);
}

index_t ArchiveProxy::replay(
    AtomicBuffer &buffer,
    std::int64_t recordingId,
    std::int64_t position,
    std::int64_t length,
    const std::string &replayChannel,
    std::int32_t replayStreamId,
    std::int32_t fileIoMaxLength,
    std::int64_t replayToken,
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
        .fileIoMaxLength(fileIoMaxLength)
        .replayToken(replayToken)
        .putReplayChannel(replayChannel);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::boundedReplay(
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
    std::int64_t controlSessionId)
{
    BoundedReplayRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .recordingId(recordingId)
        .position(position)
        .length(length)
        .limitCounterId(limitCounterId)
        .replayStreamId(replayStreamId)
        .fileIoMaxLength(fileIoMaxLength)
        .replayToken(replayToken)
        .putReplayChannel(replayChannel);

    return messageAndHeaderLength(request);
}


util::index_t ArchiveProxy::stopReplay(
    AtomicBuffer &buffer,
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

util::index_t ArchiveProxy::stopAllReplays(
    AtomicBuffer &buffer,
    std::int64_t recordingId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    StopAllReplaysRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .recordingId(recordingId);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::listRecordings(
    AtomicBuffer &buffer,
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
    AtomicBuffer &buffer,
    std::int64_t fromRecordingId,
    std::int32_t recordCount,
    const std::string &channelFragment,
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
    AtomicBuffer &buffer,
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

util::index_t ArchiveProxy::getStartPosition(
    AtomicBuffer &buffer,
    std::int64_t recordingId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    StartPositionRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .recordingId(recordingId);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::getRecordingPosition(
    AtomicBuffer &buffer,
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
    AtomicBuffer &buffer,
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

util::index_t ArchiveProxy::getMaxRecordedPosition(
    AtomicBuffer &buffer,
    std::int64_t recordingId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    MaxRecordedPositionRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .recordingId(recordingId);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::findLastMatchingRecording(
    AtomicBuffer &buffer,
    std::int64_t minRecordingId,
    const std::string &channelFragment,
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
    AtomicBuffer &buffer,
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

util::index_t ArchiveProxy::purgeRecording(
    AtomicBuffer &buffer,
    std::int64_t recordingId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    PurgeRecordingRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .recordingId(recordingId);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::listRecordingSubscriptions(
    AtomicBuffer &buffer,
    std::int32_t pseudoIndex,
    std::int32_t subscriptionCount,
    const std::string &channelFragment,
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

util::index_t ArchiveProxy::replicate(
    AtomicBuffer &buffer,
    std::int64_t srcRecordingId,
    std::int64_t dstRecordingId,
    std::int32_t srcControlStreamId,
    const std::string &srcControlChannel,
    const std::string &liveDestination,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    ReplicateRequest2 request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .stopPosition(NULL_POSITION)
        .channelTagId(NULL_VALUE)
        .subscriptionTagId(NULL_VALUE)
        .srcRecordingId(srcRecordingId)
        .dstRecordingId(dstRecordingId)
        .srcControlStreamId(srcControlStreamId)
        .fileIoMaxLength(NULL_VALUE)
        .putSrcControlChannel(srcControlChannel)
        .putLiveDestination(liveDestination)
        .putReplicationChannel(nullptr, 0);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::replicate(
    AtomicBuffer &buffer,
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
    ReplicateRequest2 request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .srcRecordingId(srcRecordingId)
        .dstRecordingId(dstRecordingId)
        .stopPosition(stopPosition)
        .channelTagId(NULL_VALUE)
        .subscriptionTagId(NULL_VALUE)
        .srcControlStreamId(srcControlStreamId)
        .fileIoMaxLength(NULL_VALUE)
        .putSrcControlChannel(srcControlChannel)
        .putLiveDestination(liveDestination)
        .putReplicationChannel(replicationChannel);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::taggedReplicate(
    AtomicBuffer &buffer,
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
    ReplicateRequest2 request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .srcRecordingId(srcRecordingId)
        .dstRecordingId(dstRecordingId)
        .stopPosition(NULL_POSITION)
        .channelTagId(channelTagId)
        .subscriptionTagId(subscriptionTagId)
        .srcControlStreamId(srcControlStreamId)
        .fileIoMaxLength(NULL_VALUE)
        .putSrcControlChannel(srcControlChannel)
        .putLiveDestination(liveDestination)
        .putReplicationChannel(nullptr, 0);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::taggedReplicate(
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
    std::int64_t controlSessionId)
{
    ReplicateRequest2 request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .srcRecordingId(srcRecordingId)
        .dstRecordingId(dstRecordingId)
        .stopPosition(stopPosition)
        .channelTagId(channelTagId)
        .subscriptionTagId(subscriptionTagId)
        .srcControlStreamId(srcControlStreamId)
        .fileIoMaxLength(NULL_VALUE)
        .putSrcControlChannel(srcControlChannel)
        .putLiveDestination(liveDestination)
        .putReplicationChannel(replicationChannel);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::replicate(
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
    std::int64_t controlSessionId)
{
    ReplicateRequest2 request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .srcRecordingId(srcRecordingId)
        .dstRecordingId(dstRecordingId)
        .stopPosition(stopPosition)
        .channelTagId(channelTagId)
        .subscriptionTagId(subscriptionTagId)
        .srcControlStreamId(srcControlStreamId)
        .fileIoMaxLength(fileIoMaxLength)
        .replicationSessionId(replicationSessionId)
        .putSrcControlChannel(srcControlChannel)
        .putLiveDestination(liveDestination)
        .putReplicationChannel(replicationChannel)
        .putEncodedCredentials(encodedCredentials.first, encodedCredentials.second);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::stopReplication(
    AtomicBuffer &buffer,
    std::int64_t replicationId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    StopReplicationRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .replicationId(replicationId);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::detachSegments(
    AtomicBuffer &buffer,
    std::int64_t recordingId,
    std::int64_t newStartPosition,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    DetachSegmentsRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .recordingId(recordingId)
        .newStartPosition(newStartPosition);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::deleteDetachedSegments(
    AtomicBuffer &buffer,
    std::int64_t recordingId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    DeleteDetachedSegmentsRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .recordingId(recordingId);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::purgeSegments(
    AtomicBuffer &buffer,
    std::int64_t recordingId,
    std::int64_t newStartPosition,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    PurgeSegmentsRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .recordingId(recordingId)
        .newStartPosition(newStartPosition);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::attachSegments(
    AtomicBuffer &buffer,
    std::int64_t recordingId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    AttachSegmentsRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .recordingId(recordingId);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::migrateSegments(
    AtomicBuffer &buffer,
    std::int64_t srcRecordingId,
    std::int64_t dstRecordingId,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    MigrateSegmentsRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .srcRecordingId(srcRecordingId)
        .dstRecordingId(dstRecordingId);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::keepAlive(
    AtomicBuffer &buffer,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    KeepAliveRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId);

    return messageAndHeaderLength(request);
}

util::index_t ArchiveProxy::challengeResponse(
    AtomicBuffer &buffer,
    std::pair<const char *, std::uint32_t> encodedCredentials,
    std::int64_t correlationId,
    std::int64_t controlSessionId)
{
    ChallengeResponse response;

    wrapAndApplyHeader(response, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .putEncodedCredentials(encodedCredentials.first, encodedCredentials.second);

    return messageAndHeaderLength(response);
}

util::index_t ArchiveProxy::replayTokenRequest(
    AtomicBuffer &buffer,
    std::int64_t correlationId,
    std::int64_t controlSessionId,
    std::int64_t recordingId)
{
    ReplayTokenRequest request;

    wrapAndApplyHeader(request, buffer)
        .controlSessionId(controlSessionId)
        .correlationId(correlationId)
        .recordingId(recordingId);

    return messageAndHeaderLength(request);
}
