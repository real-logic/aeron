/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.archive;

import io.aeron.Image;
import io.aeron.ImageFragmentAssembler;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.*;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Long2ObjectHashMap;

class ControlSessionDemuxer implements Session, FragmentHandler
{
    enum State
    {
        ACTIVE, INACTIVE, CLOSED
    }

    private static final int FRAGMENT_LIMIT = 10;

    private final ControlRequestDecoders decoders;
    private final Image image;
    private final ArchiveConductor conductor;
    private final ImageFragmentAssembler assembler = new ImageFragmentAssembler(this);
    private final Long2ObjectHashMap<ControlSession> controlSessionByIdMap = new Long2ObjectHashMap<>();

    private State state = State.ACTIVE;

    ControlSessionDemuxer(final ControlRequestDecoders decoders, final Image image, final ArchiveConductor conductor)
    {
        this.decoders = decoders;
        this.image = image;
        this.conductor = conductor;
    }

    public long sessionId()
    {
        return image.correlationId();
    }

    public void abort()
    {
        state = State.INACTIVE;
    }

    public void close()
    {
        state = State.CLOSED;
    }

    public boolean isDone()
    {
        return state == State.INACTIVE;
    }

    public int doWork()
    {
        int workCount = 0;

        if (state == State.ACTIVE)
        {
            workCount += image.poll(assembler, FRAGMENT_LIMIT);

            if (0 == workCount && image.isClosed())
            {
                state = State.INACTIVE;
                for (final ControlSession session : controlSessionByIdMap.values())
                {
                    session.abort();
                }
            }
        }

        return workCount;
    }

    @SuppressWarnings("MethodLength")
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final MessageHeaderDecoder headerDecoder = decoders.header;
        headerDecoder.wrap(buffer, offset);

        final int schemaId = headerDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ArchiveException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        final int templateId = headerDecoder.templateId();
        switch (templateId)
        {
            case ConnectRequestDecoder.TEMPLATE_ID:
            {
                final ConnectRequestDecoder connectRequest = decoders.connectRequest;
                connectRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final ControlSession session = conductor.newControlSession(
                    connectRequest.correlationId(),
                    connectRequest.responseStreamId(),
                    connectRequest.version(),
                    connectRequest.responseChannel(),
                    this);
                controlSessionByIdMap.put(session.sessionId(), session);
                break;
            }

            case CloseSessionRequestDecoder.TEMPLATE_ID:
            {
                final CloseSessionRequestDecoder closeSessionRequest = decoders.closeSessionRequest;
                closeSessionRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = closeSessionRequest.controlSessionId();
                final ControlSession session = controlSessionByIdMap.get(controlSessionId);
                if (null != session)
                {
                    session.abort();
                }
                break;
            }

            case StartRecordingRequestDecoder.TEMPLATE_ID:
            {
                final StartRecordingRequestDecoder startRecordingRequest = decoders.startRecordingRequest;
                startRecordingRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long correlationId = startRecordingRequest.correlationId();
                final long controlSessionId = startRecordingRequest.controlSessionId();
                final ControlSession controlSession = getControlSession(controlSessionId, correlationId);

                if (controlSession.majorVersion() < AeronArchive.Configuration.PROTOCOL_MAJOR_VERSION)
                {
                    final ExpandableArrayBuffer tempBuffer = decoders.tempBuffer;
                    final int fixedLength = StartRecordingRequestDecoder.sourceLocationEncodingOffset() + 1;
                    int i = MessageHeaderDecoder.ENCODED_LENGTH;

                    tempBuffer.putBytes(0, buffer, offset + i, fixedLength);
                    i += fixedLength;

                    final int padLength = 3;
                    tempBuffer.setMemory(fixedLength, padLength, (byte)0);
                    tempBuffer.putBytes(fixedLength + padLength, buffer, offset + i, length - i);

                    startRecordingRequest.wrap(
                        tempBuffer, 0, StartRecordingRequestDecoder.BLOCK_LENGTH, headerDecoder.version());
                }

                controlSession.onStartRecording(
                    correlationId,
                    startRecordingRequest.streamId(),
                    startRecordingRequest.sourceLocation(),
                    startRecordingRequest.channel());
                break;
            }

            case StopRecordingRequestDecoder.TEMPLATE_ID:
            {
                final StopRecordingRequestDecoder stopRecordingRequest = decoders.stopRecordingRequest;
                stopRecordingRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long correlationId = stopRecordingRequest.correlationId();
                final long controlSessionId = stopRecordingRequest.controlSessionId();
                final ControlSession controlSession = getControlSession(controlSessionId, correlationId);
                controlSession.onStopRecording(
                    correlationId,
                    stopRecordingRequest.streamId(),
                    stopRecordingRequest.channel());
                break;
            }

            case ReplayRequestDecoder.TEMPLATE_ID:
            {
                final ReplayRequestDecoder replayRequest = decoders.replayRequest;
                replayRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long correlationId = replayRequest.correlationId();
                final long controlSessionId = replayRequest.controlSessionId();
                final ControlSession controlSession = getControlSession(controlSessionId, correlationId);
                controlSession.onStartReplay(
                    correlationId,
                    replayRequest.recordingId(),
                    replayRequest.position(),
                    replayRequest.length(),
                    replayRequest.replayStreamId(),
                    replayRequest.replayChannel());
                break;
            }

            case StopReplayRequestDecoder.TEMPLATE_ID:
            {
                final StopReplayRequestDecoder stopReplayRequest = decoders.stopReplayRequest;
                stopReplayRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long correlationId = stopReplayRequest.correlationId();
                final long controlSessionId = stopReplayRequest.controlSessionId();
                final ControlSession controlSession = getControlSession(controlSessionId, correlationId);
                controlSession.onStopReplay(
                    correlationId,
                    stopReplayRequest.replaySessionId());
                break;
            }

            case ListRecordingsRequestDecoder.TEMPLATE_ID:
            {
                final ListRecordingsRequestDecoder listRecordingsRequest = decoders.listRecordingsRequest;
                listRecordingsRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long correlationId = listRecordingsRequest.correlationId();
                final long controlSessionId = listRecordingsRequest.controlSessionId();
                final ControlSession controlSession = getControlSession(controlSessionId, correlationId);
                controlSession.onListRecordings(
                    correlationId,
                    listRecordingsRequest.fromRecordingId(),
                    listRecordingsRequest.recordCount());
                break;
            }

            case ListRecordingsForUriRequestDecoder.TEMPLATE_ID:
            {
                final ListRecordingsForUriRequestDecoder listRecordingsForUriRequest =
                    decoders.listRecordingsForUriRequest;
                listRecordingsForUriRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final int channelLength = listRecordingsForUriRequest.channelLength();
                final byte[] bytes = 0 == channelLength ? ArrayUtil.EMPTY_BYTE_ARRAY : new byte[channelLength];
                listRecordingsForUriRequest.getChannel(bytes, 0, channelLength);

                final long correlationId = listRecordingsForUriRequest.correlationId();
                final long controlSessionId = listRecordingsForUriRequest.controlSessionId();
                final ControlSession controlSession = getControlSession(controlSessionId, correlationId);
                controlSession.onListRecordingsForUri(
                    correlationId,
                    listRecordingsForUriRequest.fromRecordingId(),
                    listRecordingsForUriRequest.recordCount(),
                    listRecordingsForUriRequest.streamId(),
                    bytes);
                break;
            }

            case ListRecordingRequestDecoder.TEMPLATE_ID:
            {
                final ListRecordingRequestDecoder listRecordingRequest = decoders.listRecordingRequest;
                listRecordingRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long correlationId = listRecordingRequest.correlationId();
                final long controlSessionId = listRecordingRequest.controlSessionId();
                final ControlSession controlSession = getControlSession(controlSessionId, correlationId);
                controlSession.onListRecording(
                    correlationId,
                    listRecordingRequest.recordingId());
                break;
            }

            case ExtendRecordingRequestDecoder.TEMPLATE_ID:
            {
                final ExtendRecordingRequestDecoder extendRecordingRequest = decoders.extendRecordingRequest;
                extendRecordingRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long correlationId = extendRecordingRequest.correlationId();
                final long controlSessionId = extendRecordingRequest.controlSessionId();
                final ControlSession controlSession = getControlSession(controlSessionId, correlationId);

                if (controlSession.majorVersion() < AeronArchive.Configuration.PROTOCOL_MAJOR_VERSION)
                {
                    final ExpandableArrayBuffer tempBuffer = decoders.tempBuffer;
                    final int fixedLength = ExtendRecordingRequestDecoder.sourceLocationEncodingOffset() + 1;
                    int i = MessageHeaderDecoder.ENCODED_LENGTH;

                    tempBuffer.putBytes(0, buffer, offset + i, fixedLength);
                    i += fixedLength;

                    final int padLength = 3;
                    tempBuffer.setMemory(fixedLength, padLength, (byte)0);
                    tempBuffer.putBytes(fixedLength + padLength, buffer, offset + i, length - i);

                    extendRecordingRequest.wrap(
                        tempBuffer, 0, ExtendRecordingRequestDecoder.BLOCK_LENGTH, headerDecoder.version());
                }

                controlSession.onExtendRecording(
                    correlationId,
                    extendRecordingRequest.recordingId(),
                    extendRecordingRequest.streamId(),
                    extendRecordingRequest.sourceLocation(),
                    extendRecordingRequest.channel());
                break;
            }

            case RecordingPositionRequestDecoder.TEMPLATE_ID:
            {
                final RecordingPositionRequestDecoder recordingPositionRequest = decoders.recordingPositionRequest;
                recordingPositionRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long correlationId = recordingPositionRequest.correlationId();
                final long controlSessionId = recordingPositionRequest.controlSessionId();
                final ControlSession controlSession = getControlSession(controlSessionId, correlationId);
                controlSession.onGetRecordingPosition(
                    correlationId,
                    recordingPositionRequest.recordingId());
                break;
            }

            case TruncateRecordingRequestDecoder.TEMPLATE_ID:
            {
                final TruncateRecordingRequestDecoder truncateRecordingRequest = decoders.truncateRecordingRequest;
                truncateRecordingRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long correlationId = truncateRecordingRequest.correlationId();
                final long controlSessionId = truncateRecordingRequest.controlSessionId();
                final ControlSession controlSession = getControlSession(controlSessionId, correlationId);
                controlSession.onTruncateRecording(
                    correlationId,
                    truncateRecordingRequest.recordingId(),
                    truncateRecordingRequest.position());
                break;
            }

            case StopRecordingSubscriptionRequestDecoder.TEMPLATE_ID:
            {
                final StopRecordingSubscriptionRequestDecoder stopRecordingSubscriptionRequest =
                    decoders.stopRecordingSubscriptionRequest;
                stopRecordingSubscriptionRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long correlationId = stopRecordingSubscriptionRequest.correlationId();
                final long controlSessionId = stopRecordingSubscriptionRequest.controlSessionId();
                final ControlSession controlSession = getControlSession(controlSessionId, correlationId);
                controlSession.onStopRecordingSubscription(
                    correlationId,
                    stopRecordingSubscriptionRequest.subscriptionId());
                break;
            }

            case StopPositionRequestDecoder.TEMPLATE_ID:
            {
                final StopPositionRequestDecoder stopPositionRequest = decoders.stopPositionRequest;
                stopPositionRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long correlationId = stopPositionRequest.correlationId();
                final long controlSessionId = stopPositionRequest.controlSessionId();
                final ControlSession controlSession = getControlSession(controlSessionId, correlationId);
                controlSession.onGetStopPosition(
                    correlationId,
                    stopPositionRequest.recordingId());
                break;
            }

            case FindLastMatchingRecordingRequestDecoder.TEMPLATE_ID:
            {
                final FindLastMatchingRecordingRequestDecoder findLastMatchingRecordingRequest =
                    decoders.findLastMatchingRecordingRequest;
                findLastMatchingRecordingRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final int channelLength = findLastMatchingRecordingRequest.channelLength();
                final byte[] bytes = 0 == channelLength ? ArrayUtil.EMPTY_BYTE_ARRAY : new byte[channelLength];
                findLastMatchingRecordingRequest.getChannel(bytes, 0, channelLength);

                final long correlationId = findLastMatchingRecordingRequest.correlationId();
                final long controlSessionId = findLastMatchingRecordingRequest.controlSessionId();
                final ControlSession controlSession = getControlSession(controlSessionId, correlationId);
                controlSession.onFindLastMatchingRecording(
                    correlationId,
                    findLastMatchingRecordingRequest.minRecordingId(),
                    findLastMatchingRecordingRequest.sessionId(),
                    findLastMatchingRecordingRequest.streamId(), bytes);
                break;
            }

            case ListRecordingSubscriptionsRequestDecoder.TEMPLATE_ID:
            {
                final ListRecordingSubscriptionsRequestDecoder listRecordingSubscriptionsRequest =
                    decoders.listRecordingSubscriptionsRequest;
                listRecordingSubscriptionsRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long correlationId = listRecordingSubscriptionsRequest.correlationId();
                final long controlSessionId = listRecordingSubscriptionsRequest.controlSessionId();
                final ControlSession controlSession = getControlSession(controlSessionId, correlationId);
                controlSession.onListRecordingSubscriptions(
                    correlationId,
                    listRecordingSubscriptionsRequest.pseudoIndex(),
                    listRecordingSubscriptionsRequest.subscriptionCount(),
                    listRecordingSubscriptionsRequest.applyStreamId() == BooleanType.TRUE,
                    listRecordingSubscriptionsRequest.streamId(),
                    listRecordingSubscriptionsRequest.channel());
                break;
            }

            case BoundedReplayRequestDecoder.TEMPLATE_ID:
            {
                final BoundedReplayRequestDecoder boundedReplayRequest = decoders.boundedReplayRequest;
                boundedReplayRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long correlationId = boundedReplayRequest.correlationId();
                final long controlSessionId = boundedReplayRequest.controlSessionId();
                final ControlSession controlSession = getControlSession(controlSessionId, correlationId);
                controlSession.onStartBoundedReplay(
                    correlationId,
                    boundedReplayRequest.recordingId(),
                    boundedReplayRequest.position(),
                    boundedReplayRequest.length(),
                    boundedReplayRequest.limitCounterId(),
                    boundedReplayRequest.replayStreamId(),
                    boundedReplayRequest.replayChannel());
                break;
            }

            case StopAllReplaysRequestDecoder.TEMPLATE_ID:
            {
                final StopAllReplaysRequestDecoder stopAllReplaysRequest = decoders.stopAllReplaysRequest;
                stopAllReplaysRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long correlationId = stopAllReplaysRequest.correlationId();
                final long controlSessionId = stopAllReplaysRequest.controlSessionId();
                final ControlSession controlSession = getControlSession(controlSessionId, correlationId);
                controlSession.onStopAllReplays(
                    correlationId,
                    stopAllReplaysRequest.recordingId());
                break;
            }

            case ReplicateRequestDecoder.TEMPLATE_ID:
            {
                final ReplicateRequestDecoder replicateRequest = decoders.replicateRequest;
                replicateRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long correlationId = replicateRequest.correlationId();
                final long controlSessionId = replicateRequest.controlSessionId();
                final ControlSession controlSession = getControlSession(controlSessionId, correlationId);
                controlSession.onReplicate(
                    correlationId,
                    replicateRequest.srcRecordingId(),
                    replicateRequest.dstRecordingId(),
                    replicateRequest.srcControlStreamId(),
                    replicateRequest.srcControlChannel(),
                    replicateRequest.liveDestination());
                break;
            }

            case StopReplicationRequestDecoder.TEMPLATE_ID:
            {
                final StopReplicationRequestDecoder stopReplicationRequest = decoders.stopReplicationRequest;
                stopReplicationRequest.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long correlationId = stopReplicationRequest.correlationId();
                final long controlSessionId = stopReplicationRequest.controlSessionId();
                final ControlSession controlSession = getControlSession(controlSessionId, correlationId);
                controlSession.onStopReplication(
                    correlationId,
                    stopReplicationRequest.replicationId());
                break;
            }
        }
    }

    void removeControlSession(final ControlSession controlSession)
    {
        controlSessionByIdMap.remove(controlSession.sessionId());
    }

    private ControlSession getControlSession(final long controlSessionId, final long correlationId)
    {
        final ControlSession controlSession = controlSessionByIdMap.get(controlSessionId);
        if (controlSession == null)
        {
            throw new ArchiveException("unknown controlSessionId=" + controlSessionId +
                " for correlationId=" + correlationId +
                " from source=" + image.sourceIdentity());
        }

        return controlSession;
    }
}
