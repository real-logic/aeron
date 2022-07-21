/*
 * Copyright 2014-2022 Real Logic Limited.
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

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.ImageFragmentAssembler;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.*;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.security.AuthorisationService;
import org.agrona.DirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Long2ObjectHashMap;

class ControlSessionDemuxer implements Session, FragmentHandler
{
    private static final int FRAGMENT_LIMIT = 10;
    private static final int FILE_IO_MAX_LENGTH_VERSION = 7;

    private final ControlRequestDecoders decoders;
    private final Image image;
    private final AuthorisationService authorisationService;
    private final ImageFragmentAssembler assembler = new ImageFragmentAssembler(this);
    private final Long2ObjectHashMap<ControlSession> controlSessionByIdMap = new Long2ObjectHashMap<>();
    private final ArchiveConductor conductor;

    private boolean isActive = true;

    ControlSessionDemuxer(
        final ControlRequestDecoders decoders,
        final Image image,
        final ArchiveConductor conductor,
        final AuthorisationService authorisationService)
    {
        this.decoders = decoders;
        this.image = image;
        this.conductor = conductor;
        this.authorisationService = authorisationService;
    }

    /**
     * {@inheritDoc}
     */
    public long sessionId()
    {
        return image.correlationId();
    }

    /**
     * {@inheritDoc}
     */
    public void abort()
    {
        isActive = false;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
    }

    /**
     * {@inheritDoc}
     */
    public boolean isDone()
    {
        return !isActive;
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        int workCount = 0;

        if (isActive)
        {
            workCount += image.poll(assembler, FRAGMENT_LIMIT);

            if (0 == workCount && image.isClosed())
            {
                isActive = false;
                for (final ControlSession session : controlSessionByIdMap.values())
                {
                    session.abort();
                }
            }
        }

        return workCount;
    }

    /**
     * {@inheritDoc}
     */
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
                final ConnectRequestDecoder decoder = decoders.connectRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final ControlSession session = conductor.newControlSession(
                    decoder.correlationId(),
                    decoder.responseStreamId(),
                    decoder.version(),
                    decoder.responseChannel(),
                    ArrayUtil.EMPTY_BYTE_ARRAY,
                    this);
                controlSessionByIdMap.put(session.sessionId(), session);
                break;
            }

            case CloseSessionRequestDecoder.TEMPLATE_ID:
            {
                final CloseSessionRequestDecoder decoder = decoders.closeSessionRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final ControlSession session = controlSessionByIdMap.get(controlSessionId);
                if (null != session)
                {
                    session.abort();
                }
                break;
            }

            case StartRecordingRequestDecoder.TEMPLATE_ID:
            {
                final StartRecordingRequestDecoder decoder = decoders.startRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStartRecording(
                        correlationId,
                        decoder.streamId(),
                        decoder.sourceLocation(),
                        false,
                        decoder.channel());
                }
                break;
            }

            case StopRecordingRequestDecoder.TEMPLATE_ID:
            {
                final StopRecordingRequestDecoder decoder = decoders.stopRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStopRecording(correlationId, decoder.streamId(), decoder.channel());
                }
                break;
            }

            case ReplayRequestDecoder.TEMPLATE_ID:
            {
                final ReplayRequestDecoder decoder = decoders.replayRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);
                final int fileIoMaxLength = FILE_IO_MAX_LENGTH_VERSION <= headerDecoder.version() ?
                    decoder.fileIoMaxLength() : Aeron.NULL_VALUE;

                if (null != controlSession)
                {
                    controlSession.onStartReplay(
                        correlationId,
                        decoder.recordingId(),
                        decoder.position(),
                        decoder.length(),
                        fileIoMaxLength,
                        decoder.replayStreamId(),
                        decoder.replayChannel());
                }
                break;
            }

            case StopReplayRequestDecoder.TEMPLATE_ID:
            {
                final StopReplayRequestDecoder decoder = decoders.stopReplayRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStopReplay(correlationId, decoder.replaySessionId());
                }
                break;
            }

            case ListRecordingsRequestDecoder.TEMPLATE_ID:
            {
                final ListRecordingsRequestDecoder decoder = decoders.listRecordingsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onListRecordings(correlationId, decoder.fromRecordingId(), decoder.recordCount());
                }
                break;
            }

            case ListRecordingsForUriRequestDecoder.TEMPLATE_ID:
            {
                final ListRecordingsForUriRequestDecoder decoder = decoders.listRecordingsForUriRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    final int channelLength = decoder.channelLength();
                    final byte[] bytes = 0 == channelLength ? ArrayUtil.EMPTY_BYTE_ARRAY : new byte[channelLength];
                    decoder.getChannel(bytes, 0, channelLength);

                    controlSession.onListRecordingsForUri(
                        correlationId,
                        decoder.fromRecordingId(),
                        decoder.recordCount(),
                        decoder.streamId(),
                        bytes);
                }
                break;
            }

            case ListRecordingRequestDecoder.TEMPLATE_ID:
            {
                final ListRecordingRequestDecoder decoder = decoders.listRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onListRecording(correlationId, decoder.recordingId());
                }
                break;
            }

            case ExtendRecordingRequestDecoder.TEMPLATE_ID:
            {
                final ExtendRecordingRequestDecoder decoder = decoders.extendRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onExtendRecording(
                        correlationId,
                        decoder.recordingId(),
                        decoder.streamId(),
                        decoder.sourceLocation(),
                        false,
                        decoder.channel());
                }
                break;
            }

            case RecordingPositionRequestDecoder.TEMPLATE_ID:
            {
                final RecordingPositionRequestDecoder decoder = decoders.recordingPositionRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onGetRecordingPosition(correlationId, decoder.recordingId());
                }
                break;
            }

            case TruncateRecordingRequestDecoder.TEMPLATE_ID:
            {
                final TruncateRecordingRequestDecoder decoder = decoders.truncateRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onTruncateRecording(correlationId, decoder.recordingId(), decoder.position());
                }
                break;
            }

            case StopRecordingSubscriptionRequestDecoder.TEMPLATE_ID:
            {
                final StopRecordingSubscriptionRequestDecoder decoder = decoders.stopRecordingSubscriptionRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStopRecordingSubscription(correlationId, decoder.subscriptionId());
                }
                break;
            }

            case StopPositionRequestDecoder.TEMPLATE_ID:
            {
                final StopPositionRequestDecoder decoder = decoders.stopPositionRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onGetStopPosition(correlationId, decoder.recordingId());
                }
                break;
            }

            case FindLastMatchingRecordingRequestDecoder.TEMPLATE_ID:
            {
                final FindLastMatchingRecordingRequestDecoder decoder = decoders.findLastMatchingRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    final int channelLength = decoder.channelLength();
                    final byte[] bytes = 0 == channelLength ? ArrayUtil.EMPTY_BYTE_ARRAY : new byte[channelLength];

                    decoder.getChannel(bytes, 0, channelLength);
                    controlSession.onFindLastMatchingRecording(
                        correlationId,
                        decoder.minRecordingId(),
                        decoder.sessionId(),
                        decoder.streamId(),
                        bytes);
                }
                break;
            }

            case ListRecordingSubscriptionsRequestDecoder.TEMPLATE_ID:
            {
                final ListRecordingSubscriptionsRequestDecoder decoder = decoders.listRecordingSubscriptionsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onListRecordingSubscriptions(
                        correlationId,
                        decoder.pseudoIndex(),
                        decoder.subscriptionCount(),
                        decoder.applyStreamId() == BooleanType.TRUE,
                        decoder.streamId(),
                        decoder.channel());
                }
                break;
            }

            case BoundedReplayRequestDecoder.TEMPLATE_ID:
            {
                final BoundedReplayRequestDecoder decoder = decoders.boundedReplayRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                final int fileIoMaxLength = FILE_IO_MAX_LENGTH_VERSION <= headerDecoder.version() ?
                    decoder.fileIoMaxLength() : Aeron.NULL_VALUE;

                if (null != controlSession)
                {
                    controlSession.onStartBoundedReplay(
                        correlationId,
                        decoder.recordingId(),
                        decoder.position(),
                        decoder.length(),
                        decoder.limitCounterId(),
                        fileIoMaxLength,
                        decoder.replayStreamId(),
                        decoder.replayChannel());
                }
                break;
            }

            case StopAllReplaysRequestDecoder.TEMPLATE_ID:
            {
                final StopAllReplaysRequestDecoder decoder = decoders.stopAllReplaysRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStopAllReplays(correlationId, decoder.recordingId());
                }
                break;
            }

            case ReplicateRequestDecoder.TEMPLATE_ID:
            {
                final ReplicateRequestDecoder decoder = decoders.replicateRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onReplicate(
                        correlationId,
                        decoder.srcRecordingId(),
                        decoder.dstRecordingId(),
                        AeronArchive.NULL_POSITION,
                        Aeron.NULL_VALUE,
                        Aeron.NULL_VALUE,
                        decoder.srcControlStreamId(),
                        Aeron.NULL_VALUE,
                        decoder.srcControlChannel(),
                        decoder.liveDestination(),
                        ""
                    );
                }
                break;
            }

            case StopReplicationRequestDecoder.TEMPLATE_ID:
            {
                final StopReplicationRequestDecoder decoder = decoders.stopReplicationRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStopReplication(correlationId, decoder.replicationId());
                }
                break;
            }

            case StartPositionRequestDecoder.TEMPLATE_ID:
            {
                final StartPositionRequestDecoder decoder = decoders.startPositionRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onGetStartPosition(correlationId, decoder.recordingId());
                }
                break;
            }

            case DetachSegmentsRequestDecoder.TEMPLATE_ID:
            {
                final DetachSegmentsRequestDecoder decoder = decoders.detachSegmentsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onDetachSegments(correlationId, decoder.recordingId(), decoder.newStartPosition());
                }
                break;
            }

            case DeleteDetachedSegmentsRequestDecoder.TEMPLATE_ID:
            {
                final DeleteDetachedSegmentsRequestDecoder decoder = decoders.deleteDetachedSegmentsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onDeleteDetachedSegments(correlationId, decoder.recordingId());
                }
                break;
            }

            case PurgeSegmentsRequestDecoder.TEMPLATE_ID:
            {
                final PurgeSegmentsRequestDecoder decoder = decoders.purgeSegmentsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onPurgeSegments(correlationId, decoder.recordingId(), decoder.newStartPosition());
                }
                break;
            }

            case AttachSegmentsRequestDecoder.TEMPLATE_ID:
            {
                final AttachSegmentsRequestDecoder decoder = decoders.attachSegmentsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onAttachSegments(correlationId, decoder.recordingId());
                }
                break;
            }

            case MigrateSegmentsRequestDecoder.TEMPLATE_ID:
            {
                final MigrateSegmentsRequestDecoder decoder = decoders.migrateSegmentsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onMigrateSegments(correlationId, decoder.srcRecordingId(), decoder.dstRecordingId());
                }
                break;
            }

            case AuthConnectRequestDecoder.TEMPLATE_ID:
            {
                final AuthConnectRequestDecoder decoder = decoders.authConnectRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final String responseChannel = decoder.responseChannel();
                final int credentialsLength = decoder.encodedCredentialsLength();
                final byte[] credentials;

                if (credentialsLength > 0)
                {
                    credentials = new byte[credentialsLength];
                    decoder.getEncodedCredentials(credentials, 0, credentialsLength);
                }
                else
                {
                    credentials = ArrayUtil.EMPTY_BYTE_ARRAY;
                }

                final ControlSession session = conductor.newControlSession(
                    decoder.correlationId(),
                    decoder.responseStreamId(),
                    decoder.version(),
                    responseChannel,
                    credentials,
                    this);
                controlSessionByIdMap.put(session.sessionId(), session);
                break;
            }

            case ChallengeResponseDecoder.TEMPLATE_ID:
            {
                final ChallengeResponseDecoder decoder = decoders.challengeResponse;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final ControlSession session = controlSessionByIdMap.get(controlSessionId);
                if (null != session)
                {
                    final int credentialsLength = decoder.encodedCredentialsLength();
                    final byte[] credentials;

                    if (credentialsLength > 0)
                    {
                        credentials = new byte[credentialsLength];
                        decoder.getEncodedCredentials(credentials, 0, credentialsLength);
                    }
                    else
                    {
                        credentials = ArrayUtil.EMPTY_BYTE_ARRAY;
                    }

                    session.onChallengeResponse(decoder.correlationId(), credentials);
                }
                break;
            }

            case KeepAliveRequestDecoder.TEMPLATE_ID:
            {
                final KeepAliveRequestDecoder decoder = decoders.keepAliveRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onKeepAlive(correlationId);
                }
                break;
            }

            case TaggedReplicateRequestDecoder.TEMPLATE_ID:
            {
                final TaggedReplicateRequestDecoder decoder = decoders.taggedReplicateRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onReplicate(
                        correlationId,
                        decoder.srcRecordingId(),
                        decoder.dstRecordingId(),
                        AeronArchive.NULL_POSITION,
                        decoder.channelTagId(),
                        decoder.subscriptionTagId(),
                        decoder.srcControlStreamId(),
                        Aeron.NULL_VALUE,
                        decoder.srcControlChannel(),
                        decoder.liveDestination(),
                        ""
                    );
                }
                break;
            }

            case StartRecordingRequest2Decoder.TEMPLATE_ID:
            {
                final StartRecordingRequest2Decoder decoder = decoders.startRecordingRequest2;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStartRecording(
                        correlationId,
                        decoder.streamId(),
                        decoder.sourceLocation(),
                        decoder.autoStop() == BooleanType.TRUE,
                        decoder.channel());
                }
                break;
            }

            case ExtendRecordingRequest2Decoder.TEMPLATE_ID:
            {
                final ExtendRecordingRequest2Decoder decoder = decoders.extendRecordingRequest2;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onExtendRecording(
                        correlationId,
                        decoder.recordingId(),
                        decoder.streamId(),
                        decoder.sourceLocation(),
                        decoder.autoStop() == BooleanType.TRUE,
                        decoder.channel());
                }
                break;
            }

            case StopRecordingByIdentityRequestDecoder.TEMPLATE_ID:
            {
                final StopRecordingByIdentityRequestDecoder decoder = decoders.stopRecordingByIdentityRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStopRecordingByIdentity(correlationId, decoder.recordingId());
                }
                break;
            }

            case ReplicateRequest2Decoder.TEMPLATE_ID:
            {
                final ReplicateRequest2Decoder decoder = decoders.replicateRequest2;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                final int fileIoMaxLength = FILE_IO_MAX_LENGTH_VERSION <= headerDecoder.version() ?
                    decoder.fileIoMaxLength() : Aeron.NULL_VALUE;

                if (null != controlSession)
                {
                    controlSession.onReplicate(
                        correlationId,
                        decoder.srcRecordingId(),
                        decoder.dstRecordingId(),
                        decoder.stopPosition(),
                        decoder.channelTagId(),
                        decoder.subscriptionTagId(),
                        decoder.srcControlStreamId(),
                        fileIoMaxLength,
                        decoder.srcControlChannel(),
                        decoder.liveDestination(),
                        decoder.replicationChannel()
                    );
                }
                break;
            }

            case PurgeRecordingRequestDecoder.TEMPLATE_ID:
            {
                final PurgeRecordingRequestDecoder decoder = decoders.purgeRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onPurgeRecording(correlationId, decoder.recordingId());
                }
                break;
            }
        }
    }

    void removeControlSession(final long sessionId)
    {
        controlSessionByIdMap.remove(sessionId);
    }

    private ControlSession getControlSession(
        final long correlationId, final long controlSessionId, final int templateId)
    {
        final ControlSession controlSession = controlSessionByIdMap.get(controlSessionId);
        if (null != controlSession)
        {
            final byte[] principal = controlSession.encodedPrincipal();
            if (!authorisationService.isAuthorised(MessageHeaderDecoder.SCHEMA_ID, templateId, null, principal))
            {
                conductor.logWarning("unauthorised archive action=" + templateId +
                    " controlSessionId=" + controlSessionId + " source=" + image.sourceIdentity());

                controlSession.attemptErrorResponse(
                    correlationId,
                    ArchiveException.UNAUTHORISED_ACTION,
                    "unauthorised action",
                    conductor.controlResponseProxy());

                return null;
            }
        }
        else
        {
            conductor.logWarning("control request for unknown controlSessionId=" + controlSessionId +
                " source=" + image.sourceIdentity());
        }

        return controlSession;
    }
}
