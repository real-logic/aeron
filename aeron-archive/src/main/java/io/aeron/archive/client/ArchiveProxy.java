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
package io.aeron.archive.client;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.codecs.*;
import io.aeron.security.CredentialsSupplier;
import io.aeron.security.NullCredentialsSupplier;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.*;

import static io.aeron.archive.client.AeronArchive.Configuration.MESSAGE_TIMEOUT_DEFAULT_NS;

/**
 * Proxy class for encapsulating encoding and sending of control protocol messages to an archive.
 */
public class ArchiveProxy
{
    /**
     * Default number of retry attempts to be made when offering requests.
     */
    public static final int DEFAULT_RETRY_ATTEMPTS = 3;

    private final long connectTimeoutNs;
    private final int retryAttempts;
    private final IdleStrategy retryIdleStrategy;
    private final NanoClock nanoClock;
    private final CredentialsSupplier credentialsSupplier;

    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
    private final Publication publication;
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private StartRecordingRequestEncoder startRecordingRequestEncoder;
    private StopRecordingRequestEncoder stopRecordingRequestEncoder;
    private StopRecordingSubscriptionRequestEncoder stopRecordingSubscriptionRequestEncoder;
    private ReplayRequestEncoder replayRequestEncoder;
    private StopReplayRequestEncoder stopReplayRequestEncoder;
    private ListRecordingsRequestEncoder listRecordingsRequestEncoder;
    private ListRecordingsForUriRequestEncoder listRecordingsForUriRequestEncoder;
    private ListRecordingRequestEncoder listRecordingRequestEncoder;
    private ExtendRecordingRequestEncoder extendRecordingRequestEncoder;
    private RecordingPositionRequestEncoder recordingPositionRequestEncoder;
    private TruncateRecordingRequestEncoder truncateRecordingRequestEncoder;
    private StopPositionRequestEncoder stopPositionRequestEncoder;
    private FindLastMatchingRecordingRequestEncoder findLastMatchingRecordingRequestEncoder;
    private ListRecordingSubscriptionsRequestEncoder listRecordingSubscriptionsRequestEncoder;
    private BoundedReplayRequestEncoder boundedReplayRequestEncoder;
    private StopAllReplaysRequestEncoder stopAllReplaysRequestEncoder;
    private ReplicateRequestEncoder replicateRequestEncoder;
    private StopReplicationRequestEncoder stopReplicationRequestEncoder;
    private StartPositionRequestEncoder startPositionRequestEncoder;
    private DetachSegmentsRequestEncoder detachSegmentsRequestEncoder;
    private DeleteDetachedSegmentsRequestEncoder deleteDetachedSegmentsRequestEncoder;
    private PurgeSegmentsRequestEncoder purgeSegmentsRequestEncoder;
    private AttachSegmentsRequestEncoder attachSegmentsRequestEncoder;
    private MigrateSegmentsRequestEncoder migrateSegmentsRequestEncoder;
    private TaggedReplicateRequestEncoder taggedReplicateRequestEncoder;

    /**
     * Create a proxy with a {@link Publication} for sending control message requests.
     * <p>
     * This provides a default {@link IdleStrategy} of a {@link YieldingIdleStrategy} when offers are back pressured
     * with a defaults of {@link AeronArchive.Configuration#MESSAGE_TIMEOUT_DEFAULT_NS} and
     * {@link #DEFAULT_RETRY_ATTEMPTS}.
     *
     * @param publication publication for sending control messages to an archive.
     */
    public ArchiveProxy(final Publication publication)
    {
        this(
            publication,
            YieldingIdleStrategy.INSTANCE,
            SystemNanoClock.INSTANCE,
            MESSAGE_TIMEOUT_DEFAULT_NS,
            DEFAULT_RETRY_ATTEMPTS,
            new NullCredentialsSupplier());
    }

    /**
     * Create a proxy with a {@link Publication} for sending control message requests.
     *
     * @param publication         publication for sending control messages to an archive.
     * @param retryIdleStrategy   for what should happen between retry attempts at offering messages.
     * @param nanoClock           to be used for calculating checking deadlines.
     * @param connectTimeoutNs    for for connection requests.
     * @param retryAttempts       for offering control messages before giving up.
     * @param credentialsSupplier for the AuthConnectRequest
     */
    public ArchiveProxy(
        final Publication publication,
        final IdleStrategy retryIdleStrategy,
        final NanoClock nanoClock,
        final long connectTimeoutNs,
        final int retryAttempts,
        final CredentialsSupplier credentialsSupplier)
    {
        this.publication = publication;
        this.retryIdleStrategy = retryIdleStrategy;
        this.nanoClock = nanoClock;
        this.connectTimeoutNs = connectTimeoutNs;
        this.retryAttempts = retryAttempts;
        this.credentialsSupplier = credentialsSupplier;
    }

    /**
     * Get the {@link Publication} used for sending control messages.
     *
     * @return the {@link Publication} used for sending control messages.
     */
    public Publication publication()
    {
        return publication;
    }

    /**
     * Connect to an archive on its control interface providing the response stream details.
     *
     * @param responseChannel  for the control message responses.
     * @param responseStreamId for the control message responses.
     * @param correlationId    for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean connect(final String responseChannel, final int responseStreamId, final long correlationId)
    {
        final byte[] encodedCredentials = credentialsSupplier.encodedCredentials();

        final AuthConnectRequestEncoder connectRequestEncoder = new AuthConnectRequestEncoder();
        connectRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .responseStreamId(responseStreamId)
            .version(AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION)
            .responseChannel(responseChannel)
            .putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);

        return offerWithTimeout(connectRequestEncoder.encodedLength(), null);
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
    public boolean tryConnect(final String responseChannel, final int responseStreamId, final long correlationId)
    {
        final byte[] encodedCredentials = credentialsSupplier.encodedCredentials();

        final AuthConnectRequestEncoder connectRequestEncoder = new AuthConnectRequestEncoder();
        connectRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .responseStreamId(responseStreamId)
            .version(AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION)
            .responseChannel(responseChannel)
            .putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + connectRequestEncoder.encodedLength();

        return publication.offer(buffer, 0, length) > 0;
    }

    /**
     * Connect to an archive on its control interface providing the response stream details.
     *
     * @param responseChannel    for the control message responses.
     * @param responseStreamId   for the control message responses.
     * @param correlationId      for this request.
     * @param aeronClientInvoker for aeron client conductor thread.
     * @return true if successfully offered otherwise false.
     */
    public boolean connect(
        final String responseChannel,
        final int responseStreamId,
        final long correlationId,
        final AgentInvoker aeronClientInvoker)
    {
        final byte[] encodedCredentials = credentialsSupplier.encodedCredentials();

        final AuthConnectRequestEncoder connectRequestEncoder = new AuthConnectRequestEncoder();
        connectRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .responseStreamId(responseStreamId)
            .version(AeronArchive.Configuration.PROTOCOL_SEMANTIC_VERSION)
            .responseChannel(responseChannel)
            .putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);

        return offerWithTimeout(connectRequestEncoder.encodedLength(), aeronClientInvoker);
    }

    /**
     * Keep this archive session alive by notifying the archive.
     *
     * @param controlSessionId with the archive.
     * @param correlationId    for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean keepAlive(final long controlSessionId, final long correlationId)
    {
        final KeepAliveRequestEncoder keepAliveRequestEncoder = new KeepAliveRequestEncoder();
        keepAliveRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId);

        return offer(keepAliveRequestEncoder.encodedLength());
    }

    /**
     * Close this control session with the archive.
     *
     * @param controlSessionId with the archive.
     * @return true if successfully offered otherwise false.
     */
    public boolean closeSession(final long controlSessionId)
    {
        final CloseSessionRequestEncoder closeSessionRequestEncoder = new CloseSessionRequestEncoder();
        closeSessionRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId);

        return offer(closeSessionRequestEncoder.encodedLength());
    }

    /**
     * Try send a ChallengeResponse to an archive on its control interface providing the credentials. Only one
     * attempt will be made to offer the request.
     *
     * @param encodedCredentials to send.
     * @param correlationId      for this response.
     * @param controlSessionId   for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean tryChallengeResponse(
        final byte[] encodedCredentials, final long correlationId, final long controlSessionId)
    {
        final ChallengeResponseEncoder challengeResponseEncoder = new ChallengeResponseEncoder();
        challengeResponseEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + challengeResponseEncoder.encodedLength();

        return publication.offer(buffer, 0, length) > 0;
    }

    /**
     * Start recording streams for a given channel and stream id pairing.
     *
     * @param channel          to be recorded.
     * @param streamId         to be recorded.
     * @param sourceLocation   of the publication to be recorded.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean startRecording(
        final String channel,
        final int streamId,
        final SourceLocation sourceLocation,
        final long correlationId,
        final long controlSessionId)
    {
        if (null == startRecordingRequestEncoder)
        {
            startRecordingRequestEncoder = new StartRecordingRequestEncoder();
        }

        startRecordingRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .streamId(streamId)
            .sourceLocation(sourceLocation)
            .channel(channel);

        return offer(startRecordingRequestEncoder.encodedLength());
    }

    /**
     * Stop an active recording.
     *
     * @param channel          to be stopped.
     * @param streamId         to be stopped.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean stopRecording(
        final String channel, final int streamId, final long correlationId, final long controlSessionId)
    {
        if (null == stopRecordingRequestEncoder)
        {
            stopRecordingRequestEncoder = new StopRecordingRequestEncoder();
        }

        stopRecordingRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .streamId(streamId)
            .channel(channel);

        return offer(stopRecordingRequestEncoder.encodedLength());
    }

    /**
     * Stop an active recording by the {@link Subscription#registrationId()} it was registered with.
     *
     * @param subscriptionId   that identifies the subscription in the archive doing the recording.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean stopRecording(final long subscriptionId, final long correlationId, final long controlSessionId)
    {
        if (null == stopRecordingSubscriptionRequestEncoder)
        {
            stopRecordingSubscriptionRequestEncoder = new StopRecordingSubscriptionRequestEncoder();
        }

        stopRecordingSubscriptionRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .subscriptionId(subscriptionId);

        return offer(stopRecordingSubscriptionRequestEncoder.encodedLength());
    }

    /**
     * Replay a recording from a given position.
     *
     * @param recordingId      to be replayed.
     * @param position         from which the replay should be started.
     * @param length           of the stream to be replayed. Use {@link Long#MAX_VALUE} to follow a live stream.
     * @param replayChannel    to which the replay should be sent.
     * @param replayStreamId   to which the replay should be sent.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean replay(
        final long recordingId,
        final long position,
        final long length,
        final String replayChannel,
        final int replayStreamId,
        final long correlationId,
        final long controlSessionId)
    {
        if (null == replayRequestEncoder)
        {
            replayRequestEncoder = new ReplayRequestEncoder();
        }

        replayRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId)
            .position(position)
            .length(length)
            .replayStreamId(replayStreamId)
            .replayChannel(replayChannel);

        return offer(replayRequestEncoder.encodedLength());
    }

    /**
     * Replay a recording from a given position bounded by a position counter.
     *
     * @param recordingId      to be replayed.
     * @param position         from which the replay should be started.
     * @param length           of the stream to be replayed. Use {@link Long#MAX_VALUE} to follow a live stream.
     * @param limitCounterId   to use as the replay bound.
     * @param replayChannel    to which the replay should be sent.
     * @param replayStreamId   to which the replay should be sent.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean boundedReplay(
        final long recordingId,
        final long position,
        final long length,
        final int limitCounterId,
        final String replayChannel,
        final int replayStreamId,
        final long correlationId,
        final long controlSessionId)
    {
        if (null == boundedReplayRequestEncoder)
        {
            boundedReplayRequestEncoder = new BoundedReplayRequestEncoder();
        }

        boundedReplayRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId)
            .position(position)
            .length(length)
            .limitCounterId(limitCounterId)
            .replayStreamId(replayStreamId)
            .replayChannel(replayChannel);

        return offer(boundedReplayRequestEncoder.encodedLength());
    }

    /**
     * Stop an existing replay session.
     *
     * @param replaySessionId  that should be stopped.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean stopReplay(final long replaySessionId, final long correlationId, final long controlSessionId)
    {
        if (null == stopReplayRequestEncoder)
        {
            stopReplayRequestEncoder = new StopReplayRequestEncoder();
        }

        stopReplayRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .replaySessionId(replaySessionId);

        return offer(stopReplayRequestEncoder.encodedLength());
    }

    /**
     * Stop any existing replay sessions for recording Id or all replay sessions regardless of recording Id.
     *
     * @param recordingId      that should be stopped.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean stopAllReplays(final long recordingId, final long correlationId, final long controlSessionId)
    {
        if (null == stopAllReplaysRequestEncoder)
        {
            stopAllReplaysRequestEncoder = new StopAllReplaysRequestEncoder();
        }

        stopAllReplaysRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId);

        return offer(stopAllReplaysRequestEncoder.encodedLength());
    }

    /**
     * List a range of recording descriptors.
     *
     * @param fromRecordingId  at which to begin listing.
     * @param recordCount      for the number of descriptors to be listed.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean listRecordings(
        final long fromRecordingId, final int recordCount, final long correlationId, final long controlSessionId)
    {
        if (null == listRecordingsRequestEncoder)
        {
            listRecordingsRequestEncoder = new ListRecordingsRequestEncoder();
        }

        listRecordingsRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .fromRecordingId(fromRecordingId)
            .recordCount(recordCount);

        return offer(listRecordingsRequestEncoder.encodedLength());
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
     * @return true if successfully offered otherwise false.
     */
    public boolean listRecordingsForUri(
        final long fromRecordingId,
        final int recordCount,
        final String channelFragment,
        final int streamId,
        final long correlationId,
        final long controlSessionId)
    {
        if (null == listRecordingsForUriRequestEncoder)
        {
            listRecordingsForUriRequestEncoder = new ListRecordingsForUriRequestEncoder();
        }

        listRecordingsForUriRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .fromRecordingId(fromRecordingId)
            .recordCount(recordCount)
            .streamId(streamId)
            .channel(channelFragment);

        return offer(listRecordingsForUriRequestEncoder.encodedLength());
    }

    /**
     * List a recording descriptor for a given recording id.
     *
     * @param recordingId      at which to begin listing.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean listRecording(final long recordingId, final long correlationId, final long controlSessionId)
    {
        if (null == listRecordingRequestEncoder)
        {
            listRecordingRequestEncoder = new ListRecordingRequestEncoder();
        }

        listRecordingRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId);

        return offer(listRecordingRequestEncoder.encodedLength());
    }

    /**
     * Extend an existing, non-active, recorded stream for a the same channel and stream id.
     * <p>
     * The channel must be configured for the initial position from which it will be extended. This can be done
     * with {@link ChannelUriStringBuilder#initialPosition(long, int, int)}. The details required to initialise can
     * be found by calling {@link #listRecording(long, long, long)}.
     *
     * @param channel          to be recorded.
     * @param streamId         to be recorded.
     * @param sourceLocation   of the publication to be recorded.
     * @param recordingId      to be extended.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean extendRecording(
        final String channel,
        final int streamId,
        final SourceLocation sourceLocation,
        final long recordingId,
        final long correlationId,
        final long controlSessionId)
    {
        if (null == extendRecordingRequestEncoder)
        {
            extendRecordingRequestEncoder = new ExtendRecordingRequestEncoder();
        }

        extendRecordingRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId)
            .streamId(streamId)
            .sourceLocation(sourceLocation)
            .channel(channel);

        return offer(extendRecordingRequestEncoder.encodedLength());
    }

    /**
     * Get the recorded position of an active recording.
     *
     * @param recordingId      of the active recording that the position is being requested for.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean getRecordingPosition(final long recordingId, final long correlationId, final long controlSessionId)
    {
        if (null == recordingPositionRequestEncoder)
        {
            recordingPositionRequestEncoder = new RecordingPositionRequestEncoder();
        }

        recordingPositionRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId);

        return offer(recordingPositionRequestEncoder.encodedLength());
    }

    /**
     * Truncate a stopped recording to a given position that is less than the stopped position. The provided position
     * must be on a fragment boundary. Truncating a recording to the start position effectively deletes the recording.
     *
     * @param recordingId      of the stopped recording to be truncated.
     * @param position         to which the recording will be truncated.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean truncateRecording(
        final long recordingId, final long position, final long correlationId, final long controlSessionId)
    {
        if (null == truncateRecordingRequestEncoder)
        {
            truncateRecordingRequestEncoder = new TruncateRecordingRequestEncoder();
        }

        truncateRecordingRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId)
            .position(position);

        return offer(truncateRecordingRequestEncoder.encodedLength());
    }

    /**
     * Get the start position of a recording.
     *
     * @param recordingId      of the recording that the position is being requested for.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean getStartPosition(final long recordingId, final long correlationId, final long controlSessionId)
    {
        if (null == startPositionRequestEncoder)
        {
            startPositionRequestEncoder = new StartPositionRequestEncoder();
        }

        startPositionRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId);

        return offer(startPositionRequestEncoder.encodedLength());
    }

    /**
     * Get the stop position of a recording.
     *
     * @param recordingId      of the recording that the stop position is being requested for.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean getStopPosition(final long recordingId, final long correlationId, final long controlSessionId)
    {
        if (null == stopPositionRequestEncoder)
        {
            stopPositionRequestEncoder = new StopPositionRequestEncoder();
        }

        stopPositionRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId);

        return offer(stopPositionRequestEncoder.encodedLength());
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
     * @return true if successfully offered otherwise false.
     */
    public boolean findLastMatchingRecording(
        final long minRecordingId,
        final String channelFragment,
        final int streamId,
        final int sessionId,
        final long correlationId,
        final long controlSessionId)
    {
        if (null == findLastMatchingRecordingRequestEncoder)
        {
            findLastMatchingRecordingRequestEncoder = new FindLastMatchingRecordingRequestEncoder();
        }

        findLastMatchingRecordingRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .minRecordingId(minRecordingId)
            .sessionId(sessionId)
            .streamId(streamId)
            .channel(channelFragment);

        return offer(findLastMatchingRecordingRequestEncoder.encodedLength());
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
     * @return true if successfully offered otherwise false.
     */
    public boolean listRecordingSubscriptions(
        final int pseudoIndex,
        final int subscriptionCount,
        final String channelFragment,
        final int streamId,
        final boolean applyStreamId,
        final long correlationId,
        final long controlSessionId)
    {
        if (null == listRecordingSubscriptionsRequestEncoder)
        {
            listRecordingSubscriptionsRequestEncoder = new ListRecordingSubscriptionsRequestEncoder();
        }

        listRecordingSubscriptionsRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .pseudoIndex(pseudoIndex)
            .subscriptionCount(subscriptionCount)
            .applyStreamId(applyStreamId ? BooleanType.TRUE : BooleanType.FALSE)
            .streamId(streamId)
            .channel(channelFragment);

        return offer(listRecordingSubscriptionsRequestEncoder.encodedLength());
    }

    /**
     * Replicate a recording from a source archive to a destination which can be considered a backup for a primary
     * archive. The source recording will be replayed via the provided replay channel and use the original stream id.
     * If the destination recording id is {@link io.aeron.Aeron#NULL_VALUE} then a new destination recording is created,
     * otherwise the provided destination recording id will be extended. The details of the source recording
     * descriptor will be replicated.
     * <p>
     * For a source recording that is still active the replay can merge with the live stream and then follow it
     * directly and no longer require the replay from the source. This would require a multicast live destination.
     * <p>
     * Errors will be reported asynchronously and can be checked for with {@link AeronArchive#pollForErrorResponse()}
     * or {@link AeronArchive#checkForErrorResponse()}.
     *
     * @param srcRecordingId     recording id which must exist in the source archive.
     * @param dstRecordingId     recording to extend in the destination, otherwise {@link io.aeron.Aeron#NULL_VALUE}.
     * @param srcControlChannel  remote control channel for the source archive to instruct the replay on.
     * @param srcControlStreamId remote control stream id for the source archive to instruct the replay on.
     * @param liveDestination    destination for the live stream if merge is required. Empty or null for no merge.
     * @param correlationId      for this request.
     * @param controlSessionId   for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean replicate(
        final long srcRecordingId,
        final long dstRecordingId,
        final int srcControlStreamId,
        final String srcControlChannel,
        final String liveDestination,
        final long correlationId,
        final long controlSessionId)
    {
        if (null == replicateRequestEncoder)
        {
            replicateRequestEncoder = new ReplicateRequestEncoder();
        }

        replicateRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .srcRecordingId(srcRecordingId)
            .dstRecordingId(dstRecordingId)
            .srcControlStreamId(srcControlStreamId)
            .srcControlChannel(srcControlChannel)
            .liveDestination(liveDestination);

        return offer(replicateRequestEncoder.encodedLength());
    }

    /**
     * Replicate a recording from a source archive to a destination which can be considered a backup for a primary
     * archive. The source recording will be replayed via the provided replay channel and use the original stream id.
     * If the destination recording id is {@link io.aeron.Aeron#NULL_VALUE} then a new destination recording is created,
     * otherwise the provided destination recording id will be extended. The details of the source recording
     * descriptor will be replicated. The subscription used in the archive will be tagged with the provided tags.
     * <p>
     * For a source recording that is still active the replay can merge with the live stream and then follow it
     * directly and no longer require the replay from the source. This would require a multicast live destination.
     * <p>
     * Errors will be reported asynchronously and can be checked for with {@link AeronArchive#pollForErrorResponse()}
     * or {@link AeronArchive#checkForErrorResponse()}.
     *
     * @param srcRecordingId     recording id which must exist in the source archive.
     * @param dstRecordingId     recording to extend in the destination, otherwise {@link io.aeron.Aeron#NULL_VALUE}.
     * @param channelTagId       used to tag the replication subscription.
     * @param subscriptionTagId  used to tag the replication subscription.
     * @param srcControlChannel  remote control channel for the source archive to instruct the replay on.
     * @param srcControlStreamId remote control stream id for the source archive to instruct the replay on.
     * @param liveDestination    destination for the live stream if merge is required. Empty or null for no merge.
     * @param correlationId      for this request.
     * @param controlSessionId   for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean taggedReplicate(
        final long srcRecordingId,
        final long dstRecordingId,
        final long channelTagId,
        final long subscriptionTagId,
        final int srcControlStreamId,
        final String srcControlChannel,
        final String liveDestination,
        final long correlationId,
        final long controlSessionId)
    {
        if (null == taggedReplicateRequestEncoder)
        {
            taggedReplicateRequestEncoder = new TaggedReplicateRequestEncoder();
        }

        taggedReplicateRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .srcRecordingId(srcRecordingId)
            .dstRecordingId(dstRecordingId)
            .channelTagId(channelTagId)
            .subscriptionTagId(subscriptionTagId)
            .srcControlStreamId(srcControlStreamId)
            .srcControlChannel(srcControlChannel)
            .liveDestination(liveDestination);

        return offer(taggedReplicateRequestEncoder.encodedLength());
    }

    /**
     * Stop an active replication by the registration id it was registered with.
     *
     * @param replicationId    that identifies the session in the archive doing the replication.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean stopReplication(final long replicationId, final long correlationId, final long controlSessionId)
    {
        if (null == stopReplicationRequestEncoder)
        {
            stopReplicationRequestEncoder = new StopReplicationRequestEncoder();
        }

        stopReplicationRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .replicationId(replicationId);

        return offer(stopReplicationRequestEncoder.encodedLength());
    }

    /**
     * Detach segments from the beginning of a recording up to the provided new start position.
     * <p>
     * The new start position must be first byte position of a segment after the existing start position.
     * <p>
     * It is not possible to detach segments which are active for recording or being replayed.
     *
     * @param recordingId      to which the operation applies.
     * @param newStartPosition for the recording after the segments are detached.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     * @see AeronArchive#segmentFileBasePosition(long, long, int, int)
     */
    public boolean detachSegments(
        final long recordingId, final long newStartPosition, final long correlationId, final long controlSessionId)
    {
        if (null == detachSegmentsRequestEncoder)
        {
            detachSegmentsRequestEncoder = new DetachSegmentsRequestEncoder();
        }

        detachSegmentsRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId)
            .newStartPosition(newStartPosition);

        return offer(detachSegmentsRequestEncoder.encodedLength());
    }

    /**
     * Delete segments which have been previously detached from a recording.
     *
     * @param recordingId      to which the operation applies.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     * @see #detachSegments(long, long, long, long)
     */
    public boolean deleteDetachedSegments(final long recordingId, final long correlationId, final long controlSessionId)
    {
        if (null == deleteDetachedSegmentsRequestEncoder)
        {
            deleteDetachedSegmentsRequestEncoder = new DeleteDetachedSegmentsRequestEncoder();
        }

        deleteDetachedSegmentsRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId);

        return offer(deleteDetachedSegmentsRequestEncoder.encodedLength());
    }

    /**
     * Purge (detach and delete) segments from the beginning of a recording up to the provided new start position.
     * <p>
     * The new start position must be first byte position of a segment after the existing start position.
     * <p>
     * It is not possible to purge segments which are active for recording or being replayed.
     *
     * @param recordingId      to which the operation applies.
     * @param newStartPosition for the recording after the segments are detached.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     * @see #detachSegments(long, long, long, long)
     * @see #deleteDetachedSegments(long, long, long)
     * @see AeronArchive#segmentFileBasePosition(long, long, int, int)
     */
    public boolean purgeSegments(
        final long recordingId, final long newStartPosition, final long correlationId, final long controlSessionId)
    {
        if (null == purgeSegmentsRequestEncoder)
        {
            purgeSegmentsRequestEncoder = new PurgeSegmentsRequestEncoder();
        }

        purgeSegmentsRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId)
            .newStartPosition(newStartPosition);

        return offer(purgeSegmentsRequestEncoder.encodedLength());
    }

    /**
     * Attach segments to the beginning of a recording to restore history that was previously detached.
     * <p>
     * Segment files must match the existing recording and join exactly to the start position of the recording
     * they are being attached to.
     *
     * @param recordingId      to which the operation applies.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     * @see #detachSegments(long, long, long, long)
     */
    public boolean attachSegments(final long recordingId, final long correlationId, final long controlSessionId)
    {
        if (null == attachSegmentsRequestEncoder)
        {
            attachSegmentsRequestEncoder = new AttachSegmentsRequestEncoder();
        }

        attachSegmentsRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId);

        return offer(attachSegmentsRequestEncoder.encodedLength());
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
     * @return true if successfully offered otherwise false.
     */
    public boolean migrateSegments(
        final long srcRecordingId, final long dstRecordingId, final long correlationId, final long controlSessionId)
    {
        if (null == migrateSegmentsRequestEncoder)
        {
            migrateSegmentsRequestEncoder = new MigrateSegmentsRequestEncoder();
        }

        migrateSegmentsRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .srcRecordingId(srcRecordingId)
            .dstRecordingId(dstRecordingId);

        return offer(migrateSegmentsRequestEncoder.encodedLength());
    }

    private boolean offer(final int length)
    {
        retryIdleStrategy.reset();

        int attempts = retryAttempts;
        while (true)
        {
            final long result = publication.offer(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + length);
            if (result > 0)
            {
                return true;
            }

            if (result == Publication.CLOSED)
            {
                throw new ArchiveException("connection to the archive has been closed");
            }

            if (result == Publication.NOT_CONNECTED)
            {
                throw new ArchiveException("connection to the archive is no longer available");
            }

            if (result == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new ArchiveException("offer failed due to max position being reached");
            }

            if (--attempts <= 0)
            {
                return false;
            }

            retryIdleStrategy.idle();
        }
    }

    private boolean offerWithTimeout(final int length, final AgentInvoker aeronClientInvoker)
    {
        retryIdleStrategy.reset();

        final long deadlineNs = nanoClock.nanoTime() + connectTimeoutNs;
        while (true)
        {
            final long result = publication.offer(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + length);
            if (result > 0)
            {
                return true;
            }

            if (result == Publication.CLOSED)
            {
                throw new ArchiveException("connection to the archive has been closed");
            }

            if (result == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new ArchiveException("offer failed due to max position being reached");
            }

            if (deadlineNs - nanoClock.nanoTime() < 0)
            {
                return false;
            }

            if (null != aeronClientInvoker)
            {
                aeronClientInvoker.invoke();
            }

            retryIdleStrategy.idle();
        }
    }
}
