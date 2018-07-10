/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.archive.client;

import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.codecs.*;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.*;

import static io.aeron.archive.client.AeronArchive.Configuration.MESSAGE_TIMEOUT_DEFAULT_NS;

/**
 * Proxy class for encapsulating encoding and sending of control protocol messages to an archive.
 */
public class ArchiveProxy
{
    /**
     * Default number of retry attempts to be made at offering requests.
     */
    public static final int DEFAULT_RETRY_ATTEMPTS = 3;

    private final long connectTimeoutNs;
    private final int retryAttempts;
    private final IdleStrategy retryIdleStrategy;
    private final NanoClock nanoClock;

    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(124);
    private final Publication publication;
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final ConnectRequestEncoder connectRequestEncoder = new ConnectRequestEncoder();
    private final CloseSessionRequestEncoder closeSessionRequestEncoder = new CloseSessionRequestEncoder();
    private final StartRecordingRequestEncoder startRecordingRequestEncoder = new StartRecordingRequestEncoder();
    private final ReplayRequestEncoder replayRequestEncoder = new ReplayRequestEncoder();
    private final StopReplayRequestEncoder stopReplayRequestEncoder = new StopReplayRequestEncoder();
    private final StopRecordingRequestEncoder stopRecordingRequestEncoder = new StopRecordingRequestEncoder();
    private final StopRecordingSubscriptionRequestEncoder stopRecordingSubscriptionRequestEncoder =
        new StopRecordingSubscriptionRequestEncoder();
    private final ListRecordingsRequestEncoder listRecordingsRequestEncoder = new ListRecordingsRequestEncoder();
    private final ListRecordingsForUriRequestEncoder listRecordingsForUriRequestEncoder =
        new ListRecordingsForUriRequestEncoder();
    private final ListRecordingRequestEncoder listRecordingRequestEncoder = new ListRecordingRequestEncoder();
    private final ExtendRecordingRequestEncoder extendRecordingRequestEncoder = new ExtendRecordingRequestEncoder();
    private final RecordingPositionRequestEncoder recordingPositionRequestEncoder =
        new RecordingPositionRequestEncoder();
    private final TruncateRecordingRequestEncoder truncateRecordingRequestEncoder =
        new TruncateRecordingRequestEncoder();

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
            new YieldingIdleStrategy(),
            new SystemNanoClock(),
            MESSAGE_TIMEOUT_DEFAULT_NS,
            DEFAULT_RETRY_ATTEMPTS);
    }

    /**
     * Create a proxy with a {@link Publication} for sending control message requests.
     *
     * @param publication       publication for sending control messages to an archive.
     * @param retryIdleStrategy for what should happen between retry attempts at offering messages.
     * @param nanoClock         to be used for calculating checking deadlines.
     * @param connectTimeoutNs  for for connection requests.
     * @param retryAttempts     for offering control messages before giving up.
     */
    public ArchiveProxy(
        final Publication publication,
        final IdleStrategy retryIdleStrategy,
        final NanoClock nanoClock,
        final long connectTimeoutNs,
        final int retryAttempts)
    {
        this.publication = publication;
        this.retryIdleStrategy = retryIdleStrategy;
        this.nanoClock = nanoClock;
        this.connectTimeoutNs = connectTimeoutNs;
        this.retryAttempts = retryAttempts;
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
        connectRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .responseStreamId(responseStreamId)
            .responseChannel(responseChannel);

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
        connectRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .responseStreamId(responseStreamId)
            .responseChannel(responseChannel);

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
        connectRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .responseStreamId(responseStreamId)
            .responseChannel(responseChannel);

        return offerWithTimeout(connectRequestEncoder.encodedLength(), aeronClientInvoker);
    }

    /**
     * Close this control session with the archive.
     *
     * @param controlSessionId with the archive.
     * @return true if successfully offered otherwise false.
     */
    public boolean closeSession(final long controlSessionId)
    {
        closeSessionRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId);

        return offer(closeSessionRequestEncoder.encodedLength());
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
        final String channel,
        final int streamId,
        final long correlationId,
        final long controlSessionId)
    {
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
    public boolean stopRecording(
        final long subscriptionId,
        final long correlationId,
        final long controlSessionId)
    {
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
     * Stop an existing replay session.
     *
     * @param replaySessionId  that should be stopped.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean stopReplay(
        final long replaySessionId,
        final long correlationId,
        final long controlSessionId)
    {
        stopReplayRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .replaySessionId(replaySessionId);

        return offer(replayRequestEncoder.encodedLength());
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
        final long fromRecordingId,
        final int recordCount,
        final long correlationId,
        final long controlSessionId)
    {
        listRecordingsRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .fromRecordingId(fromRecordingId)
            .recordCount(recordCount);

        return offer(listRecordingsRequestEncoder.encodedLength());
    }

    /**
     * List a range of recording descriptors which match a channel and stream id.
     *
     * @param fromRecordingId  at which to begin listing.
     * @param recordCount      for the number of descriptors to be listed.
     * @param channel          to match recordings on.
     * @param streamId         to match recordings on.
     * @param correlationId    for this request.
     * @param controlSessionId for this request.
     * @return true if successfully offered otherwise false.
     */
    public boolean listRecordingsForUri(
        final long fromRecordingId,
        final int recordCount,
        final String channel,
        final int streamId,
        final long correlationId,
        final long controlSessionId)
    {
        listRecordingsForUriRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .fromRecordingId(fromRecordingId)
            .recordCount(recordCount)
            .streamId(streamId)
            .channel(channel);

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
        listRecordingRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId);

        return offer(listRecordingRequestEncoder.encodedLength());
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
        truncateRecordingRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .recordingId(recordingId)
            .position(position);

        return offer(truncateRecordingRequestEncoder.encodedLength());
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

            if (nanoClock.nanoTime() > deadlineNs)
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
