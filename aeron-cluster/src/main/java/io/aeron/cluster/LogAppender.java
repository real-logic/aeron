/*
 * Copyright 2017 Real Logic Ltd.
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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;

class LogAppender
{
    private static final int SEND_ATTEMPTS = 3;

    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final SessionOpenEventEncoder connectEventEncoder = new SessionOpenEventEncoder();
    private final SessionCloseEventEncoder closeEventEncoder = new SessionCloseEventEncoder();
    private final TimerEventEncoder timerEventEncoder = new TimerEventEncoder();
    private final ClusterActionRequestEncoder actionRequestEncoder = new ClusterActionRequestEncoder();
    private final ExpandableArrayBuffer expandableArrayBuffer = new ExpandableArrayBuffer();
    private final BufferClaim bufferClaim = new BufferClaim();
    private Publication publication;
    private String recordingChannel;

    public int connect(final Aeron aeron, final AeronArchive aeronArchive, final String channel, final int streamId)
    {
        if (null != publication)
        {
            throw new IllegalStateException("Publication already exists");
        }

        publication = aeron.addExclusivePublication(channel, streamId);
        final int sessionId = publication.sessionId();
        recordingChannel = ChannelUri.addSessionId(channel, sessionId);
        aeronArchive.startRecording(recordingChannel, streamId, SourceLocation.LOCAL);

        return sessionId;
    }

    public void disconnect()
    {
        if (null != publication)
        {
            publication.close();
            publication = null;
        }
    }

    public int sessionId()
    {
        return publication.sessionId();
    }

    public String recordingChannel()
    {
        return recordingChannel;
    }

    public long position()
    {
        if (null == publication)
        {
            return 0;
        }

        return publication.position();
    }

    public boolean appendMessage(final DirectBuffer buffer, final int offset, final int length, final long nowMs)
    {
        final int timestampOffset =
            offset + MessageHeaderEncoder.ENCODED_LENGTH + SessionHeaderEncoder.timestampEncodingOffset();

        ((MutableDirectBuffer)buffer).putLong(timestampOffset, nowMs, SessionHeaderEncoder.BYTE_ORDER);

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.offer(buffer, offset, length);
            if (result > 0)
            {
                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    public long appendConnectedSession(final ClusterSession session, final long nowMs)
    {
        long result = -1;
        final byte[] sessionPrincipalData = session.principalData();
        final String channel = session.responseChannel();

        connectEventEncoder
            .wrapAndApplyHeader(expandableArrayBuffer, 0, messageHeaderEncoder)
            .clusterSessionId(session.id())
            .correlationId(session.lastCorrelationId())
            .timestamp(nowMs)
            .responseStreamId(session.responseStreamId())
            .responseChannel(channel)
            .putPrincipalData(sessionPrincipalData, 0, sessionPrincipalData.length);

        final int length = connectEventEncoder.encodedLength() + MessageHeaderEncoder.ENCODED_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            result = publication.offer(expandableArrayBuffer, 0, length);
            if (result > 0)
            {
                return result;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return result;
    }

    public boolean appendClosedSession(final ClusterSession session, final CloseReason closeReason, final long nowMs)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + SessionCloseEventEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                closeEventEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .clusterSessionId(session.id())
                    .timestamp(nowMs)
                    .closeReason(closeReason);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    public boolean appendTimerEvent(final long correlationId, final long nowMs)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + TimerEventEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                timerEventEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .correlationId(correlationId)
                    .timestamp(nowMs);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    public boolean appendClusterAction(
        final ClusterAction action, final long logPosition, final long leadershipTermId, final long nowMs)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ClusterActionRequestEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                actionRequestEncoder.wrapAndApplyHeader(
                    bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .logPosition(logPosition)
                    .leadershipTermId(leadershipTermId)
                    .timestamp(nowMs)
                    .action(action);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    private static void checkResult(final long result)
    {
        if (result == Publication.NOT_CONNECTED ||
            result == Publication.CLOSED ||
            result == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new IllegalStateException("Unexpected publication state: " + result);
        }
    }
}
