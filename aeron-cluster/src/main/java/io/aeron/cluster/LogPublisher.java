/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron.cluster;

import io.aeron.*;
import io.aeron.cluster.service.ClusterClock;
import io.aeron.cluster.codecs.*;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;

import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.agrona.BitUtil.align;

final class LogPublisher
{
    private static final int SEND_ATTEMPTS = 3;

    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final SessionMessageHeaderEncoder sessionHeaderEncoder = new SessionMessageHeaderEncoder();
    private final SessionOpenEventEncoder sessionOpenEventEncoder = new SessionOpenEventEncoder();
    private final SessionCloseEventEncoder sessionCloseEventEncoder = new SessionCloseEventEncoder();
    private final TimerEventEncoder timerEventEncoder = new TimerEventEncoder();
    private final ClusterActionRequestEncoder clusterActionRequestEncoder = new ClusterActionRequestEncoder();
    private final NewLeadershipTermEventEncoder newLeadershipTermEventEncoder = new NewLeadershipTermEventEncoder();
    private final MembershipChangeEventEncoder membershipChangeEventEncoder = new MembershipChangeEventEncoder();
    private final UnsafeBuffer sessionHeaderBuffer = new UnsafeBuffer(new byte[SESSION_HEADER_LENGTH]);
    private final ExpandableArrayBuffer expandableArrayBuffer = new ExpandableArrayBuffer();
    private final BufferClaim bufferClaim = new BufferClaim();

    private ExclusivePublication publication;

    LogPublisher()
    {
        sessionHeaderEncoder.wrapAndApplyHeader(sessionHeaderBuffer, 0, new MessageHeaderEncoder());
    }

    void publication(final ExclusivePublication publication)
    {
        if (null != this.publication)
        {
            this.publication.close();
        }
        this.publication = publication;
    }

    void disconnect(final ErrorHandler errorHandler)
    {
        if (null != publication)
        {
            CloseHelper.close(errorHandler, publication);
            this.publication = null;
        }
    }

    boolean isConnected()
    {
        if (null == publication)
        {
            return false;
        }

        return publication.isConnected();
    }

    long position()
    {
        if (null == publication)
        {
            return 0;
        }

        return publication.position();
    }

    int sessionId()
    {
        return publication.sessionId();
    }

    void addDestination(final boolean isLogChannelMultiDestination, final String followerLogEndpoint)
    {
        if (isLogChannelMultiDestination && null != publication)
        {
            publication.asyncAddDestination("aeron:udp?endpoint=" + followerLogEndpoint);
        }
    }

    void removeDestination(final boolean isLogChannelMultiDestination, final String followerLogEndpoint)
    {
        if (isLogChannelMultiDestination && null != publication)
        {
            publication.asyncRemoveDestination("aeron:udp?endpoint=" + followerLogEndpoint);
        }
    }

    long appendMessage(
        final long leadershipTermId,
        final long clusterSessionId,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        sessionHeaderEncoder
            .leadershipTermId(leadershipTermId)
            .clusterSessionId(clusterSessionId)
            .timestamp(timestamp);

        int attempts = SEND_ATTEMPTS;
        long result;
        do
        {
            result = publication.offer(sessionHeaderBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length, null);

            if (result > 0)
            {
                break;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return result;
    }

    long appendSessionOpen(final ClusterSession session, final long leadershipTermId, final long timestamp)
    {
        long result;
        final byte[] encodedPrincipal = session.encodedPrincipal();
        final String channel = session.responseChannel();

        sessionOpenEventEncoder
            .wrapAndApplyHeader(expandableArrayBuffer, 0, messageHeaderEncoder)
            .leadershipTermId(leadershipTermId)
            .clusterSessionId(session.id())
            .correlationId(session.correlationId())
            .timestamp(timestamp)
            .responseStreamId(session.responseStreamId())
            .responseChannel(channel)
            .putEncodedPrincipal(encodedPrincipal, 0, encodedPrincipal.length);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + sessionOpenEventEncoder.encodedLength();

        int attempts = SEND_ATTEMPTS;
        do
        {
            result = publication.offer(expandableArrayBuffer, 0, length, null);
            if (result > 0)
            {
                break;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return result;
    }

    boolean appendSessionClose(final ClusterSession session, final long leadershipTermId, final long timestamp)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + SessionCloseEventEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                sessionCloseEventEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .clusterSessionId(session.id())
                    .timestamp(timestamp)
                    .closeReason(session.closeReason());

                bufferClaim.commit();
                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    long appendTimer(final long correlationId, final long leadershipTermId, final long timestamp)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + TimerEventEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        long result;
        do
        {
            result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                timerEventEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .correlationId(correlationId)
                    .timestamp(timestamp);

                bufferClaim.commit();
                break;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return result;
    }

    boolean appendClusterAction(final long leadershipTermId, final long timestamp, final ClusterAction action)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ClusterActionRequestEncoder.BLOCK_LENGTH;
        final int fragmentLength = DataHeaderFlyweight.HEADER_LENGTH + length;
        final int alignedFragmentLength = align(fragmentLength, FRAME_ALIGNMENT);

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long logPosition = publication.position() + alignedFragmentLength;
            final long result = publication.tryClaim(length, bufferClaim);

            if (result > 0)
            {
                clusterActionRequestEncoder.wrapAndApplyHeader(
                    bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .logPosition(logPosition)
                    .timestamp(timestamp)
                    .action(action);

                bufferClaim.commit();
                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean appendNewLeadershipTermEvent(
        final long leadershipTermId,
        final long timestamp,
        final long termBaseLogPosition,
        final int leaderMemberId,
        final int logSessionId,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + NewLeadershipTermEventEncoder.BLOCK_LENGTH;
        final int fragmentLength = DataHeaderFlyweight.HEADER_LENGTH + length;
        final int alignedFragmentLength = align(fragmentLength, FRAME_ALIGNMENT);

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long logPosition = publication.position() + alignedFragmentLength;
            final long result = publication.tryClaim(length, bufferClaim);

            if (result > 0)
            {
                newLeadershipTermEventEncoder.wrapAndApplyHeader(
                    bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .logPosition(logPosition)
                    .timestamp(timestamp)
                    .termBaseLogPosition(termBaseLogPosition)
                    .leaderMemberId(leaderMemberId)
                    .logSessionId(logSessionId)
                    .timeUnit(ClusterClock.map(timeUnit))
                    .appVersion(appVersion);

                bufferClaim.commit();
                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    long appendMembershipChangeEvent(
        final long leadershipTermId,
        final long timestamp,
        final int leaderMemberId,
        final int clusterSize,
        final ChangeType changeType,
        final int memberId,
        final String clusterMembers)
    {
        long result;
        final int fragmentedLength = computeMembershipChangeEventFragmentedLength(clusterMembers);

        int attempts = SEND_ATTEMPTS;
        do
        {
            membershipChangeEventEncoder
                .wrapAndApplyHeader(expandableArrayBuffer, 0, messageHeaderEncoder)
                .leadershipTermId(leadershipTermId)
                .logPosition(publication.position() + fragmentedLength)
                .timestamp(timestamp)
                .leaderMemberId(leaderMemberId)
                .clusterSize(clusterSize)
                .changeType(changeType)
                .memberId(memberId)
                .clusterMembers(clusterMembers);

            final int length = MessageHeaderEncoder.ENCODED_LENGTH + membershipChangeEventEncoder.encodedLength();
            result = publication.offer(expandableArrayBuffer, 0, length, null);
            if (result > 0)
            {
                break;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return result;
    }

    private int computeMembershipChangeEventFragmentedLength(final String clusterMembers)
    {
        final int messageLength = MessageHeaderEncoder.ENCODED_LENGTH +
            MembershipChangeEventEncoder.BLOCK_LENGTH +
            MembershipChangeEventEncoder.clusterMembersHeaderLength() +
            clusterMembers.length();

        final int maxPayloadLength = publication.maxPayloadLength();
        final int numMaxPayloads = messageLength / maxPayloadLength;
        final int remainingPayload = messageLength % maxPayloadLength;
        final int lastFrameLength = remainingPayload > 0 ? align(remainingPayload + HEADER_LENGTH, FRAME_ALIGNMENT) : 0;

        return (numMaxPayloads * (maxPayloadLength + HEADER_LENGTH)) + lastFrameLength;
    }

    private static void checkResult(final long result)
    {
        if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new AeronException("unexpected publication state: " + result);
        }
    }
}
