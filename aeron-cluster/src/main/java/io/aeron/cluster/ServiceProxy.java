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

import io.aeron.Publication;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.cluster.service.Cluster;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.*;

final class ServiceProxy implements AutoCloseable
{
    private static final int SEND_ATTEMPTS = 5;

    private final BufferClaim bufferClaim = new BufferClaim();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final JoinLogEncoder joinLogEncoder = new JoinLogEncoder();
    private final ClusterMembersResponseEncoder clusterMembersResponseEncoder = new ClusterMembersResponseEncoder();
    private final ServiceTerminationPositionEncoder serviceTerminationPositionEncoder =
        new ServiceTerminationPositionEncoder();
    private final ClusterMembersExtendedResponseEncoder clusterMembersExtendedResponseEncoder =
        new ClusterMembersExtendedResponseEncoder();
    private final ExpandableArrayBuffer expandableArrayBuffer = new ExpandableArrayBuffer();
    private final Publication publication;

    ServiceProxy(final Publication publication)
    {
        this.publication = publication;
    }

    public void close()
    {
        CloseHelper.close(publication);
    }

    void joinLog(
        final long leadershipTermId,
        final long logPosition,
        final long maxLogPosition,
        final int memberId,
        final int logSessionId,
        final int logStreamId,
        final boolean isStartup,
        final Cluster.Role role,
        final String channel)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + JoinLogEncoder.BLOCK_LENGTH +
            JoinLogEncoder.logChannelHeaderLength() + channel.length();
        long result;

        int attempts = SEND_ATTEMPTS;
        do
        {
            result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                joinLogEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .logPosition(logPosition)
                    .maxLogPosition(maxLogPosition)
                    .memberId(memberId)
                    .logSessionId(logSessionId)
                    .logStreamId(logStreamId)
                    .isStartup(isStartup ? BooleanType.TRUE : BooleanType.FALSE)
                    .role(role.code())
                    .logChannel(channel);

                bufferClaim.commit();

                return;
            }

            checkResult(result);
            if (Publication.BACK_PRESSURED == result)
            {
                Thread.yield();
            }
        }
        while (--attempts > 0);

        throw new ClusterException("failed to send join log request: result=" + result);
    }

    void clusterMembersResponse(
        final long correlationId, final int leaderMemberId, final String activeMembers, final String passiveFollowers)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ClusterMembersResponseEncoder.BLOCK_LENGTH +
            ClusterMembersResponseEncoder.activeMembersHeaderLength() + activeMembers.length() +
            ClusterMembersResponseEncoder.passiveFollowersHeaderLength() + passiveFollowers.length();

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                clusterMembersResponseEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .correlationId(correlationId)
                    .leaderMemberId(leaderMemberId)
                    .activeMembers(activeMembers)
                    .passiveFollowers(passiveFollowers);

                bufferClaim.commit();

                return;
            }

            checkResult(result);
            if (Publication.BACK_PRESSURED == result)
            {
                Thread.yield();
            }
        }
        while (--attempts > 0);

        throw new ClusterException("failed to send cluster members response");
    }

    void clusterMembersExtendedResponse(
        final long correlationId,
        final long currentTimeNs,
        final int leaderMemberId,
        final int memberId,
        final ClusterMember[] activeMembers,
        final ClusterMember[] passiveMembers)
    {
        clusterMembersExtendedResponseEncoder
            .wrapAndApplyHeader(expandableArrayBuffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .currentTimeNs(currentTimeNs)
            .leaderMemberId(leaderMemberId)
            .memberId(memberId);

        final ClusterMembersExtendedResponseEncoder.ActiveMembersEncoder activeMembersEncoder =
            clusterMembersExtendedResponseEncoder.activeMembersCount(activeMembers.length);
        for (final ClusterMember member : activeMembers)
        {
            activeMembersEncoder.next()
                .leadershipTermId(member.leadershipTermId())
                .logPosition(member.logPosition())
                .timeOfLastAppendNs(member.timeOfLastAppendPositionNs())
                .memberId(member.id())
                .ingressEndpoint(member.ingressEndpoint())
                .consensusEndpoint(member.consensusEndpoint())
                .logEndpoint(member.logEndpoint())
                .catchupEndpoint(member.catchupEndpoint())
                .archiveEndpoint(member.archiveEndpoint());
        }

        final ClusterMembersExtendedResponseEncoder.PassiveMembersEncoder passiveMembersEncoder =
            clusterMembersExtendedResponseEncoder.passiveMembersCount(passiveMembers.length);
        for (final ClusterMember member : passiveMembers)
        {
            passiveMembersEncoder.next()
                .leadershipTermId(member.leadershipTermId())
                .logPosition(member.logPosition())
                .timeOfLastAppendNs(member.timeOfLastAppendPositionNs())
                .memberId(member.id())
                .ingressEndpoint(member.ingressEndpoint())
                .consensusEndpoint(member.consensusEndpoint())
                .logEndpoint(member.logEndpoint())
                .catchupEndpoint(member.catchupEndpoint())
                .archiveEndpoint(member.archiveEndpoint());
        }

        final int length = clusterMembersExtendedResponseEncoder.encodedLength() + MessageHeaderEncoder.ENCODED_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.offer(expandableArrayBuffer, 0, length, null);
            if (result > 0)
            {
                return;
            }

            checkResult(result);
            if (Publication.BACK_PRESSURED == result)
            {
                Thread.yield();
            }
        }
        while (--attempts > 0);

        throw new ClusterException("failed to send cluster members extended response");
    }

    void terminationPosition(final long logPosition, final ErrorHandler errorHandler)
    {
        if (!publication.isClosed())
        {
            final int length = MessageHeaderDecoder.ENCODED_LENGTH + ServiceTerminationPositionEncoder.BLOCK_LENGTH;
            long result;

            int attempts = SEND_ATTEMPTS;
            do
            {
                result = publication.tryClaim(length, bufferClaim);
                if (result > 0)
                {
                    serviceTerminationPositionEncoder
                        .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                        .logPosition(logPosition);

                    bufferClaim.commit();

                    return;
                }

                if (Publication.BACK_PRESSURED == result)
                {
                    Thread.yield();
                }
            }
            while (--attempts > 0);

            errorHandler.onError(new ClusterException(
                "failed to send service termination position: result=" + result, AeronException.Category.WARN));
        }
    }

    private static void checkResult(final long result)
    {
        if (result == Publication.NOT_CONNECTED ||
            result == Publication.CLOSED ||
            result == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new ClusterException("unexpected publication state: " + result);
        }
    }
}
