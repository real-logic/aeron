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
package io.aeron.cluster;

import io.aeron.Publication;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.CloseHelper;

final class ServiceProxy implements AutoCloseable
{
    private static final int SEND_ATTEMPTS = 3;

    private final BufferClaim bufferClaim = new BufferClaim();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final JoinLogEncoder joinLogEncoder = new JoinLogEncoder();
    private final ClusterMembersResponseEncoder clusterMembersResponseEncoder = new ClusterMembersResponseEncoder();
    private final ServiceTerminationPositionEncoder serviceTerminationPositionEncoder =
        new ServiceTerminationPositionEncoder();
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
        final String channel)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + JoinLogEncoder.BLOCK_LENGTH +
            JoinLogEncoder.logChannelHeaderLength() + channel.length();

        int attempts = SEND_ATTEMPTS * 2;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
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
                    .logChannel(channel);

                bufferClaim.commit();

                return;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        throw new ClusterException("failed to send join log request");
    }

    void clusterMembersResponse(
        final long correlationId, final int leaderMemberId, final String activeMembers, final String passiveFollowers)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ClusterMembersResponseEncoder.BLOCK_LENGTH +
            ClusterMembersResponseEncoder.activeMembersHeaderLength() + activeMembers.length() +
            ClusterMembersResponseEncoder.passiveFollowersHeaderLength() + passiveFollowers.length();

        int attempts = SEND_ATTEMPTS * 2;
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
        }
        while (--attempts > 0);

        throw new ClusterException("failed to send cluster members response");
    }

    void terminationPosition(final long logPosition)
    {
        final int length = MessageHeaderDecoder.ENCODED_LENGTH + ServiceTerminationPositionEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS * 2;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                serviceTerminationPositionEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .logPosition(logPosition);

                bufferClaim.commit();

                return;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        throw new ClusterException("failed to send service termination position");
    }

    private static void checkResult(final long result)
    {
        if (result == Publication.NOT_CONNECTED ||
            result == Publication.CLOSED ||
            result == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new AeronException("unexpected publication state: " + result);
        }
    }
}
