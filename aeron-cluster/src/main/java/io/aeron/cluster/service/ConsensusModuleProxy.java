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
package io.aeron.cluster.service;

import io.aeron.DirectBufferVector;
import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;

/**
 * Proxy for communicating with the Consensus Module over IPC.
 * <p>
 * <b>Note: </b>This class is not for public use.
 */
public final class ConsensusModuleProxy implements AutoCloseable
{
    private static final int SEND_ATTEMPTS = 3;

    private final BufferClaim bufferClaim = new BufferClaim();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final ScheduleTimerEncoder scheduleTimerEncoder = new ScheduleTimerEncoder();
    private final CancelTimerEncoder cancelTimerEncoder = new CancelTimerEncoder();
    private final ServiceAckEncoder serviceAckEncoder = new ServiceAckEncoder();
    private final CloseSessionEncoder closeSessionEncoder = new CloseSessionEncoder();
    private final ClusterMembersQueryEncoder clusterMembersQueryEncoder = new ClusterMembersQueryEncoder();
    private final RemoveMemberEncoder removeMemberEncoder = new RemoveMemberEncoder();
    private final Publication publication;

    /**
     * Construct a proxy to the consensus module that will send messages over a provided {@link Publication}.
     *
     * @param publication for sending messages to the consensus module.
     */
    public ConsensusModuleProxy(final Publication publication)
    {
        this.publication = publication;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.close(publication);
    }

    boolean scheduleTimer(final long correlationId, final long deadline)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ScheduleTimerEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                scheduleTimerEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .correlationId(correlationId)
                    .deadline(deadline);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean cancelTimer(final long correlationId)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + CancelTimerEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                cancelTimerEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .correlationId(correlationId);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    long offer(
        final DirectBuffer headerBuffer,
        final int headerOffset,
        final int headerLength,
        final DirectBuffer messageBuffer,
        final int messageOffset,
        final int messageLength)
    {
        return publication.offer(headerBuffer, headerOffset, headerLength, messageBuffer, messageOffset, messageLength);
    }

    long offer(final DirectBufferVector[] vectors)
    {
        return publication.offer(vectors, null);
    }

    long tryClaim(final int length, final BufferClaim bufferClaim, final DirectBuffer sessionHeader)
    {
        final long result = publication.tryClaim(length, bufferClaim);
        if (result > 0)
        {
            bufferClaim.putBytes(sessionHeader, 0, AeronCluster.SESSION_HEADER_LENGTH);
        }

        return result;
    }

    boolean ack(
        final long logPosition, final long timestamp, final long ackId, final long relevantId, final int serviceId)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ServiceAckEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                serviceAckEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .logPosition(logPosition)
                    .timestamp(timestamp)
                    .ackId(ackId)
                    .relevantId(relevantId)
                    .serviceId(serviceId);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    boolean closeSession(final long clusterSessionId)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + CloseSessionEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                closeSessionEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .clusterSessionId(clusterSessionId);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    /**
     * Query for the current cluster members.
     *
     * @param correlationId for the request.
     * @return true of the request was successfully sent, otherwise false.
     */
    public boolean clusterMembersQuery(final long correlationId)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ClusterMembersQueryEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                clusterMembersQueryEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .correlationId(correlationId)
                    .extended(BooleanType.TRUE);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);
        }
        while (--attempts > 0);

        return false;
    }

    /**
     * Remove a member by id from the cluster.
     *
     * @param memberId  to be removed.
     * @param isPassive to indicate if the member is passive or not.
     * @return true of the request was successfully sent, otherwise false.
     */
    public boolean removeMember(final int memberId, final BooleanType isPassive)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + RemoveMemberEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                removeMemberEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .memberId(memberId)
                    .isPassive(isPassive);

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
            throw new ClusterException("unexpected publication state: " + result);
        }
    }
}
