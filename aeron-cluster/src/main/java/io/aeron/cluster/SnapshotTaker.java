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

import io.aeron.Publication;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.IdleStrategy;

class SnapshotTaker
{
    private static final int ENCODED_MARKER_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + SnapshotMarkerEncoder.BLOCK_LENGTH;

    private static final int ENCODED_TIMER_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + TimerEncoder.BLOCK_LENGTH;

    private final BufferClaim bufferClaim = new BufferClaim();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final SnapshotMarkerEncoder snapshotMarkerEncoder = new SnapshotMarkerEncoder();
    private final ClusterSessionEncoder clusterSessionEncoder = new ClusterSessionEncoder();
    private final TimerEncoder timerEncoder = new TimerEncoder();
    private final Publication publication;
    private final AgentInvoker aeronClientInvoker;
    private final IdleStrategy idleStrategy;

    SnapshotTaker(
        final Publication publication, final AgentInvoker aeronClientInvoker, final IdleStrategy idleStrategy)
    {
        this.publication = publication;
        this.aeronClientInvoker = aeronClientInvoker;
        this.idleStrategy = idleStrategy;
    }

    public void markBegin(final long snapshotTypeId, final int snapshotIndex)
    {
        markSnapshot(snapshotTypeId, snapshotIndex, SnapshotMark.BEGIN);
    }

    public void markEnd(final long snapshotTypeId, final int snapshotIndex)
    {
        markSnapshot(snapshotTypeId, snapshotIndex, SnapshotMark.END);
    }

    public void markSnapshot(final long snapshotTypeId, final int snapshotIndex, final SnapshotMark snapshotMark)
    {
        idleStrategy.reset();
        while (true)
        {
            final long result = publication.tryClaim(ENCODED_MARKER_LENGTH, bufferClaim);
            if (result > 0)
            {
                snapshotMarkerEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .typeId(snapshotTypeId)
                    .index(0)
                    .mark(snapshotMark);

                bufferClaim.commit();
                break;
            }

            checkResult(result);
            checkInterruptedStatus();
            invokeAeronClient();
            idleStrategy.idle();
        }
    }

    public void snapshotSession(final ClusterSession session)
    {
        final String responseChannel = session.responsePublication().channel();
        final int responseStreamId = session.responsePublication().streamId();

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ClusterSessionEncoder.BLOCK_LENGTH +
            ClusterSessionEncoder.responseChannelHeaderLength() + responseChannel.length();

        idleStrategy.reset();
        while (true)
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                clusterSessionEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .clusterSessionId(session.id())
                    .lastCorrelationId(session.lastCorrelationId())
                    .timeOfLastActivity(session.timeOfLastActivityMs())
                    .responseStreamId(responseStreamId)
                    .responseChannel(responseChannel);

                bufferClaim.commit();
                break;
            }

            checkResult(result);
            checkInterruptedStatus();
            invokeAeronClient();
            idleStrategy.idle();
        }
    }

    public void snapshotTimer(final long correlationId, final long deadline)
    {
        idleStrategy.reset();
        while (true)
        {
            final long result = publication.tryClaim(ENCODED_TIMER_LENGTH, bufferClaim);
            if (result > 0)
            {
                timerEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .correlationId(correlationId)
                    .deadline(deadline);

                bufferClaim.commit();
                break;
            }

            checkResult(result);
            checkInterruptedStatus();
            invokeAeronClient();
            idleStrategy.idle();
        }
    }

    private void invokeAeronClient()
    {
        if (null != aeronClientInvoker)
        {
            aeronClientInvoker.invoke();
        }
    }

    private static void checkInterruptedStatus()
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new AgentTerminationException("Unexpected interrupt during operation");
        }
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
