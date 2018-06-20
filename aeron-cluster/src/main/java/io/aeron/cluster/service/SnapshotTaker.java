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
package io.aeron.cluster.service;

import io.aeron.Publication;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SnapshotMark;
import io.aeron.cluster.codecs.SnapshotMarkerEncoder;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.IdleStrategy;

public class SnapshotTaker
{
    protected static final int ENCODED_MARKER_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + SnapshotMarkerEncoder.BLOCK_LENGTH;
    protected final BufferClaim bufferClaim = new BufferClaim();
    protected final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    protected final Publication publication;
    protected final IdleStrategy idleStrategy;
    protected final AgentInvoker aeronClientInvoker;
    private final SnapshotMarkerEncoder snapshotMarkerEncoder = new SnapshotMarkerEncoder();

    public SnapshotTaker(
        final Publication publication, final IdleStrategy idleStrategy, final AgentInvoker aeronClientInvoker)
    {
        this.publication = publication;
        this.idleStrategy = idleStrategy;
        this.aeronClientInvoker = aeronClientInvoker;
    }

    public void markBegin(
        final long snapshotTypeId, final long logPosition, final long leadershipTermId, final int snapshotIndex)
    {
        markSnapshot(snapshotTypeId, logPosition, leadershipTermId, snapshotIndex, SnapshotMark.BEGIN);
    }

    public void markEnd(
        final long snapshotTypeId, final long logPosition, final long leadershipTermId, final int snapshotIndex)
    {
        markSnapshot(snapshotTypeId, logPosition, leadershipTermId, snapshotIndex, SnapshotMark.END);
    }

    public void markSnapshot(
        final long snapshotTypeId,
        final long logPosition,
        final long leadershipTermId,
        final int snapshotIndex,
        final SnapshotMark snapshotMark)
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
                    .logPosition(logPosition)
                    .leadershipTermId(leadershipTermId)
                    .index(snapshotIndex)
                    .mark(snapshotMark);

                bufferClaim.commit();
                break;
            }

            checkResultAndIdle(result);
        }
    }

    protected static void checkInterruptedStatus()
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new AgentTerminationException("unexpected interrupt during operation");
        }
    }

    protected static void checkResult(final long result)
    {
        if (result == Publication.NOT_CONNECTED ||
            result == Publication.CLOSED ||
            result == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new AeronException("unexpected publication state: " + result);
        }
    }

    protected void checkResultAndIdle(final long result)
    {
        checkResult(result);
        checkInterruptedStatus();
        invokeAeronClient();
        idleStrategy.idle();
    }

    protected void invokeAeronClient()
    {
        if (null != aeronClientInvoker)
        {
            aeronClientInvoker.invoke();
        }
    }
}
