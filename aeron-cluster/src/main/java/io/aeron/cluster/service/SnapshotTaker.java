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
package io.aeron.cluster.service;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.cluster.client.ClusterClock;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SnapshotMark;
import io.aeron.cluster.codecs.SnapshotMarkerEncoder;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.IdleStrategy;

import java.util.concurrent.TimeUnit;

/**
 * Based class of common functions required to take a snapshot of cluster state.
 */
public class SnapshotTaker
{
    protected static final int ENCODED_MARKER_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + SnapshotMarkerEncoder.BLOCK_LENGTH;
    protected final BufferClaim bufferClaim = new BufferClaim();
    protected final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    protected final ExclusivePublication publication;
    protected final IdleStrategy idleStrategy;
    protected final AgentInvoker aeronAgentInvoker;
    private final SnapshotMarkerEncoder snapshotMarkerEncoder = new SnapshotMarkerEncoder();

    public SnapshotTaker(
        final ExclusivePublication publication, final IdleStrategy idleStrategy, final AgentInvoker aeronAgentInvoker)
    {
        this.publication = publication;
        this.idleStrategy = idleStrategy;
        this.aeronAgentInvoker = aeronAgentInvoker;
    }

    public void markBegin(
        final long snapshotTypeId,
        final long logPosition,
        final long leadershipTermId,
        final int snapshotIndex,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        markSnapshot(
            snapshotTypeId, logPosition, leadershipTermId, snapshotIndex, SnapshotMark.BEGIN, timeUnit, appVersion);
    }

    public void markEnd(
        final long snapshotTypeId,
        final long logPosition,
        final long leadershipTermId,
        final int snapshotIndex,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        markSnapshot(
            snapshotTypeId, logPosition, leadershipTermId, snapshotIndex, SnapshotMark.END, timeUnit, appVersion);
    }

    public void markSnapshot(
        final long snapshotTypeId,
        final long logPosition,
        final long leadershipTermId,
        final int snapshotIndex,
        final SnapshotMark snapshotMark,
        final TimeUnit timeUnit,
        final int appVersion)
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
                    .mark(snapshotMark)
                    .timeUnit(ClusterClock.map(timeUnit))
                    .appVersion(appVersion);

                bufferClaim.commit();
                break;
            }

            checkResultAndIdle(result);
        }
    }

    protected static void checkInterruptStatus()
    {
        if (Thread.interrupted())
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
        checkInterruptStatus();
        invokeAgentClient();
        idleStrategy.idle();
    }

    protected void invokeAgentClient()
    {
        if (null != aeronAgentInvoker)
        {
            aeronAgentInvoker.invoke();
        }
    }
}
