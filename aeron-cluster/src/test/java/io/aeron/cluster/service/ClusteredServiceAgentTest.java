/*
 * Copyright 2014-2022 Real Logic Limited.
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

import io.aeron.*;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.driver.DutyCycleTracker;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.test.Tests;
import org.agrona.DirectBuffer;
import org.agrona.MarkFile;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.*;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.AeronCounters.CLUSTER_COMMIT_POSITION_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_RECOVERY_STATE_TYPE_ID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ClusteredServiceAgentTest
{
    @Test
    void shouldClaimAndWriteToBufferWhenFollower()
    {
        final Aeron aeron = mock(Aeron.class);
        final Publication publication = mock(Publication.class);

        final ClusteredServiceContainer.Context ctx = new ClusteredServiceContainer.Context()
            .aeron(aeron)
            .idleStrategySupplier(() -> YieldingIdleStrategy.INSTANCE);
        final ClusteredServiceAgent clusteredServiceAgent = new ClusteredServiceAgent(ctx);

        final BufferClaim bufferClaim = new BufferClaim();
        final int length = 64;
        final DirectBuffer msg = new UnsafeBuffer(new byte[length]);

        final long l = clusteredServiceAgent.tryClaim(0, publication, length, bufferClaim);
        assertEquals(ClientSession.MOCKED_OFFER, l);
        final MutableDirectBuffer buffer = bufferClaim.buffer();
        buffer.putBytes(bufferClaim.offset() + AeronCluster.SESSION_HEADER_LENGTH, msg, 0, length);
    }

    @Test
    void shouldAbortClusteredServiceIfCommitPositionCounterIsClosed()
    {
        final UnsafeBuffer claimBuffer = new UnsafeBuffer(new byte[64 * 1024]);
        final Aeron aeron = mock(Aeron.class);
        final ConcurrentPublication publication = mock(ConcurrentPublication.class);
        final Subscription subscription = mock(Subscription.class);
        when(aeron.addPublication(any(), anyInt())).thenReturn(publication);
        when(aeron.addSubscription(any(), anyInt())).thenReturn(subscription);
        when(publication.tryClaim(anyInt(), any())).thenAnswer(
            invocation ->
            {
                final BufferClaim claim = invocation.getArgument(1, BufferClaim.class);
                claim.wrap(claimBuffer, 0, claimBuffer.capacity());
                return invocation.getArgument(0, Integer.class).longValue();
            });
        final ArgumentCaptor<UnavailableCounterHandler> captor =
            ArgumentCaptor.forClass(UnavailableCounterHandler.class);
        final ClusterMarkFile markFile = mock(ClusterMarkFile.class);
        final CountersManager countersManager = new CountersManager(
            new UnsafeBuffer(new byte[64 * 1024]), new UnsafeBuffer(new byte[16 * 1024]));

        when(aeron.addCounter(anyInt(), any(), anyInt(), anyInt(), any(), anyInt(), anyInt())).then(
            invocation ->
            {
                final int counterId = countersManager.allocate(
                    invocation.getArgument(0, Integer.class),
                    invocation.getArgument(1, DirectBuffer.class),
                    invocation.getArgument(2, Integer.class),
                    invocation.getArgument(3, Integer.class),
                    invocation.getArgument(4, DirectBuffer.class),
                    invocation.getArgument(5, Integer.class),
                    invocation.getArgument(6, Integer.class));
                return new Counter(countersManager, 0, counterId);
            });

        RecoveryState.allocate(aeron, NULL_VALUE, 0, 0, 0, 0);

        final int commitPositionCounterId = countersManager.allocate(
            "commit-pos", CLUSTER_COMMIT_POSITION_TYPE_ID);
        final int recoveryStateCounterId = countersManager.allocate(
            "recovery-state", CLUSTER_RECOVERY_STATE_TYPE_ID);
        countersManager.setCounterValue(recoveryStateCounterId, NULL_VALUE);

        when(aeron.countersReader()).thenReturn(countersManager);
        when(aeron.isClosed()).thenReturn(false);

        final ClusteredServiceContainer.Context ctx = new ClusteredServiceContainer.Context()
            .aeron(aeron)
            .nanoClock(SystemNanoClock.INSTANCE)
            .epochClock(SystemEpochClock.INSTANCE)
            .clusterMarkFile(markFile)
            .clusteredService(mock(ClusteredService.class))
            .dutyCycleTracker(new DutyCycleTracker())
            .idleStrategySupplier(() -> YieldingIdleStrategy.INSTANCE)
            .terminationHook(() -> {});
        final ClusteredServiceAgent clusteredServiceAgent = new ClusteredServiceAgent(ctx);

        clusteredServiceAgent.onStart();

        verify(aeron).addUnavailableCounterHandler(captor.capture());

        clusteredServiceAgent.doWork();

        captor.getValue().onUnavailableCounter(countersManager, 0, commitPositionCounterId);

        assertThrows(AgentTerminationException.class, clusteredServiceAgent::doWork);
    }
}