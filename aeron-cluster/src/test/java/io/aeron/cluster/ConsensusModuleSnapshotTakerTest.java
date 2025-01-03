/*
 * Copyright 2014-2025 Real Logic Limited.
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

import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.cluster.codecs.*;
import io.aeron.cluster.service.ClusterClock;
import io.aeron.logbuffer.BufferClaim;

import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

import static io.aeron.Publication.ADMIN_ACTION;
import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.agrona.BitUtil.align;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class ConsensusModuleSnapshotTakerTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ConsensusModuleDecoder consensusModuleDecoder = new ConsensusModuleDecoder();
    private final PendingMessageTrackerDecoder pendingMessageTrackerDecoder = new PendingMessageTrackerDecoder();
    private final TimerDecoder timerDecoder = new TimerDecoder();
    private final ClusterSessionDecoder clusterSessionDecoder = new ClusterSessionDecoder();
    private final ExclusivePublication publication = mock(ExclusivePublication.class);
    private final IdleStrategy idleStrategy = mock(IdleStrategy.class);

    private final ConsensusModuleSnapshotTaker snapshotTaker = new ConsensusModuleSnapshotTaker(
        publication, idleStrategy, null);

    @Test
    void snapshotConsensusModuleState()
    {
        final int offset = 32;
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ConsensusModuleEncoder.BLOCK_LENGTH;
        final long nextSessionId = 42;
        final long nextServiceSessionId = -4_000_000_001L;
        final long logServiceSessionId = -4_000_000_000L;
        final int pendingMessageCapacity = 4096;
        when(publication.tryClaim(eq(length), any()))
            .thenReturn(BACK_PRESSURED, ADMIN_ACTION)
            .thenAnswer(mockTryClaim(offset));

        snapshotTaker.snapshotConsensusModuleState(
            nextSessionId, nextServiceSessionId, logServiceSessionId, pendingMessageCapacity);

        final InOrder inOrder = inOrder(idleStrategy, publication);
        inOrder.verify(idleStrategy).reset();
        inOrder.verify(publication).tryClaim(anyInt(), any());
        inOrder.verify(idleStrategy).idle();
        inOrder.verify(publication).tryClaim(anyInt(), any());
        inOrder.verify(idleStrategy).idle();
        inOrder.verify(publication).tryClaim(anyInt(), any());
        inOrder.verifyNoMoreInteractions();

        consensusModuleDecoder.wrapAndApplyHeader(buffer, offset + HEADER_LENGTH, messageHeaderDecoder);
        assertEquals(nextSessionId, consensusModuleDecoder.nextSessionId());
        assertEquals(nextServiceSessionId, consensusModuleDecoder.nextServiceSessionId());
        assertEquals(logServiceSessionId, consensusModuleDecoder.logServiceSessionId());
        assertEquals(pendingMessageCapacity, consensusModuleDecoder.pendingMessageCapacity());
    }

    @Test
    void snapshotPendingServiceMessageTracker()
    {
        final int offset = 108;
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + PendingMessageTrackerEncoder.BLOCK_LENGTH;
        final int serviceId = 6;
        final PendingServiceMessageTracker pendingServiceMessageTracker = new PendingServiceMessageTracker(
            serviceId, mock(Counter.class), mock(LogPublisher.class), mock(ClusterClock.class));
        pendingServiceMessageTracker.enqueueMessage(buffer, 32, 0);
        final int capacity = pendingServiceMessageTracker.size();
        when(publication.tryClaim(eq(length), any()))
            .thenReturn(ADMIN_ACTION)
            .thenAnswer(mockTryClaim(offset));
        when(publication.offer(any(), anyInt(), anyInt()))
            .thenReturn(BACK_PRESSURED, 9L);

        snapshotTaker.snapshot(pendingServiceMessageTracker, mock(ErrorHandler.class));

        final InOrder inOrder = inOrder(idleStrategy, publication);
        inOrder.verify(idleStrategy).reset();
        inOrder.verify(publication).tryClaim(anyInt(), any());
        inOrder.verify(idleStrategy).idle();
        inOrder.verify(publication).tryClaim(anyInt(), any());
        inOrder.verify(idleStrategy).reset();
        inOrder.verify(publication).offer(any(), anyInt(), anyInt());
        inOrder.verify(idleStrategy).idle();
        inOrder.verify(publication).offer(any(), anyInt(), anyInt());
        inOrder.verifyNoMoreInteractions();

        pendingMessageTrackerDecoder.wrapAndApplyHeader(buffer, offset + HEADER_LENGTH, messageHeaderDecoder);
        assertEquals(-8791026472627208190L, pendingMessageTrackerDecoder.nextServiceSessionId());
        assertEquals(-8791026472627208192L, pendingMessageTrackerDecoder.logServiceSessionId());
        assertEquals(capacity, pendingMessageTrackerDecoder.pendingMessageCapacity());
        assertEquals(serviceId, pendingMessageTrackerDecoder.serviceId());
    }

    @Test
    void snapshotPendingServiceMessageTrackerWithServiceMessagesMissedByFollower()
    {
        final int serviceId = 6;
        final PendingServiceMessageTracker pendingServiceMessageTracker = new PendingServiceMessageTracker(
            serviceId, mock(Counter.class), mock(LogPublisher.class), mock(ClusterClock.class));
        final AtomicBuffer headerMessageBuffer = new UnsafeBuffer(new byte[1024]);

        final long expectedLogServiceSessionId = pendingServiceMessageTracker.logServiceSessionId() + 1;
        final long expectedNextServiceSessionId = expectedLogServiceSessionId + 1;
        when(publication.tryClaim(anyInt(), any())).thenAnswer(
            (invocation) ->
            {
                final int length = invocation.getArgument(0, Integer.class);
                final BufferClaim bufferClaim = invocation.getArgument(1, BufferClaim.class);

                bufferClaim.wrap(headerMessageBuffer, 0, length + 32);
                return (long)length;
            });

        pendingServiceMessageTracker.sweepFollowerMessages(expectedLogServiceSessionId);

        snapshotTaker.snapshot(pendingServiceMessageTracker, mock(ErrorHandler.class));

        pendingMessageTrackerDecoder.wrapAndApplyHeader(headerMessageBuffer, HEADER_LENGTH, messageHeaderDecoder);
        assertEquals(expectedNextServiceSessionId, pendingMessageTrackerDecoder.nextServiceSessionId());
        assertEquals(expectedLogServiceSessionId, pendingMessageTrackerDecoder.logServiceSessionId());
    }

    @Test
    void snapshotTimer()
    {
        final int offset = 18;
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + TimerEncoder.BLOCK_LENGTH;
        final long correlationId = -901;
        final long deadline = 12345678901L;
        when(publication.tryClaim(eq(length), any()))
            .thenReturn(BACK_PRESSURED, ADMIN_ACTION)
            .thenAnswer(mockTryClaim(offset));

        snapshotTaker.snapshotTimer(correlationId, deadline);

        final InOrder inOrder = inOrder(idleStrategy, publication);
        inOrder.verify(idleStrategy).reset();
        inOrder.verify(publication).tryClaim(anyInt(), any());
        inOrder.verify(idleStrategy).idle();
        inOrder.verify(publication).tryClaim(anyInt(), any());
        inOrder.verify(idleStrategy).idle();
        inOrder.verify(publication).tryClaim(anyInt(), any());
        inOrder.verifyNoMoreInteractions();

        timerDecoder.wrapAndApplyHeader(buffer, offset + HEADER_LENGTH, messageHeaderDecoder);
        assertEquals(correlationId, timerDecoder.correlationId());
        assertEquals(deadline, timerDecoder.deadline());
    }

    @Test
    void snapshotSessionShouldUseTryClaimIfDataFitsIntoMaxPayloadSize()
    {
        final int offset = 10;
        final String responseChannel = "aeron:ipc";
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ClusterSessionEncoder.BLOCK_LENGTH +
            ClusterSessionEncoder.responseChannelHeaderLength() + responseChannel.length();
        final ClusterSession clusterSession = new ClusterSession(556, 42, responseChannel);
        clusterSession.loadSnapshotState(
            13, 1024, 800, CloseReason.CLIENT_ACTION);
        when(publication.maxPayloadLength()).thenReturn(length);
        when(publication.tryClaim(eq(length), any()))
            .thenReturn(BACK_PRESSURED, ADMIN_ACTION)
            .thenAnswer(mockTryClaim(offset));

        snapshotTaker.snapshotSession(clusterSession);

        final InOrder inOrder = inOrder(idleStrategy, publication);
        inOrder.verify(publication).maxPayloadLength();
        inOrder.verify(idleStrategy).reset();
        inOrder.verify(publication).tryClaim(anyInt(), any());
        inOrder.verify(idleStrategy).idle();
        inOrder.verify(publication).tryClaim(anyInt(), any());
        inOrder.verify(idleStrategy).idle();
        inOrder.verify(publication).tryClaim(anyInt(), any());
        inOrder.verifyNoMoreInteractions();

        clusterSessionDecoder.wrapAndApplyHeader(buffer, offset + HEADER_LENGTH, messageHeaderDecoder);
        assertEquals(clusterSession.id(), clusterSessionDecoder.clusterSessionId());
        assertEquals(clusterSession.correlationId(), clusterSessionDecoder.correlationId());
        assertEquals(clusterSession.openedLogPosition(), clusterSessionDecoder.openedLogPosition());
        assertEquals(Aeron.NULL_VALUE, clusterSessionDecoder.timeOfLastActivity());
        assertEquals(clusterSession.closeReason(), clusterSessionDecoder.closeReason());
        assertEquals(clusterSession.responseStreamId(), clusterSessionDecoder.responseStreamId());
        assertEquals(responseChannel, clusterSessionDecoder.responseChannel());
    }

    @Test
    void snapshotSessionShouldUseOfferIfDataDoesNotFitIntoMaxPayloadSize()
    {
        final String responseChannel = "aeron:ipc?alias=very very very long string|mtu=4444";
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ClusterSessionEncoder.BLOCK_LENGTH +
            ClusterSessionEncoder.responseChannelHeaderLength() + responseChannel.length();
        final ClusterSession clusterSession = new ClusterSession(42, 4, responseChannel);
        clusterSession.loadSnapshotState(
            -1, 76, 98, CloseReason.TIMEOUT);

        when(publication.maxPayloadLength()).thenReturn(length - 1);
        when(publication.offer(any(), eq(0), eq(length)))
            .thenReturn(BACK_PRESSURED, ADMIN_ACTION)
            .thenAnswer(mockOffer());

        snapshotTaker.snapshotSession(clusterSession);

        final InOrder inOrder = inOrder(idleStrategy, publication);
        inOrder.verify(publication).maxPayloadLength();
        inOrder.verify(idleStrategy).reset();
        inOrder.verify(publication).offer(any(), anyInt(), anyInt());
        inOrder.verify(idleStrategy).idle();
        inOrder.verify(publication).offer(any(), anyInt(), anyInt());
        inOrder.verify(idleStrategy).idle();
        inOrder.verify(publication).offer(any(), anyInt(), anyInt());
        inOrder.verifyNoMoreInteractions();

        clusterSessionDecoder.wrapAndApplyHeader(buffer, 0, messageHeaderDecoder);
        assertEquals(clusterSession.id(), clusterSessionDecoder.clusterSessionId());
        assertEquals(clusterSession.correlationId(), clusterSessionDecoder.correlationId());
        assertEquals(clusterSession.openedLogPosition(), clusterSessionDecoder.openedLogPosition());
        assertEquals(Aeron.NULL_VALUE, clusterSessionDecoder.timeOfLastActivity());
        assertEquals(clusterSession.closeReason(), clusterSessionDecoder.closeReason());
        assertEquals(clusterSession.responseStreamId(), clusterSessionDecoder.responseStreamId());
        assertEquals(responseChannel, clusterSessionDecoder.responseChannel());
    }

    private Answer<Long> mockTryClaim(final int offset)
    {
        return (invocation) ->
        {
            final int length = invocation.getArgument(0);
            final BufferClaim bufferClaim = invocation.getArgument(1);
            final int frameLength = length + HEADER_LENGTH;
            final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
            bufferClaim.wrap(buffer, offset, alignedFrameLength);
            return 1024L;
        };
    }

    private Answer<Long> mockOffer()
    {
        return (invocation) ->
        {
            final DirectBuffer srcBuffer = invocation.getArgument(0);
            final int srcOffset = invocation.getArgument(1);
            final int length = invocation.getArgument(2);
            buffer.putBytes(0, srcBuffer, srcOffset, length);
            return 128L;
        };
    }
}
