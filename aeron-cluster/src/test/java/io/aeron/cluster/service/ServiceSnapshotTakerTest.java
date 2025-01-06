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
package io.aeron.cluster.service;

import io.aeron.ExclusivePublication;
import io.aeron.cluster.codecs.ClientSessionDecoder;
import io.aeron.cluster.codecs.ClientSessionEncoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.Publication.ADMIN_ACTION;
import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.agrona.BitUtil.align;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class ServiceSnapshotTakerTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ClientSessionDecoder clientSessionDecoder = new ClientSessionDecoder();
    private final ExclusivePublication publication = mock(ExclusivePublication.class);
    private final IdleStrategy idleStrategy = mock(IdleStrategy.class);
    private final ServiceSnapshotTaker serviceSnapshotTaker = new ServiceSnapshotTaker(publication, idleStrategy, null);

    @Test
    void snapshotSessionUsesTryClaimIfDataFitIntoMaxPayloadSize()
    {
        final int offset = 40;
        final String responseChannel = "aeron:udp?endpoint=localhost:8080";
        final byte[] encodedPrincipal = new byte[100];
        ThreadLocalRandom.current().nextBytes(encodedPrincipal);
        final ContainerClientSession session =
            new ContainerClientSession(42, 8, responseChannel, encodedPrincipal, null);
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ClientSessionEncoder.BLOCK_LENGTH +
            ClientSessionEncoder.responseChannelHeaderLength() + responseChannel.length() +
            ClientSessionEncoder.encodedPrincipalHeaderLength() + encodedPrincipal.length;
        when(publication.maxPayloadLength()).thenReturn(length);
        when(publication.tryClaim(eq(length), any()))
            .thenReturn(BACK_PRESSURED)
            .thenAnswer(mockTryClaim(offset));

        serviceSnapshotTaker.snapshotSession(session);

        final InOrder inOrder = inOrder(idleStrategy, publication);
        inOrder.verify(publication).maxPayloadLength();
        inOrder.verify(idleStrategy).reset();
        inOrder.verify(publication).tryClaim(anyInt(), any());
        inOrder.verify(idleStrategy).idle();
        inOrder.verify(publication).tryClaim(anyInt(), any());
        inOrder.verifyNoMoreInteractions();

        clientSessionDecoder.wrapAndApplyHeader(buffer, offset + HEADER_LENGTH, messageHeaderDecoder);
        assertEquals(session.id(), clientSessionDecoder.clusterSessionId());
        assertEquals(session.responseStreamId(), clientSessionDecoder.responseStreamId());
        assertEquals(responseChannel, clientSessionDecoder.responseChannel());
        assertEquals(encodedPrincipal.length, clientSessionDecoder.encodedPrincipalLength());
        final byte[] snapshotPrincipal = new byte[encodedPrincipal.length];
        clientSessionDecoder.getEncodedPrincipal(snapshotPrincipal, 0, snapshotPrincipal.length);
        assertArrayEquals(encodedPrincipal, snapshotPrincipal);
    }

    @Test
    void snapshotSessionUsesOfferIfDataDoesNotIntoMaxPayloadSize()
    {
        final String responseChannel = "aeron:udp?endpoint=localhost:8080|alias=long time ago";
        final byte[] encodedPrincipal = new byte[1000];
        ThreadLocalRandom.current().nextBytes(encodedPrincipal);
        final ContainerClientSession session =
            new ContainerClientSession(8, -3, responseChannel, encodedPrincipal, null);
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ClientSessionEncoder.BLOCK_LENGTH +
            ClientSessionEncoder.responseChannelHeaderLength() + responseChannel.length() +
            ClientSessionEncoder.encodedPrincipalHeaderLength() + encodedPrincipal.length;
        when(publication.maxPayloadLength()).thenReturn(20);
        when(publication.offer(any(), eq(0), eq(length)))
            .thenReturn(BACK_PRESSURED, ADMIN_ACTION)
            .thenAnswer(mockOffer());

        serviceSnapshotTaker.snapshotSession(session);

        final InOrder inOrder = inOrder(idleStrategy, publication);
        inOrder.verify(publication).maxPayloadLength();
        inOrder.verify(idleStrategy).reset();
        inOrder.verify(publication).offer(any(), anyInt(), anyInt());
        inOrder.verify(idleStrategy).idle();
        inOrder.verify(publication).offer(any(), anyInt(), anyInt());
        inOrder.verify(idleStrategy).idle();
        inOrder.verify(publication).offer(any(), anyInt(), anyInt());
        inOrder.verifyNoMoreInteractions();

        clientSessionDecoder.wrapAndApplyHeader(buffer, 0, messageHeaderDecoder);
        assertEquals(session.id(), clientSessionDecoder.clusterSessionId());
        assertEquals(session.responseStreamId(), clientSessionDecoder.responseStreamId());
        assertEquals(responseChannel, clientSessionDecoder.responseChannel());
        assertEquals(encodedPrincipal.length, clientSessionDecoder.encodedPrincipalLength());
        final byte[] snapshotPrincipal = new byte[encodedPrincipal.length];
        clientSessionDecoder.getEncodedPrincipal(snapshotPrincipal, 0, snapshotPrincipal.length);
        assertArrayEquals(encodedPrincipal, snapshotPrincipal);
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
