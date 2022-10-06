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
package io.aeron;

import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.ReadablePosition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.stubbing.Answer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static io.aeron.Publication.*;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PublicationTest
{
    private static final String CHANNEL = "aeron:udp?endpoint=localhost:40124";
    private static final int STREAM_ID_1 = 1002;
    private static final int SESSION_ID_1 = 13;
    private static final int TERM_ID_1 = 1;
    private static final int CORRELATION_ID = 2000;
    private static final int SEND_BUFFER_CAPACITY = 1024;
    private static final int PARTITION_INDEX = 0;
    private static final int MTU_LENGTH = 4096;
    private static final int PAGE_SIZE = 4 * 1024;
    private static final int TERM_LENGTH = TERM_MIN_LENGTH * 4;
    private static final int POSITION_BITS_TO_SHIFT = LogBufferDescriptor.positionBitsToShift(TERM_LENGTH);
    private static final int DEFAULT_FRAME_TYPE = DataHeaderFlyweight.HDR_TYPE_RTTM;
    private static final int SESSION_ID = 42;
    private static final int STREAM_ID = 111;
    private final BufferClaim bufferClaim = new BufferClaim();
    private final ByteBuffer sendBuffer = allocate(SEND_BUFFER_CAPACITY);
    private final UnsafeBuffer atomicSendBuffer = new UnsafeBuffer(sendBuffer);
    private final UnsafeBuffer logMetaDataBuffer = spy(new UnsafeBuffer(allocate(LOG_META_DATA_LENGTH)));
    private final UnsafeBuffer[] termBuffers = new UnsafeBuffer[PARTITION_COUNT];
    private final ClientConductor conductor = mock(ClientConductor.class);
    private final LogBuffers logBuffers = mock(LogBuffers.class);
    private final ReadablePosition publicationLimit = mock(ReadablePosition.class);
    private ConcurrentPublication publication;

    @BeforeEach
    void setUp()
    {
        when(publicationLimit.getVolatile()).thenReturn(2L * SEND_BUFFER_CAPACITY);
        when(logBuffers.duplicateTermBuffers()).thenReturn(termBuffers);
        when(logBuffers.termLength()).thenReturn(TERM_LENGTH);
        when(logBuffers.metaDataBuffer()).thenReturn(logMetaDataBuffer);

        final UnsafeBuffer defaultHeader = DataHeaderFlyweight.createDefaultHeader(SESSION_ID, STREAM_ID, TERM_ID_1);
        defaultHeader.putShort(DataHeaderFlyweight.TYPE_FIELD_OFFSET, (short)DEFAULT_FRAME_TYPE, LITTLE_ENDIAN);
        storeDefaultFrameHeader(logMetaDataBuffer, defaultHeader);

        initialTermId(logMetaDataBuffer, TERM_ID_1);
        mtuLength(logMetaDataBuffer, MTU_LENGTH);
        termLength(logMetaDataBuffer, TERM_LENGTH);
        pageSize(logMetaDataBuffer, PAGE_SIZE);
        isConnected(logMetaDataBuffer, false);

        for (int i = 0; i < PARTITION_COUNT; i++)
        {
            termBuffers[i] = new UnsafeBuffer(allocate(TERM_LENGTH));
        }

        publication = new ConcurrentPublication(
            conductor,
            CHANNEL,
            STREAM_ID_1,
            SESSION_ID_1,
            publicationLimit,
            ChannelEndpointStatus.NO_ID_ALLOCATED,
            logBuffers,
            CORRELATION_ID,
            CORRELATION_ID);

        initialiseTailWithTermId(logMetaDataBuffer, PARTITION_INDEX, TERM_ID_1);

        doAnswer(
            (invocation) ->
            {
                publication.internalClose();
                return null;
            }).when(conductor).removePublication(publication);
    }

    @Test
    void shouldEnsureThePublicationIsOpenBeforeReadingPosition()
    {
        publication.close();
        assertEquals(CLOSED, publication.position());

        verify(conductor).removePublication(publication);
    }

    @Test
    void shouldEnsureThePublicationIsOpenBeforeOffer()
    {
        publication.close();
        assertTrue(publication.isClosed());
        assertEquals(CLOSED, publication.offer(atomicSendBuffer));
    }

    @Test
    void shouldEnsureThePublicationIsOpenBeforeClaim()
    {
        publication.close();
        final BufferClaim bufferClaim = new BufferClaim();
        assertEquals(CLOSED, publication.tryClaim(SEND_BUFFER_CAPACITY, bufferClaim));
    }

    @Test
    void shouldReportThatPublicationHasNotBeenConnectedYet()
    {
        when(publicationLimit.getVolatile()).thenReturn(0L);
        isConnected(logMetaDataBuffer, false);
        assertFalse(publication.isConnected());
    }

    @Test
    void shouldReportThatPublicationHasBeenConnectedYet()
    {
        isConnected(logMetaDataBuffer, true);
        assertTrue(publication.isConnected());
    }

    @Test
    void shouldReportInitialPosition()
    {
        assertEquals(0L, publication.position());
    }

    @Test
    void shouldReportMaxMessageLength()
    {
        assertEquals(FrameDescriptor.computeMaxMessageLength(TERM_LENGTH), publication.maxMessageLength());
    }

    @Test
    void shouldRemovePublicationOnClose()
    {
        publication.close();

        verify(conductor).removePublication(publication);
    }

    @Test
    void shouldReturnErrorMessages()
    {
        assertEquals("NOT_CONNECTED", errorString(-1L));
        assertEquals("BACK_PRESSURED", errorString(-2L));
        assertEquals("ADMIN_ACTION", errorString(-3L));
        assertEquals("CLOSED", errorString(-4L));
        assertEquals("MAX_POSITION_EXCEEDED", errorString(-5L));
        assertEquals("NONE", errorString(0L));
        assertEquals("NONE", errorString(1L));
        assertEquals("UNKNOWN", errorString(-6L));
        assertEquals("UNKNOWN", errorString(Long.MIN_VALUE));
    }

    @Test
    void tryClaimReturnsClosedIfPublicationWasClosed()
    {
        publication.close();

        assertEquals(CLOSED, publication.tryClaim(1, bufferClaim));

        assertSpaceNotClaimed();
    }

    @Test
    void tryClaimReturnsAdminActionIfTermCountAndTermIdDoNotMatch()
    {
        final int termCount = 9;
        assertEquals(indexByTermCount(termCount), PARTITION_INDEX);
        initialiseTailWithTermId(logMetaDataBuffer, PARTITION_INDEX, TERM_ID_1 + 5);

        assertEquals(ADMIN_ACTION, publication.tryClaim(1, bufferClaim));

        assertSpaceNotClaimed();
    }

    @Test
    void tryClaimReturnsMaxPositionExceededIfPublicationLimitReached()
    {
        when(publicationLimit.getVolatile()).thenReturn(16L);
        rawTail(logMetaDataBuffer, 1, packTail(Integer.MIN_VALUE, TERM_LENGTH - 128));
        activeTermCount(logMetaDataBuffer, Integer.MAX_VALUE);
        isConnected(logMetaDataBuffer, true);

        assertEquals(MAX_POSITION_EXCEEDED, publication.tryClaim(128, bufferClaim));

        assertSpaceNotClaimed();
    }

    @Test
    void tryClaimReturnsBackPressuredIfPublicationLimitReached()
    {
        when(publicationLimit.getVolatile()).thenReturn(16L);
        initialiseTailWithTermId(logMetaDataBuffer, 1, TERM_ID_1 + 1);
        activeTermCount(logMetaDataBuffer, 1);
        isConnected(logMetaDataBuffer, true);

        assertEquals(BACK_PRESSURED, publication.tryClaim(1, bufferClaim));

        assertSpaceNotClaimed();
    }

    @Test
    void tryClaimReturnsNotConnectedIfPublicationLimitReachedAndNotConnected()
    {
        when(publicationLimit.getVolatile()).thenReturn(16L);
        initialiseTailWithTermId(logMetaDataBuffer, 1, TERM_ID_1 + 1);
        activeTermCount(logMetaDataBuffer, 1);

        assertEquals(NOT_CONNECTED, publication.tryClaim(1, bufferClaim));

        assertSpaceNotClaimed();
    }

    @Test
    void tryClaimReturnsAdminActionIfTermWasRotated()
    {
        final int termOffset = TERM_LENGTH - 64;
        when(publicationLimit.getVolatile()).thenReturn(1L + Integer.MAX_VALUE);
        rawTailVolatile(logMetaDataBuffer, 0, packTail(TERM_ID_1, termOffset));
        rawTailVolatile(logMetaDataBuffer, 1, packTail(TERM_ID_1 + 1 - PARTITION_COUNT, 555));
        rawTailVolatile(logMetaDataBuffer, 2, packTail(STREAM_ID, 777));

        assertEquals(ADMIN_ACTION, publication.tryClaim(60, bufferClaim));

        assertSpaceNotClaimed();
        assertEquals(FrameDescriptor.PADDING_FRAME_TYPE, FrameDescriptor.frameType(termBuffers[0], termOffset));
        assertEquals(64, FrameDescriptor.frameLength(termBuffers[0], termOffset));
        assertEquals(packTail(TERM_ID_1, termOffset + 96), rawTail(logMetaDataBuffer, 0));
        assertEquals(packTail(TERM_ID_1 + 1, 0), rawTail(logMetaDataBuffer, 1));
        assertEquals(packTail(STREAM_ID, 777), rawTail(logMetaDataBuffer, 2));
    }

    @Test
    void tryClaimReturnsMaxPositionExceededIfThereIsNotEnoughSpaceLeft()
    {
        when(publicationLimit.getVolatile()).thenReturn(1L + Integer.MAX_VALUE);
        final int termOffset = TERM_LENGTH - 128;
        rawTail(logMetaDataBuffer, 1, packTail(Integer.MIN_VALUE, termOffset));
        rawTail(logMetaDataBuffer, 0, packTail(55, 55));
        rawTail(logMetaDataBuffer, 2, packTail(222, 222));
        activeTermCount(logMetaDataBuffer, Integer.MAX_VALUE);
        isConnected(logMetaDataBuffer, true);

        assertEquals(BACK_PRESSURED, publication.tryClaim(96, bufferClaim));

        assertSpaceNotClaimed();
        assertEquals(0, FrameDescriptor.frameLength(termBuffers[1], termOffset));
        assertEquals(packTail(Integer.MIN_VALUE, termOffset), rawTail(logMetaDataBuffer, 1));
        assertEquals(packTail(55, 55), rawTail(logMetaDataBuffer, 0));
        assertEquals(packTail(222, 222), rawTail(logMetaDataBuffer, 2));
    }

    @ParameterizedTest
    @MethodSource("positions")
    void tryClaimShouldReturnPositionAtWhichTheClaimedSpaceEnds(
        final int length,
        final int termId,
        final int termCount,
        final long tailAfterUpdate,
        final long expectedPosition)
    {
        when(publicationLimit.getVolatile()).thenReturn(Long.MAX_VALUE);
        isConnected(logMetaDataBuffer, true);
        activeTermCount(logMetaDataBuffer, termCount);
        final int index = indexByTermCount(termCount);
        final long rawTail = packTail(termId, 192);
        rawTail(logMetaDataBuffer, index, rawTail);
        final int frameLength = length + HEADER_LENGTH;
        final int totalLength = BitUtil.align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
        doAnswer((Answer<Long>)invocation -> tailAfterUpdate)
            .when(logMetaDataBuffer)
            .getAndAddLong(TERM_TAIL_COUNTERS_OFFSET + (index * SIZE_OF_LONG), totalLength);

        final long position = publication.tryClaim(length, bufferClaim);

        assertEquals(expectedPosition, position);
        assertEquals(length, bufferClaim.length());
        final UnsafeBuffer buffer = termBuffers[index];
        final int termOffset = termOffset(tailAfterUpdate);
        assertEquals(DEFAULT_FRAME_TYPE, FrameDescriptor.frameType(buffer, termOffset));
        assertEquals(-frameLength, FrameDescriptor.frameLength(buffer, termOffset));
        assertEquals(SESSION_ID, FrameDescriptor.frameSessionId(buffer, termOffset));
        assertEquals(STREAM_ID, DataHeaderFlyweight.streamId(buffer, termOffset));
    }

    private void assertSpaceNotClaimed()
    {
        final MutableDirectBuffer buffer = bufferClaim.buffer();
        assertEquals(0, buffer.capacity());
    }

    private static List<Arguments> positions()
    {
        return Arrays.asList(
            Arguments.of(
                100,
                31,
                30,
                packTail(31, 16 * 1024),
                computePosition(31, 16 * 1024 + 160, POSITION_BITS_TO_SHIFT, TERM_ID_1)),
            Arguments.of(
                999,
                7,
                6,
                packTail(212, 0),
                computePosition(212, 1056, POSITION_BITS_TO_SHIFT, TERM_ID_1)));
    }
}
