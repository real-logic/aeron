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
package io.aeron.logbuffer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.concurrent.UnsafeBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_DATA;
import static org.agrona.BitUtil.align;

class TermScannerTest
{
    private static final int TERM_BUFFER_CAPACITY = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int MTU_LENGTH = 1024;
    private static final int HEADER_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;

    private final UnsafeBuffer termBuffer = mock(UnsafeBuffer.class);

    @BeforeEach
    void setUp()
    {
        when(termBuffer.capacity()).thenReturn(TERM_BUFFER_CAPACITY);
    }

    @Test
    void shouldPackPaddingAndOffsetIntoResultingStatus()
    {
        final int padding = 77;
        final int available = 65000;

        final long scanOutcome = TermScanner.pack(padding, available);

        assertEquals(padding, TermScanner.padding(scanOutcome));
        assertEquals(available, TermScanner.available(scanOutcome));
    }

    @Test
    void shouldReturnZeroOnEmptyLog()
    {
        final long scanOutcome = TermScanner.scanForAvailability(termBuffer, 0, MTU_LENGTH);
        assertEquals(0, TermScanner.available(scanOutcome));
        assertEquals(0, TermScanner.padding(scanOutcome));
    }

    @Test
    void shouldScanSingleMessage()
    {
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int frameOffset = 0;

        when(termBuffer.getIntVolatile(frameOffset)).thenReturn(frameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);

        final long scanOutcome = TermScanner.scanForAvailability(termBuffer, frameOffset, MTU_LENGTH);
        assertEquals(alignedFrameLength, TermScanner.available(scanOutcome));
        assertEquals(0, TermScanner.padding(scanOutcome));

        final InOrder inOrder = inOrder(termBuffer);
        inOrder.verify(termBuffer).getIntVolatile(frameOffset);
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));
    }

    @Test
    void shouldFailToScanMessageLargerThanMaxLength()
    {
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int maxLength = alignedFrameLength - 1;
        final int frameOffset = 0;

        when(termBuffer.getIntVolatile(frameOffset)).thenReturn(frameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);

        final long scanOutcome = TermScanner.scanForAvailability(termBuffer, frameOffset, maxLength);
        assertEquals(-alignedFrameLength, TermScanner.available(scanOutcome));
        assertEquals(-1, TermScanner.padding(scanOutcome));

        final InOrder inOrder = inOrder(termBuffer);
        inOrder.verify(termBuffer).getIntVolatile(frameOffset);
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));
    }

    @Test
    void shouldScanTwoMessagesThatFitInSingleMtu()
    {
        final int msgLength = 100;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        int frameOffset = 0;

        when(termBuffer.getIntVolatile(frameOffset)).thenReturn(frameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(frameOffset + alignedFrameLength)).thenReturn(alignedFrameLength);
        when(termBuffer.getShort(typeOffset(frameOffset + alignedFrameLength))).thenReturn((short)HDR_TYPE_DATA);

        final long scanOutcome = TermScanner.scanForAvailability(termBuffer, frameOffset, MTU_LENGTH);
        assertEquals(alignedFrameLength * 2, TermScanner.available(scanOutcome));
        assertEquals(0, TermScanner.padding(scanOutcome));

        final InOrder inOrder = inOrder(termBuffer);
        inOrder.verify(termBuffer).getIntVolatile(frameOffset);
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));

        frameOffset += alignedFrameLength;
        inOrder.verify(termBuffer).getIntVolatile(frameOffset);
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));
    }

    @Test
    void shouldScanTwoMessagesAndStopAtMtuBoundary()
    {
        final int frameTwoLength = align(HEADER_LENGTH + 1, FRAME_ALIGNMENT);
        final int frameOneLength = MTU_LENGTH - frameTwoLength;

        int frameOffset = 0;

        when(termBuffer.getIntVolatile(frameOffset)).thenReturn(frameOneLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(frameOffset + frameOneLength)).thenReturn(frameTwoLength);
        when(termBuffer.getShort(typeOffset(frameOffset + frameOneLength))).thenReturn((short)HDR_TYPE_DATA);

        final long scanOutcome = TermScanner.scanForAvailability(termBuffer, frameOffset, MTU_LENGTH);
        assertEquals(frameOneLength + frameTwoLength, TermScanner.available(scanOutcome));
        assertEquals(0, TermScanner.padding(scanOutcome));

        final InOrder inOrder = inOrder(termBuffer);
        inOrder.verify(termBuffer).getIntVolatile(frameOffset);
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));

        frameOffset += frameOneLength;
        inOrder.verify(termBuffer).getIntVolatile(frameOffset);
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));
    }

    @Test
    void shouldScanTwoMessagesAndStopAtSecondThatSpansMtu()
    {
        final int frameTwoLength = align(HEADER_LENGTH * 2, FRAME_ALIGNMENT);
        final int frameOneLength = MTU_LENGTH - (frameTwoLength / 2);
        int frameOffset = 0;

        when(termBuffer.getIntVolatile(frameOffset)).thenReturn(frameOneLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(frameOffset + frameOneLength)).thenReturn(frameTwoLength);
        when(termBuffer.getShort(typeOffset(frameOffset + frameOneLength))).thenReturn((short)HDR_TYPE_DATA);

        final long scanOutcome = TermScanner.scanForAvailability(termBuffer, frameOffset, MTU_LENGTH);
        assertEquals(frameOneLength, TermScanner.available(scanOutcome));
        assertEquals(0, TermScanner.padding(scanOutcome));

        final InOrder inOrder = inOrder(termBuffer);
        inOrder.verify(termBuffer).getIntVolatile(frameOffset);
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));

        frameOffset += frameOneLength;
        inOrder.verify(termBuffer).getIntVolatile(frameOffset);
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));
    }

    @Test
    void shouldScanLastFrameInBuffer()
    {
        final int alignedFrameLength = align(HEADER_LENGTH * 2, FRAME_ALIGNMENT);
        final int frameOffset = TERM_BUFFER_CAPACITY - alignedFrameLength;

        when(termBuffer.getIntVolatile(frameOffset)).thenReturn(alignedFrameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);

        final long scanOutcome = TermScanner.scanForAvailability(termBuffer, frameOffset, MTU_LENGTH);
        assertEquals(alignedFrameLength, TermScanner.available(scanOutcome));
        assertEquals(0, TermScanner.padding(scanOutcome));
    }

    @Test
    void shouldScanLastMessageInBufferPlusPadding()
    {
        final int alignedFrameLength = align(HEADER_LENGTH * 2, FRAME_ALIGNMENT);
        final int paddingFrameLength = align(HEADER_LENGTH * 3, FRAME_ALIGNMENT);
        final int frameOffset = TERM_BUFFER_CAPACITY - (alignedFrameLength + paddingFrameLength);

        when(termBuffer.getIntVolatile(frameOffset)).thenReturn(alignedFrameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(frameOffset + alignedFrameLength)).thenReturn(paddingFrameLength);
        when(termBuffer.getShort(typeOffset(frameOffset + alignedFrameLength))).thenReturn((short)PADDING_FRAME_TYPE);

        final long scanOutcome = TermScanner.scanForAvailability(termBuffer, frameOffset, MTU_LENGTH);
        assertEquals(alignedFrameLength + HEADER_LENGTH, TermScanner.available(scanOutcome));
        assertEquals(paddingFrameLength - HEADER_LENGTH, TermScanner.padding(scanOutcome));
    }

    @Test
    void shouldScanLastMessageInBufferMinusPaddingLimitedByMtu()
    {
        final int alignedFrameLength = align(HEADER_LENGTH, FRAME_ALIGNMENT);
        final int frameOffset = TERM_BUFFER_CAPACITY - align(HEADER_LENGTH * 3, FRAME_ALIGNMENT);
        final int mtu = alignedFrameLength + 8;

        when(termBuffer.getIntVolatile(frameOffset)).thenReturn(alignedFrameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(frameOffset + alignedFrameLength)).thenReturn(alignedFrameLength * 2);
        when(termBuffer.getShort(typeOffset(frameOffset + alignedFrameLength))).thenReturn((short)PADDING_FRAME_TYPE);

        final long scanOutcome = TermScanner.scanForAvailability(termBuffer, frameOffset, mtu);
        assertEquals(alignedFrameLength, TermScanner.available(scanOutcome));
        assertEquals(0, TermScanner.padding(scanOutcome));
    }
}