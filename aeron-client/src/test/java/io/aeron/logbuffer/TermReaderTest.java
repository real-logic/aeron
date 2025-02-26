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

import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.Position;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_DATA;
import static org.agrona.BitUtil.align;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class TermReaderTest
{
    private static final int TERM_BUFFER_CAPACITY = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int HEADER_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;
    private static final int INITIAL_TERM_ID = 7;
    private static final int POSITION_BITS_TO_SHIFT = LogBufferDescriptor.positionBitsToShift(TERM_BUFFER_CAPACITY);

    private final Header header = new Header(INITIAL_TERM_ID, TERM_BUFFER_CAPACITY);
    private final UnsafeBuffer termBuffer = mock(UnsafeBuffer.class);
    private final ErrorHandler errorHandler = mock(ErrorHandler.class);
    private final FragmentHandler handler = mock(FragmentHandler.class);
    private final Position subscriberPosition = mock(Position.class);

    @BeforeEach
    void setUp()
    {
        when(termBuffer.capacity()).thenReturn(TERM_BUFFER_CAPACITY);
    }

    @Test
    void shouldReadFirstMessage()
    {
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int termOffset = 0;

        when(termBuffer.getIntVolatile(0)).thenReturn(frameLength);
        when(termBuffer.getShort(typeOffset(0))).thenReturn((short)HDR_TYPE_DATA);

        final int readOutcome = TermReader.read(
            termBuffer, termOffset, handler, Integer.MAX_VALUE, header, errorHandler, 0, subscriberPosition);
        assertEquals(1, readOutcome);

        final InOrder inOrder = inOrder(termBuffer, handler, subscriberPosition);
        inOrder.verify(termBuffer).getIntVolatile(0);
        inOrder.verify(handler).onFragment(eq(termBuffer), eq(HEADER_LENGTH), eq(msgLength), any(Header.class));
        inOrder.verify(subscriberPosition).setRelease(alignedFrameLength);
    }

    @Test
    void shouldNotReadPastTail()
    {
        final int termOffset = 0;

        final int readOutcome = TermReader.read(
            termBuffer, termOffset, handler, Integer.MAX_VALUE, header, errorHandler, 0, subscriberPosition);
        assertEquals(0, readOutcome);
        verify(subscriberPosition, never()).setRelease(anyLong());

        verify(termBuffer).getIntVolatile(0);
        verify(handler, never()).onFragment(any(), anyInt(), anyInt(), any());
    }

    @Test
    void shouldReadOneLimitedMessage()
    {
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int termOffset = 0;

        when(termBuffer.getIntVolatile(anyInt())).thenReturn(frameLength);
        when(termBuffer.getShort(anyInt())).thenReturn((short)HDR_TYPE_DATA);

        final int readOutcome = TermReader.read(
            termBuffer, termOffset, handler, 1, header, errorHandler, 0, subscriberPosition);
        assertEquals(1, readOutcome);

        final InOrder inOrder = inOrder(termBuffer, handler, subscriberPosition);
        inOrder.verify(termBuffer).getIntVolatile(0);
        inOrder.verify(handler).onFragment(eq(termBuffer), eq(HEADER_LENGTH), eq(msgLength), any(Header.class));
        inOrder.verify(subscriberPosition).setRelease(alignedFrameLength);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldReadMultipleMessages()
    {
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int termOffset = 0;

        when(termBuffer.getIntVolatile(0)).thenReturn(frameLength);
        when(termBuffer.getIntVolatile(alignedFrameLength)).thenReturn(frameLength);
        when(termBuffer.getShort(anyInt())).thenReturn((short)HDR_TYPE_DATA);

        final int readOutcome = TermReader.read(
            termBuffer, termOffset, handler, Integer.MAX_VALUE, header, errorHandler, 0, subscriberPosition);
        assertEquals(2, readOutcome);

        final InOrder inOrder = inOrder(termBuffer, handler, subscriberPosition);
        inOrder.verify(termBuffer).getIntVolatile(0);
        inOrder.verify(handler).onFragment(eq(termBuffer), eq(HEADER_LENGTH), eq(msgLength), any(Header.class));

        inOrder.verify(termBuffer).getIntVolatile(alignedFrameLength);
        inOrder
            .verify(handler)
            .onFragment(eq(termBuffer), eq(alignedFrameLength + HEADER_LENGTH), eq(msgLength), any(Header.class));
        inOrder.verify(subscriberPosition).setRelease(alignedFrameLength * 2L);
    }

    @Test
    void shouldReadLastMessage()
    {
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int frameOffset = TERM_BUFFER_CAPACITY - alignedFrameLength;
        final long startingPosition = LogBufferDescriptor.computePosition(
            INITIAL_TERM_ID, frameOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

        when(termBuffer.getIntVolatile(frameOffset)).thenReturn(frameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)HDR_TYPE_DATA);
        when(subscriberPosition.getVolatile()).thenReturn(startingPosition);

        final int readOutcome = TermReader.read(
            termBuffer,
            frameOffset,
            handler,
            Integer.MAX_VALUE,
            header,
            errorHandler,
            startingPosition,
            subscriberPosition);
        assertEquals(1, readOutcome);

        final InOrder inOrder = inOrder(termBuffer, handler, subscriberPosition);
        inOrder.verify(termBuffer).getIntVolatile(frameOffset);
        inOrder.verify(handler).onFragment(
            eq(termBuffer), eq(frameOffset + HEADER_LENGTH), eq(msgLength), any(Header.class));
        inOrder.verify(subscriberPosition).setRelease(TERM_BUFFER_CAPACITY);
    }

    @Test
    void shouldNotReadLastMessageWhenPadding()
    {
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int frameOffset = TERM_BUFFER_CAPACITY - alignedFrameLength;
        final long currentPosition = LogBufferDescriptor.computePosition(
            INITIAL_TERM_ID, frameOffset, POSITION_BITS_TO_SHIFT, INITIAL_TERM_ID);

        when(termBuffer.getIntVolatile(frameOffset)).thenReturn(frameLength);
        when(termBuffer.getShort(typeOffset(frameOffset))).thenReturn((short)PADDING_FRAME_TYPE);
        when(subscriberPosition.getVolatile()).thenReturn(currentPosition);

        final int readOutcome = TermReader.read(
            termBuffer,
            frameOffset,
            handler,
            Integer.MAX_VALUE,
            header,
            errorHandler,
            currentPosition,
            subscriberPosition);
        assertEquals(0, readOutcome);

        final InOrder inOrder = inOrder(termBuffer, subscriberPosition);
        inOrder.verify(termBuffer).getIntVolatile(frameOffset);
        verify(handler, never()).onFragment(any(), anyInt(), anyInt(), any());
        inOrder.verify(subscriberPosition).setRelease(TERM_BUFFER_CAPACITY);
    }
}
