/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.common.concurrent.logbuffer;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static java.lang.Integer.valueOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.agrona.BitUtil.align;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.common.protocol.HeaderFlyweight.HDR_TYPE_DATA;

public class LogScannerTest
{
    private static final int TERM_BUFFER_CAPACITY = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int META_DATA_BUFFER_CAPACITY = TERM_META_DATA_LENGTH;
    private static final int MTU_LENGTH = 1024;
    private static final int HEADER_LENGTH = 32;

    private final UnsafeBuffer termBuffer = mock(UnsafeBuffer.class);
    private final UnsafeBuffer metaDataBuffer = mock(UnsafeBuffer.class);
    private final LogScanner.AvailabilityHandler handler = mock(LogScanner.AvailabilityHandler.class);

    private LogScanner scanner;

    @Before
    public void setUp()
    {
        when(termBuffer.capacity()).thenReturn(TERM_BUFFER_CAPACITY);
        when(metaDataBuffer.capacity()).thenReturn(META_DATA_BUFFER_CAPACITY);

        scanner = new LogScanner(termBuffer, metaDataBuffer, HEADER_LENGTH);
    }

    @Test
    public void shouldReportUnderlyingCapacity()
    {
        assertThat(scanner.capacity(), is(TERM_BUFFER_CAPACITY));
    }

    @Test
    public void shouldReturnZeroOnEmptyLog()
    {
        assertThat(scanner.scanNext(handler, MTU_LENGTH), is(0));
    }

    @Test
    public void shouldScanSingleMessage()
    {
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int frameOffset = 0;

        when(metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET))
            .thenReturn(alignedFrameLength);
        when(termBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(frameLength);
        when(termBuffer.getShort(typeOffset(frameOffset)))
            .thenReturn((short)HDR_TYPE_DATA);

        assertThat(scanner.scanNext(handler, MTU_LENGTH), greaterThan(0));
        assertFalse(scanner.isComplete());

        final InOrder inOrder = inOrder(metaDataBuffer, termBuffer, handler);
        inOrder.verify(termBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));
        inOrder.verify(handler).onAvailable(termBuffer, frameOffset, alignedFrameLength);
    }

    @Test
    public void shouldFailToScanMessageLargerThanMaxLength()
    {
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int maxLength = alignedFrameLength - 1;
        final int frameOffset = 0;

        when(metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET))
            .thenReturn(alignedFrameLength);
        when(termBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(frameLength);
        when(termBuffer.getShort(typeOffset(frameOffset)))
            .thenReturn((short)HDR_TYPE_DATA);

        assertThat(scanner.scanNext(handler, maxLength), is(0));
        assertFalse(scanner.isComplete());

        final InOrder inOrder = inOrder(metaDataBuffer, termBuffer, handler);
        inOrder.verify(termBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));
        inOrder.verify(handler, never()).onAvailable(any(), anyInt(), anyInt());
    }

    @Test
    public void shouldScanTwoMessagesThatFitInSingleMtu()
    {
        final int msgLength = 100;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        int frameOffset = 0;

        when(metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET))
            .thenReturn(alignedFrameLength * 2);
        when(termBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(frameLength);
        when(termBuffer.getShort(typeOffset(frameOffset)))
            .thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(lengthOffset(frameOffset + alignedFrameLength)))
            .thenReturn(alignedFrameLength);
        when(termBuffer.getShort(typeOffset(frameOffset + alignedFrameLength)))
            .thenReturn((short)HDR_TYPE_DATA);

        assertThat(scanner.scanNext(handler, MTU_LENGTH), greaterThan(0));
        assertFalse(scanner.isComplete());

        final InOrder inOrder = inOrder(metaDataBuffer, termBuffer, handler);
        inOrder.verify(termBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));

        frameOffset += alignedFrameLength;
        inOrder.verify(termBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));

        inOrder.verify(handler).onAvailable(termBuffer, 0, alignedFrameLength * 2);
    }

    @Test
    public void shouldScanTwoMessagesAndStopAtMtuBoundary()
    {
        final int frameTwoLength = align(HEADER_LENGTH + 1, FRAME_ALIGNMENT);
        final int frameOneLength = MTU_LENGTH - frameTwoLength;

        int frameOffset = 0;

        when(metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET))
            .thenReturn(frameOneLength + frameTwoLength);
        when(termBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(frameOneLength);
        when(termBuffer.getShort(typeOffset(frameOffset)))
            .thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(lengthOffset(frameOffset + frameOneLength)))
            .thenReturn(frameTwoLength);
        when(termBuffer.getShort(typeOffset(frameOffset + frameOneLength)))
            .thenReturn((short)HDR_TYPE_DATA);

        assertThat(scanner.scanNext(handler, MTU_LENGTH), greaterThan(0));
        assertFalse(scanner.isComplete());

        final InOrder inOrder = inOrder(metaDataBuffer, termBuffer, handler);
        inOrder.verify(termBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));

        frameOffset += frameOneLength;
        inOrder.verify(termBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));

        inOrder.verify(handler).onAvailable(termBuffer, 0, frameOneLength + frameTwoLength);
    }

    @Test
    public void shouldScanTwoMessagesAndStopAtSecondThatSpansMtu()
    {
        final int frameTwoLength = align(HEADER_LENGTH * 2, FRAME_ALIGNMENT);
        final int frameOneLength = MTU_LENGTH - (frameTwoLength / 2);
        int frameOffset = 0;

        when(metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET))
            .thenReturn(frameOneLength + frameTwoLength);
        when(termBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(frameOneLength);
        when(termBuffer.getShort(typeOffset(frameOffset)))
            .thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(lengthOffset(frameOffset + frameOneLength)))
            .thenReturn(frameTwoLength);
        when(termBuffer.getShort(typeOffset(frameOffset + frameOneLength)))
            .thenReturn((short)HDR_TYPE_DATA);

        assertThat(scanner.scanNext(handler, MTU_LENGTH), greaterThan(0));
        assertFalse(scanner.isComplete());

        final InOrder inOrder = inOrder(metaDataBuffer, termBuffer, handler);
        inOrder.verify(termBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));

        frameOffset += frameOneLength;
        inOrder.verify(termBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(termBuffer).getShort(typeOffset(frameOffset));

        inOrder.verify(handler).onAvailable(termBuffer, 0, frameOneLength);
    }

    @Test
    public void shouldScanLastFrameInBuffer()
    {
        final int alignedFrameLength = align(HEADER_LENGTH * 2, FRAME_ALIGNMENT);
        final int frameOffset = TERM_BUFFER_CAPACITY - alignedFrameLength;

        when(metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET))
            .thenReturn(TERM_BUFFER_CAPACITY);
        when(termBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(alignedFrameLength);
        when(termBuffer.getShort(typeOffset(frameOffset)))
            .thenReturn((short)HDR_TYPE_DATA);

        scanner.seek(frameOffset);

        assertThat(scanner.scanNext(handler, MTU_LENGTH), greaterThan(0));
        assertTrue(scanner.isComplete());
        assertThat(scanner.scanNext(handler, MTU_LENGTH), is(0));

        verify(handler).onAvailable(termBuffer, frameOffset, alignedFrameLength);
    }

    @Test
    public void shouldScanLastMessageInBufferPlusPadding()
    {
        final int alignedFrameLength = align(HEADER_LENGTH, FRAME_ALIGNMENT);
        final int frameOffset = TERM_BUFFER_CAPACITY - align(HEADER_LENGTH * 3, FRAME_ALIGNMENT);

        when(metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET))
            .thenReturn(TERM_BUFFER_CAPACITY - FRAME_ALIGNMENT);
        when(valueOf(termBuffer.getIntVolatile(lengthOffset(frameOffset))))
            .thenReturn(alignedFrameLength);
        when(termBuffer.getShort(typeOffset(frameOffset)))
            .thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(lengthOffset(frameOffset + alignedFrameLength)))
            .thenReturn(alignedFrameLength * 2);
        when(termBuffer.getShort(typeOffset(frameOffset + alignedFrameLength)))
            .thenReturn((short)PADDING_FRAME_TYPE);

        scanner.seek(frameOffset);

        assertThat(scanner.scanNext(handler, MTU_LENGTH), greaterThan(0));
        assertTrue(scanner.isComplete());
        assertThat(scanner.scanNext(handler, MTU_LENGTH), is(0));

        verify(handler, times(1)).onAvailable(termBuffer, frameOffset, alignedFrameLength * 2);
    }

    @Test
    public void shouldScanLastMessageInBufferMinusPaddingLimitedByMtu()
    {
        final int alignedFrameLength = align(HEADER_LENGTH, FRAME_ALIGNMENT);
        final int frameOffset = TERM_BUFFER_CAPACITY - align(HEADER_LENGTH * 3, FRAME_ALIGNMENT);
        final int mtu = alignedFrameLength + 8;

        when(metaDataBuffer.getIntVolatile(TERM_TAIL_COUNTER_OFFSET))
            .thenReturn(TERM_BUFFER_CAPACITY - FRAME_ALIGNMENT);
        when(valueOf(termBuffer.getIntVolatile(lengthOffset(frameOffset))))
            .thenReturn(alignedFrameLength);
        when(termBuffer.getShort(typeOffset(frameOffset)))
            .thenReturn((short)HDR_TYPE_DATA);
        when(termBuffer.getIntVolatile(lengthOffset(frameOffset + alignedFrameLength)))
            .thenReturn(alignedFrameLength * 2);
        when(termBuffer.getShort(typeOffset(frameOffset + alignedFrameLength)))
            .thenReturn((short)PADDING_FRAME_TYPE);

        scanner.seek(frameOffset);

        assertThat(scanner.scanNext(handler, mtu), is(alignedFrameLength));
        assertTrue(!scanner.isComplete());
        assertThat(scanner.remaining(), greaterThan(0));

        verify(handler, times(1)).onAvailable(termBuffer, frameOffset, alignedFrameLength);
        verifyZeroInteractions(handler);
    }
}
