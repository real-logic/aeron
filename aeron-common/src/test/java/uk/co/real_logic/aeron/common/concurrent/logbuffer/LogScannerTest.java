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
    private static final int LOG_BUFFER_CAPACITY = LogBufferDescriptor.MIN_LOG_SIZE;
    private static final int STATE_BUFFER_CAPACITY = STATE_BUFFER_LENGTH;
    private static final int MTU_LENGTH = 1024;
    private static final int HEADER_LENGTH = 32;

    private final UnsafeBuffer logBuffer = mock(UnsafeBuffer.class);
    private final UnsafeBuffer stateBuffer = mock(UnsafeBuffer.class);
    private final LogScanner.AvailabilityHandler handler = mock(LogScanner.AvailabilityHandler.class);

    private LogScanner scanner;

    @Before
    public void setUp()
    {
        when(logBuffer.capacity()).thenReturn(LOG_BUFFER_CAPACITY);
        when(stateBuffer.capacity()).thenReturn(STATE_BUFFER_CAPACITY);

        scanner = new LogScanner(logBuffer, stateBuffer, HEADER_LENGTH);
    }

    @Test
    public void shouldReportUnderlyingCapacity()
    {
        assertThat(scanner.capacity(), is(LOG_BUFFER_CAPACITY));
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

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(alignedFrameLength);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(frameLength);
        when(logBuffer.getShort(typeOffset(frameOffset)))
            .thenReturn((short)HDR_TYPE_DATA);

        assertThat(scanner.scanNext(handler, MTU_LENGTH), greaterThan(0));
        assertFalse(scanner.isComplete());

        final InOrder inOrder = inOrder(stateBuffer, logBuffer, handler);
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset));
        inOrder.verify(handler).onAvailable(logBuffer, frameOffset, alignedFrameLength);
    }

    @Test
    public void shouldFailToScanMessageLargerThanMaxLength()
    {
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int maxLength = alignedFrameLength - 1;
        final int frameOffset = 0;

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(alignedFrameLength);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(frameLength);
        when(logBuffer.getShort(typeOffset(frameOffset)))
            .thenReturn((short)HDR_TYPE_DATA);

        assertThat(scanner.scanNext(handler, maxLength), is(0));
        assertFalse(scanner.isComplete());

        final InOrder inOrder = inOrder(stateBuffer, logBuffer, handler);
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset));
        inOrder.verify(handler, never()).onAvailable(any(), anyInt(), anyInt());
    }

    @Test
    public void shouldScanTwoMessagesThatFitInSingleMtu()
    {
        final int msgLength = 100;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        int frameOffset = 0;

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(alignedFrameLength * 2);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(frameLength);
        when(logBuffer.getShort(typeOffset(frameOffset)))
            .thenReturn((short)HDR_TYPE_DATA);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset + alignedFrameLength)))
            .thenReturn(alignedFrameLength);
        when(logBuffer.getShort(typeOffset(frameOffset + alignedFrameLength)))
            .thenReturn((short)HDR_TYPE_DATA);

        assertThat(scanner.scanNext(handler, MTU_LENGTH), greaterThan(0));
        assertFalse(scanner.isComplete());

        final InOrder inOrder = inOrder(stateBuffer, logBuffer, handler);
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset));

        frameOffset += alignedFrameLength;
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset));

        inOrder.verify(handler).onAvailable(logBuffer, 0, alignedFrameLength * 2);
    }

    @Test
    public void shouldScanTwoMessagesAndStopAtMtuBoundary()
    {
        final int frameTwoLength = align(HEADER_LENGTH + 1, FRAME_ALIGNMENT);
        final int frameOneLength = MTU_LENGTH - frameTwoLength;

        int frameOffset = 0;

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(frameOneLength + frameTwoLength);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(frameOneLength);
        when(logBuffer.getShort(typeOffset(frameOffset)))
            .thenReturn((short)HDR_TYPE_DATA);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset + frameOneLength)))
            .thenReturn(frameTwoLength);
        when(logBuffer.getShort(typeOffset(frameOffset + frameOneLength)))
            .thenReturn((short)HDR_TYPE_DATA);

        assertThat(scanner.scanNext(handler, MTU_LENGTH), greaterThan(0));
        assertFalse(scanner.isComplete());

        final InOrder inOrder = inOrder(stateBuffer, logBuffer, handler);
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset));

        frameOffset += frameOneLength;
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset));

        inOrder.verify(handler).onAvailable(logBuffer, 0, frameOneLength + frameTwoLength);
    }

    @Test
    public void shouldScanTwoMessagesAndStopAtSecondThatSpansMtu()
    {
        final int frameTwoLength = align(HEADER_LENGTH * 2, FRAME_ALIGNMENT);
        final int frameOneLength = MTU_LENGTH - (frameTwoLength / 2);
        int frameOffset = 0;

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(frameOneLength + frameTwoLength);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(frameOneLength);
        when(logBuffer.getShort(typeOffset(frameOffset)))
            .thenReturn((short)HDR_TYPE_DATA);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset + frameOneLength)))
            .thenReturn(frameTwoLength);
        when(logBuffer.getShort(typeOffset(frameOffset + frameOneLength)))
            .thenReturn((short)HDR_TYPE_DATA);

        assertThat(scanner.scanNext(handler, MTU_LENGTH), greaterThan(0));
        assertFalse(scanner.isComplete());

        final InOrder inOrder = inOrder(stateBuffer, logBuffer, handler);
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset));

        frameOffset += frameOneLength;
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset));

        inOrder.verify(handler).onAvailable(logBuffer, 0, frameOneLength);
    }

    @Test
    public void shouldScanLastFrameInBuffer()
    {
        final int alignedFrameLength = align(HEADER_LENGTH * 2, FRAME_ALIGNMENT);
        final int frameOffset = LOG_BUFFER_CAPACITY - alignedFrameLength;

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(LOG_BUFFER_CAPACITY);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(alignedFrameLength);
        when(logBuffer.getShort(typeOffset(frameOffset)))
            .thenReturn((short)HDR_TYPE_DATA);

        scanner.seek(frameOffset);

        assertThat(scanner.scanNext(handler, MTU_LENGTH), greaterThan(0));
        assertTrue(scanner.isComplete());
        assertThat(scanner.scanNext(handler, MTU_LENGTH), is(0));

        verify(handler).onAvailable(logBuffer, frameOffset, alignedFrameLength);
    }

    @Test
    public void shouldScanLastMessageInBufferPlusPadding()
    {
        final int alignedFrameLength = align(HEADER_LENGTH, FRAME_ALIGNMENT);
        final int frameOffset = LOG_BUFFER_CAPACITY - align(HEADER_LENGTH * 3, FRAME_ALIGNMENT);

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(LOG_BUFFER_CAPACITY - FRAME_ALIGNMENT);
        when(valueOf(logBuffer.getIntVolatile(lengthOffset(frameOffset))))
            .thenReturn(alignedFrameLength);
        when(logBuffer.getShort(typeOffset(frameOffset)))
            .thenReturn((short)HDR_TYPE_DATA);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset + alignedFrameLength)))
            .thenReturn(alignedFrameLength * 2);
        when(logBuffer.getShort(typeOffset(frameOffset + alignedFrameLength)))
            .thenReturn((short)PADDING_FRAME_TYPE);

        scanner.seek(frameOffset);

        assertThat(scanner.scanNext(handler, MTU_LENGTH), greaterThan(0));
        assertTrue(scanner.isComplete());
        assertThat(scanner.scanNext(handler, MTU_LENGTH), is(0));

        verify(handler, times(1)).onAvailable(logBuffer, frameOffset, alignedFrameLength * 2);
    }

    @Test
    public void shouldScanLastMessageInBufferMinusPaddingLimitedByMtu()
    {
        final int alignedFrameLength = align(HEADER_LENGTH, FRAME_ALIGNMENT);
        final int frameOffset = LOG_BUFFER_CAPACITY - align(HEADER_LENGTH * 3, FRAME_ALIGNMENT);
        final int mtu = alignedFrameLength + 8;

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(LOG_BUFFER_CAPACITY - FRAME_ALIGNMENT);
        when(valueOf(logBuffer.getIntVolatile(lengthOffset(frameOffset))))
            .thenReturn(alignedFrameLength);
        when(logBuffer.getShort(typeOffset(frameOffset)))
            .thenReturn((short)HDR_TYPE_DATA);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset + alignedFrameLength)))
            .thenReturn(alignedFrameLength * 2);
        when(logBuffer.getShort(typeOffset(frameOffset + alignedFrameLength)))
            .thenReturn((short)PADDING_FRAME_TYPE);

        scanner.seek(frameOffset);

        assertThat(scanner.scanNext(handler, mtu), is(alignedFrameLength));
        assertTrue(!scanner.isComplete());
        assertThat(scanner.remaining(), greaterThan(0));

        verify(handler, times(1)).onAvailable(logBuffer, frameOffset, alignedFrameLength);
        verifyZeroInteractions(handler);
    }
}
