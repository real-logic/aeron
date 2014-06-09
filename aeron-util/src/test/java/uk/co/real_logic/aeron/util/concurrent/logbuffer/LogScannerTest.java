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
package uk.co.real_logic.aeron.util.concurrent.logbuffer;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import static java.lang.Integer.valueOf;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.util.BitUtil.align;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.util.protocol.HeaderFlyweight.HDR_TYPE_DATA;

public class LogScannerTest
{
    private static final int LOG_BUFFER_CAPACITY = 1024 * 16;
    private static final int STATE_BUFFER_CAPACITY = STATE_BUFFER_LENGTH;
    private static final int MTU_LENGTH = 1024;
    private static final int HEADER_LENGTH = 32;

    private final AtomicBuffer logBuffer = mock(AtomicBuffer.class);
    private final AtomicBuffer stateBuffer = mock(AtomicBuffer.class);
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
    public void shouldReturnFalseOnEmptyLog()
    {
        assertFalse(scanner.scanNext(MTU_LENGTH, handler));
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
        when(logBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN))
            .thenReturn((short)HDR_TYPE_DATA);

        assertTrue(scanner.scanNext(MTU_LENGTH, handler));
        assertFalse(scanner.isComplete());

        final InOrder inOrder = inOrder(stateBuffer, logBuffer, handler);
        inOrder.verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset), LITTLE_ENDIAN);
        inOrder.verify(handler).onAvailable(frameOffset, alignedFrameLength);
    }

    @Test
    public void shouldFailToScanMessageLargerThanMaxLength()
    {
        final int maxLength = FRAME_ALIGNMENT - 1;
        final int msgLength = 1;
        final int frameLength = HEADER_LENGTH + msgLength;
        final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);
        final int frameOffset = 0;

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(alignedFrameLength);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(frameLength);
        when(logBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN))
            .thenReturn((short)HDR_TYPE_DATA);

        assertFalse(scanner.scanNext(maxLength, handler));
        assertFalse(scanner.isComplete());

        final InOrder inOrder = inOrder(stateBuffer, logBuffer, handler);
        inOrder.verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset), LITTLE_ENDIAN);
        inOrder.verify(handler, never()).onAvailable(anyInt(), anyInt());
    }

    @Test
    public void shouldScanTwoMessagesThatFitInSingleMtu()
    {
        int frameOffset = 0;

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(FRAME_ALIGNMENT * 2);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(FRAME_ALIGNMENT);
        when(logBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN))
            .thenReturn((short)HDR_TYPE_DATA);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset + FRAME_ALIGNMENT)))
            .thenReturn(FRAME_ALIGNMENT);
        when(logBuffer.getShort(typeOffset(frameOffset + FRAME_ALIGNMENT), LITTLE_ENDIAN))
            .thenReturn((short)HDR_TYPE_DATA);

        assertTrue(scanner.scanNext(MTU_LENGTH, handler));
        assertFalse(scanner.isComplete());

        final InOrder inOrder = inOrder(stateBuffer, logBuffer, handler);
        inOrder.verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset), LITTLE_ENDIAN);

        frameOffset += FRAME_ALIGNMENT;
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset), LITTLE_ENDIAN);

        inOrder.verify(handler).onAvailable(0, FRAME_ALIGNMENT * 2);
    }

    @Test
    public void shouldScanTwoMessagesAndStopAtMtuBoundary()
    {
        final int frameOneLength = MTU_LENGTH - FRAME_ALIGNMENT;
        final int frameTwoLength = FRAME_ALIGNMENT;
        int frameOffset = 0;

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(frameOneLength + frameTwoLength);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(frameOneLength);
        when(logBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN))
            .thenReturn((short)HDR_TYPE_DATA);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset + frameOneLength)))
            .thenReturn(frameTwoLength);
        when(logBuffer.getShort(typeOffset(frameOffset + frameOneLength), LITTLE_ENDIAN))
            .thenReturn((short)HDR_TYPE_DATA);

        assertTrue(scanner.scanNext(MTU_LENGTH, handler));
        assertFalse(scanner.isComplete());

        final InOrder inOrder = inOrder(stateBuffer, logBuffer, handler);
        inOrder.verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset), LITTLE_ENDIAN);

        frameOffset += frameOneLength;
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset), LITTLE_ENDIAN);

        inOrder.verify(handler).onAvailable(0, frameOneLength + frameTwoLength);
    }

    @Test
    public void shouldScanTwoMessagesAndStopAtSecondThatSpansMtu()
    {
        final int frameOneLength = MTU_LENGTH - FRAME_ALIGNMENT;
        final int frameTwoLength = FRAME_ALIGNMENT * 2;
        int frameOffset = 0;

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(frameOneLength + frameTwoLength);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(frameOneLength);
        when(logBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN))
            .thenReturn((short)HDR_TYPE_DATA);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset + frameOneLength)))
            .thenReturn(frameTwoLength);
        when(logBuffer.getShort(typeOffset(frameOffset + frameOneLength), LITTLE_ENDIAN))
            .thenReturn((short)HDR_TYPE_DATA);

        assertTrue(scanner.scanNext(MTU_LENGTH, handler));
        assertFalse(scanner.isComplete());

        final InOrder inOrder = inOrder(stateBuffer, logBuffer, handler);
        inOrder.verify(stateBuffer).getIntVolatile(TAIL_COUNTER_OFFSET);
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset), LITTLE_ENDIAN);

        frameOffset += frameOneLength;
        inOrder.verify(logBuffer).getIntVolatile(lengthOffset(frameOffset));
        inOrder.verify(logBuffer).getShort(typeOffset(frameOffset), LITTLE_ENDIAN);

        inOrder.verify(handler).onAvailable(0, frameOneLength);
    }

    @Test
    public void shouldScanLastFrameInBuffer()
    {
        final int frameOffset = LOG_BUFFER_CAPACITY - FRAME_ALIGNMENT;

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(LOG_BUFFER_CAPACITY);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset)))
            .thenReturn(FRAME_ALIGNMENT);
        when(logBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN))
            .thenReturn((short)HDR_TYPE_DATA);

        scanner.seek(frameOffset);

        assertTrue(scanner.scanNext(MTU_LENGTH, handler));
        assertTrue(scanner.isComplete());
        assertFalse(scanner.scanNext(MTU_LENGTH, handler));

        verify(handler).onAvailable(frameOffset, FRAME_ALIGNMENT);
    }

    @Test
    public void shouldScanLastMessageInBufferPlusPadding()
    {
        final int frameOffset = LOG_BUFFER_CAPACITY - (FRAME_ALIGNMENT * 3);

        when(stateBuffer.getIntVolatile(TAIL_COUNTER_OFFSET))
            .thenReturn(LOG_BUFFER_CAPACITY - FRAME_ALIGNMENT);
        when(valueOf(logBuffer.getIntVolatile(lengthOffset(frameOffset))))
            .thenReturn(FRAME_ALIGNMENT);
        when(logBuffer.getShort(typeOffset(frameOffset), LITTLE_ENDIAN))
            .thenReturn((short)HDR_TYPE_DATA);
        when(logBuffer.getIntVolatile(lengthOffset(frameOffset + FRAME_ALIGNMENT)))
            .thenReturn(FRAME_ALIGNMENT * 2);
        when(logBuffer.getShort(typeOffset(frameOffset + FRAME_ALIGNMENT), LITTLE_ENDIAN))
            .thenReturn((short)PADDING_FRAME_TYPE);

        scanner.seek(frameOffset);

        assertTrue(scanner.scanNext(MTU_LENGTH, handler));
        assertTrue(scanner.isComplete());
        assertFalse(scanner.scanNext(MTU_LENGTH, handler));

        verify(handler, times(1)).onAvailable(frameOffset, FRAME_ALIGNMENT * 2);
    }
}
